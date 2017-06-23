// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leasing

import (
	"strings"
	"sync"

	v3 "github.com/coreos/etcd/clientv3"
	concurrency "github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
)

type leasingKV struct {
	cl      *v3.Client
	pfx     string
	session *concurrency.Session
	leases  leaseCache
	ctx     context.Context
	cancel  context.CancelFunc
	header  *server.ResponseHeader
	maxRev  int64
}

type leaseCache struct {
	entries map[string]*leaseInfo
	mu      sync.Mutex
}

type leaseInfo struct {
	waitc    chan struct{}
	response *v3.GetResponse
	revision int64
}

// NewleasingKV wraps a KV instance so that all requests are wired through a leasing protocol.
func NewleasingKV(cl *v3.Client, leasingprefix string) (v3.KV, error) {
	s, err := concurrency.NewSession(cl)

	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithCancel(s.Client().Ctx())
	return &leasingKV{cl: cl, pfx: leasingprefix, session: s, leases: leaseCache{entries: make(map[string]*leaseInfo)}, ctx: cctx, cancel: cancel, maxRev: 1}, nil
}

func (lc *leaseCache) inCache(key string) *leaseInfo {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.entries[key]
}

func (lc *leaseCache) deleteKeyInCache(key string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if li := lc.entries[key]; li != nil {
		delete(lc.entries, key)
	}
}

func (lc *leaseCache) updateCacheResp(key, val string, respHeader *server.ResponseHeader) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	mapResp := lc.entries[key].response
	if len(mapResp.Kvs) == 0 {
		myKV := &mvccpb.KeyValue{
			Value:   []byte(val),
			Key:     []byte(key),
			Version: 0,
		}
		mapResp.Kvs, mapResp.More, mapResp.Count = append(mapResp.Kvs, myKV), false, 1
		mapResp.Kvs[0].CreateRevision = respHeader.Revision
	}
	if len(mapResp.Kvs) > 0 {
		if mapResp.Kvs[0].ModRevision < respHeader.Revision {
			mapResp.Header, mapResp.Kvs[0].Value = respHeader, []byte(val)
			mapResp.Kvs[0].ModRevision = respHeader.Revision
		}
		mapResp.Kvs[0].Version++
	}
}

func respHeaderPopulate(respHeader *server.ResponseHeader) *server.ResponseHeader {
	header := &server.ResponseHeader{
		ClusterId: respHeader.ClusterId,
		MemberId:  respHeader.MemberId,
		Revision:  respHeader.Revision,
		RaftTerm:  respHeader.RaftTerm,
	}
	return header
}

func (lkv *leasingKV) updateCache(ctx context.Context, key, val string, op v3.Op) (*v3.PutResponse, error) {
	var rev int64
	var wc chan struct{}
	if li := lkv.leases.inCache(key); li != nil {
		lkv.leases.mu.Lock()
		li.waitc = make(chan struct{})
		wc, rev = li.waitc, li.revision
		lkv.leases.mu.Unlock()
	}
	if rev == 0 {
		return nil, nil
	}
	txnUpd := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(op)
	respUpd, errUpd := txnUpd.Commit()
	if errUpd != nil {
		lkv.leases.deleteKeyInCache(key)
		close(wc)
		return nil, errUpd
	}
	if !respUpd.Succeeded {
		close(wc)
		return nil, nil
	}
	if respUpd.Succeeded && lkv.leases.inCache(key) != nil {
		lkv.leases.updateCacheResp(key, val, respUpd.Header)
	}
	close(wc)
	putResp := (*v3.PutResponse)(respUpd.Responses[0].GetResponsePut())
	putResp.Header = respHeaderPopulate(respUpd.Header)
	return putResp, nil
}

func (lkv *leasingKV) watchforLKDel(ctx context.Context, key string, rev int64) {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wch := lkv.cl.Watch(cctx, lkv.pfx+key, v3.WithRev(rev+1))
	for resp := range wch {
		for _, ev := range resp.Events {
			if ev.Type == v3.EventTypeDelete {
				return
			}
		}
	}
}

func (lkv *leasingKV) put(ctx context.Context, key, val string, op v3.Op) (*v3.PutResponse, error) {
	for ctx.Err() == nil {
		respPut, err := lkv.updateCache(ctx, key, val, op)
		if respPut != nil || err != nil {
			return respPut, err
		}
		if lkv.leases.inCache(key) == nil {
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(op)
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()
			if err != nil {
				return nil, err
			}
			if resp.Succeeded {
				response := (*v3.PutResponse)(resp.Responses[0].GetResponsePut())
				response.Header = respHeaderPopulate(resp.Header)
				return response, err
			}
			lkv.watchforLKDel(ctx, key, resp.Header.Revision)
		}
	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) Put(ctx context.Context, key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {
	if len(key) == 0 {
		return nil, rpctypes.ErrEmptyKey
	}
	r, err := lkv.Do(ctx, v3.OpPut(key, val, opts...))
	if err != nil {
		return nil, err
	}
	put := r.Put()
	return put, nil
}

func (lkv *leasingKV) monitorLease(ctx context.Context, key string, resp *v3.TxnResponse, rev int64) {
	nCtx, cancel := context.WithCancel(lkv.ctx)
	defer cancel()
	for nCtx.Err() == nil {
		wch := lkv.cl.Watch(nCtx, lkv.pfx+key, v3.WithRev(resp.Header.Revision+1))
		for resp := range wch {
			for _, ev := range resp.Events {
				if string(ev.Kv.Value) == "REVOKE" {
					txn := lkv.cl.Txn(nCtx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(v3.OpDelete(lkv.pfx + key))
					delResp, err := txn.Commit()
					if delResp.Succeeded {
						lkv.leases.deleteKeyInCache(key)
						return
					}
					if err != nil || !delResp.Succeeded {
						continue
					}
				}
			}
		}
	}
}

func (lc *leaseCache) returnChannel(key string) (*leaseInfo, chan struct{}) {
	li := lc.inCache(key)
	lc.mu.Lock()
	if li != nil {
		wc := li.waitc
		lc.mu.Unlock()
		return li, wc
	}
	lc.mu.Unlock()
	return li, nil
}

func (lc *leaseCache) getCachedCopy(ctx context.Context, key string, op v3.Op) *v3.GetResponse {
	//fmt.Println("in cache")
	var resp *v3.GetResponse
	li, wc := lc.returnChannel(key)
	if li != nil {
		select {
		case <-wc:
			resp = li.response
			break
		case <-ctx.Done():
			return nil
		}
	}
	if resp == nil {
		return nil
	}
	lc.mu.Lock()
	var keyCopy, valCopy []byte
	var kvs []*mvccpb.KeyValue
	flag := true
	if len(resp.Kvs) == 0 || op.IsCountOnly() || (op.IsMaxModRev() != 0 && op.IsMaxModRev() <= resp.Kvs[0].ModRevision) ||
		(op.IsMaxCreateRev() != 0 && op.IsMaxCreateRev() <= resp.Kvs[0].CreateRevision) ||
		(op.IsMinModRev() != 0 && op.IsMinModRev() >= resp.Kvs[0].ModRevision) ||
		(op.IsMinCreateRev() != 0 && op.IsMinCreateRev() >= resp.Kvs[0].CreateRevision) {
		kvs = nil
		flag = false
	}
	//fmt.Println("populating ")
	if len(resp.Kvs) > 0 && flag {
		keyCopy = make([]byte, len(resp.Kvs[0].Key))
		copy(keyCopy, resp.Kvs[0].Key)
		if !op.IsKeysOnly() {
			valCopy = make([]byte, len(resp.Kvs[0].Value))
			copy(valCopy, resp.Kvs[0].Value)
		}
		kvs = []*mvccpb.KeyValue{
			&mvccpb.KeyValue{
				Key:            keyCopy,
				CreateRevision: resp.Kvs[0].CreateRevision,
				ModRevision:    resp.Kvs[0].ModRevision,
				Version:        resp.Kvs[0].Version,
				Value:          valCopy,
				Lease:          resp.Kvs[0].Lease,
			},
		}
	}
	copyResp := &v3.GetResponse{
		Header: respHeaderPopulate(resp.Header),
		Kvs:    kvs,
		More:   resp.More,
		Count:  resp.Count,
	}
	lc.mu.Unlock()
	return copyResp
}

func (lkv *leasingKV) appendMap(getresp *v3.GetResponse, key string) {
	lkv.leases.mu.Lock()
	waitc := make(chan struct{})
	close(waitc)
	lkv.leases.entries[key] = &leaseInfo{waitc: waitc, response: getresp, revision: getresp.Header.Revision}
	if lkv.maxRev < getresp.Header.Revision {
		lkv.header = getresp.Header
	}
	lkv.leases.mu.Unlock()
}

func (lkv *leasingKV) acquireLease(ctx context.Context, key string, op v3.Op) (*v3.TxnResponse, error) {
	txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
	txn = txn.Then((op), v3.OpPut(lkv.pfx+key, "", v3.WithLease(lkv.session.Lease())))
	txn = txn.Else(op)
	resp, err := txn.Commit()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (lkv *leasingKV) get(ctx context.Context, key string, op v3.Op) (*v3.GetResponse, error) {
	var gresp *v3.GetResponse
	getResp, err := lkv.leases.getCachedCopy(ctx, key, op), ctx.Err()
	if getResp != nil || err != nil {
		return getResp, err
	}
	if op.IsSerializable() {
		//fmt.Println("here")
		return lkv.cl.Get(ctx, key, v3.WithSerializable())
	}
	resp, err := lkv.acquireLease(ctx, key, op)
	if err != nil {
		return nil, err
	}
	if resp.Succeeded {
		getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
		getResp.Header = respHeaderPopulate(resp.Header)
		lkv.appendMap(getResp, key)
		go lkv.monitorLease(ctx, key, resp, resp.Header.Revision)
		gresp = lkv.leases.getCachedCopy(ctx, key, op)
	}
	if !resp.Succeeded {
		gresp = (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	}
	return gresp, nil
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if len(key) == 0 {
		return nil, rpctypes.ErrEmptyKey
	}
	if len(opts) > 0 && len(v3.OpGet(key, opts...).RangeBytes()) > 0 || !(v3.OpGet(key, opts...).IsKeysOnly()) &&
		(len(opts) > 0 && v3.OpGet(key, opts...).Rev() > 0) {
		//fmt.Println("base client")
		r, err := lkv.cl.Do(ctx, v3.OpGet(key, opts...))
		if err != nil {
			return nil, err
		}
		get := r.Get()
		return get, nil
	}

	//fmt.Println("cache")
	r, err := lkv.Do(ctx, v3.OpGet(key, opts...))
	if err != nil {
		return nil, err
	}
	get := r.Get()
	return get, nil
}

func (lkv *leasingKV) deleteKey(ctx context.Context, key string, op v3.Op) (*v3.DeleteResponse, error) {
	var rev int64
	var wc chan struct{}
	if li := lkv.leases.inCache(key); li != nil {
		lkv.leases.mu.Lock()
		li.waitc = make(chan struct{})
		wc, rev = li.waitc, li.revision
		lkv.leases.mu.Unlock()
	}
	if rev == 0 {
		return nil, nil
	}
	txnDel := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(op)
	respDel, errDel := txnDel.Commit()
	if errDel != nil {
		lkv.leases.deleteKeyInCache(key)
		close(wc)
		return nil, errDel
	}
	if !respDel.Succeeded {
		close(wc)
		return nil, nil
	}
	lkv.leases.deleteKeyInCache(key)
	close(wc)
	delResp := (*v3.DeleteResponse)(respDel.Responses[0].GetResponseDeleteRange())
	delResp.Header = respHeaderPopulate(respDel.Header)
	return delResp, nil
}

func (lkv *leasingKV) allocateChannel(resp *v3.TxnResponse, key string) chan struct{} {
	getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	getResp.Header = respHeaderPopulate(resp.Header)
	lkv.leases.mu.Lock()
	lkv.leases.entries[key] = &leaseInfo{response: getResp,
		revision: resp.Header.Revision}
	lkv.leases.entries[key].waitc = make(chan struct{})
	wc := lkv.leases.entries[key].waitc
	lkv.leases.mu.Unlock()
	return wc
}

func maxRev(getResp *v3.GetResponse) int64 {
	var maxRev int64
	for i := range getResp.Kvs {
		if maxRev < getResp.Kvs[i].CreateRevision {
			maxRev = getResp.Kvs[i].CreateRevision
		}
	}
	return maxRev
}

func (lkv *leasingKV) delete(ctx context.Context, key string, op v3.Op) (*v3.DeleteResponse, error) {
	var wc chan struct{}
	endKey := string(op.RangeBytes())
	for ctx.Err() == nil {
		if len(op.RangeBytes()) > 0 {
			getResp, err := lkv.cl.Get(ctx, key, v3.WithRange(endKey))
			if err != nil {
				//	fmt.Println("err in Getres", err)
				return nil, err
			}
			//	fmt.Println("getresp", getResp)
			if len(getResp.Kvs) > 0 {
				for i := range getResp.Kvs {
					if _, ok := lkv.leases.entries[string(getResp.Kvs[i].Key)]; !ok {
						resp, err := lkv.acquireLease(ctx, string(getResp.Kvs[i].Key), v3.OpGet(string(getResp.Kvs[i].Key)))
						//fmt.Println("err in acquire", err)
						if err != nil {
							return nil, err
						}
						wc = lkv.allocateChannel(resp, string(getResp.Kvs[i].Key))
					}
				}

				/*for k := range lkv.leases.entries {
					fmt.Printf("key[%s] value[%s]\n", k, lkv.leases.entries[k].response)
				}*/

				maxRev := maxRev(getResp)
				txn1 := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(key).WithRange(endKey), "<", maxRev+1),
					v3.Compare(v3.CreateRevision(lkv.pfx+key).WithRange(endKey), ">", 0))
				txn1 = txn1.Then(v3.OpDelete(key, v3.WithRange(endKey)), v3.OpDelete(lkv.pfx+key, v3.WithRange(endKey)))
				delRangeResp, err := txn1.Commit()
				if err != nil {
					/*for i := range getResp.Kvs {
						if _, ok := lkv.leases.entries[string(getResp.Kvs[i].Key)]; ok {
							lkv.leases.deleteKeyInCache(string(getResp.Kvs[i].Key))
						}
					}
					fmt.Println("err in commit", err)*/
					return nil, err
				}
				//fmt.Println("here3")
				if delRangeResp.Succeeded {
					for i := range getResp.Kvs {
						if _, ok := lkv.leases.entries[string(getResp.Kvs[i].Key)]; ok {
							lkv.leases.deleteKeyInCache(string(getResp.Kvs[i].Key))
						}
					}
					delResp := (*v3.DeleteResponse)(delRangeResp.Responses[0].GetResponseDeleteRange())
					delResp.Header = respHeaderPopulate(delRangeResp.Header)
					close(wc)
					return delResp, nil
				}
				if !delRangeResp.Succeeded {
					continue
				}
			}

			if len(getResp.Kvs) == 0 {
				return nil, err
			}
		}
		respDel, err := lkv.deleteKey(ctx, key, op)
		if respDel != nil || err != nil {
			return respDel, err
		}
		if lkv.leases.inCache(key) == nil {
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(op)
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()
			if err != nil {
				//	fmt.Println("here3")
				return nil, err
			}
			if resp.Succeeded {
				return (*v3.DeleteResponse)(resp.Responses[0].GetResponseDeleteRange()), nil
			}
			lkv.watchforLKDel(ctx, key, resp.Header.Revision)
		}
	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	if len(key) == 0 {
		return nil, rpctypes.ErrEmptyKey
	}
	r, err := lkv.Do(ctx, v3.OpDelete(key, opts...))
	if err != nil {
		return nil, err
	}
	del := r.Del()
	return del, nil
}

type txnLeasing struct {
	v3.Txn
	lkv  *leasingKV
	ctx  context.Context
	cs   []v3.Cmp
	opst []v3.Op
	opse []v3.Op
}

func (lkv *leasingKV) Txn(ctx context.Context) v3.Txn {
	return &txnLeasing{lkv.cl.Txn(ctx), lkv, ctx, make([]v3.Cmp, 0), make([]v3.Op, 0), make([]v3.Op, 0)}
}

func (txn *txnLeasing) If(cs ...v3.Cmp) v3.Txn {
	for i := range cs {
		txn.cs = append(txn.cs, cs[i])
	}
	return txn
}

func (txn *txnLeasing) Then(ops ...v3.Op) v3.Txn {
	for i := range ops {
		txn.opst = append(txn.opst, ops[i])
	}
	return txn
}

func (txn *txnLeasing) Else(ops ...v3.Op) v3.Txn {
	for i := range ops {
		txn.opse = append(txn.opse, ops[i])
	}
	return txn
}

func (txn *txnLeasing) Commit() (*v3.TxnResponse, error) {
	var txnResp *v3.TxnResponse
	var i int
	cacheBool, serverTxnBool := txn.allCmpsfromCache()
	if !serverTxnBool {
		opArray := txn.defOpArray(cacheBool)
		if ok, txnResp, err := txn.noOps(opArray, cacheBool); ok {
			return txnResp, err
		}
		if responseArray, ok := txn.cacheOpArray(opArray); ok {
			return txn.lkv.allInCache(responseArray, cacheBool)
		}
		serverTxnBool = !serverTxnBool
	}

	if serverTxnBool {
		txnOps := append(txn.opst, txn.opse...)
		//fmt.Println(allOps)
		//txnOps := txn.gatherAllOps(allOps, make([]v3.Op, 0))
		//fmt.Println(txnOps)
		flag := true
		for flag == true {
			elseOps, cmps := txn.cmpUpdate(txnOps)
			resp, err := txn.lkv.cl.Txn(txn.ctx).If(cmps...).Then(v3.OpTxn(txn.cs, txn.opst, txn.opse)).Else(elseOps...).Commit()
			if err != nil {
				for i := range cmps {
					key := string(cmps[i].Key)
					if txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx)) != nil {
						txn.lkv.leases.deleteKeyInCache(strings.TrimPrefix(key, txn.lkv.pfx))
					}
				}
				return nil, err
			}
			if !resp.Succeeded {
				mapResp := make(map[string]bool)
				for i = 0; i < len(elseOps); i++ {
					key := string(elseOps[i].KeyBytes())
					if len((*v3.GetResponse)(resp.Responses[i].GetResponseRange()).Kvs) != 0 {
						mapResp[(strings.TrimPrefix(key, txn.lkv.pfx))] = true
					}
				}
				for i = 0; i < len(txnOps); i++ {
					key := string(txnOps[i].KeyBytes())
					if li := txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx)); li == nil {
						if mapResp[key] && (txnOps[i].IsPut() || txnOps[i].IsDelete()) {
							err := txn.revokeLease(key)
							if err != nil {
								return nil, err
							}
						}
					}
				}
			}
			if resp.Succeeded {
				txnResp = txn.extractResp(resp)
				txn.modifyCacheTxn(txnResp)
				flag = false
			}
		}
	}
	return txnResp, nil
}

func (lkv *leasingKV) Compact(ctx context.Context, rev int64, opts ...v3.CompactOption) (*v3.CompactResponse, error) {
	return lkv.cl.Compact(ctx, rev, opts...)
}

func (lkv *leasingKV) Do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	return lkv.do(ctx, op)
}

func (lkv *leasingKV) do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	if op.IsGet() {
		resp, err := lkv.get(ctx, string(op.KeyBytes()), op)
		if err == nil {
			return resp.OpResponse(), nil
		}
		return v3.OpResponse{}, err
	}
	if op.IsPut() {
		resp, err := lkv.put(ctx, string(op.KeyBytes()), string(op.ValueBytes()), op)
		if err == nil {
			return resp.OpResponse(), nil
		}
		return v3.OpResponse{}, err
	}
	if op.IsDelete() {
		resp, err := lkv.delete(ctx, string(op.KeyBytes()), op)
		if err == nil {
			return resp.OpResponse(), nil
		}
		return v3.OpResponse{}, err
	}
	if op.IsTxn() {
		cmps, thenOps, elseOps := op.Txn()
		resp, err := lkv.Txn(ctx).If(cmps...).Then(thenOps...).Else(elseOps...).Commit()
		if err == nil {
			return resp.OpResponse(), nil
		}
		return v3.OpResponse{}, err
	}
	return v3.OpResponse{}, nil
}
