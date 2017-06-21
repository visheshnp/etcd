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

func (lkv *leasingKV) updateCache(ctx context.Context, key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {
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
	txnUpd := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(v3.OpPut(key, val, opts...))
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

func (lkv *leasingKV) Put(ctx context.Context, key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {
	for ctx.Err() == nil {
		respPut, err := lkv.updateCache(ctx, key, val, opts...)
		if respPut != nil || err != nil {
			return respPut, err
		}
		if lkv.leases.inCache(key) == nil {
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(v3.OpPut(key, val, opts...))
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

func (lc *leaseCache) getCachedCopy(ctx context.Context, key string, opts ...v3.OpOption) *v3.GetResponse {
	var resp *v3.GetResponse
	ops := v3.OpGet(key, opts...)
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
	if len(resp.Kvs) == 0 || ops.IsCountOnly() || (ops.IsMaxModRev() != 0 && ops.IsMaxModRev() <= resp.Kvs[0].ModRevision) ||
		(ops.IsMaxCreateRev() != 0 && ops.IsMaxCreateRev() <= resp.Kvs[0].CreateRevision) ||
		(ops.IsMinModRev() != 0 && ops.IsMinModRev() >= resp.Kvs[0].ModRevision) ||
		(ops.IsMinCreateRev() != 0 && ops.IsMinCreateRev() >= resp.Kvs[0].CreateRevision) {
		kvs = nil
		flag = false
	}
	if len(resp.Kvs) > 0 && flag {
		keyCopy = make([]byte, len(resp.Kvs[0].Key))
		copy(keyCopy, resp.Kvs[0].Key)
		if !ops.IsKeysOnly() {
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

func (lkv *leasingKV) acquireLease(ctx context.Context, key string, opts ...v3.OpOption) (*v3.TxnResponse, error) {
	txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
	txn = txn.Then(v3.OpGet(key, opts...), v3.OpPut(lkv.pfx+key, "", v3.WithLease(lkv.session.Lease())))
	txn = txn.Else(v3.OpGet(key, opts...))
	resp, err := txn.Commit()
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if len(opts) > 0 && len(v3.OpGet(key, opts...).RangeBytes()) > 0 {
		return lkv.cl.Get(ctx, key, opts...)
	}
	if !(v3.OpGet(key, opts...).IsKeysOnly()) {
		if len(opts) > 0 && v3.OpGet(key, opts...).Rev() > 0 {
			return lkv.cl.Get(ctx, key, opts...)
		}
	}
	var gresp *v3.GetResponse
	getResp, err := lkv.leases.getCachedCopy(ctx, key, opts...), ctx.Err()
	if getResp != nil || err != nil {
		return getResp, err
	}
	if len(opts) > 0 && v3.OpGet(key, opts...).IsSerializable() {
		return lkv.cl.Get(ctx, key, opts...)
	}
	resp, err := lkv.acquireLease(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	if resp.Succeeded {
		getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
		getResp.Header = respHeaderPopulate(resp.Header)
		lkv.appendMap(getResp, key)
		go lkv.monitorLease(ctx, key, resp, resp.Header.Revision)
		gresp = lkv.leases.getCachedCopy(ctx, key)
	}
	if !resp.Succeeded {
		gresp = (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	}
	return gresp, nil
}

func (lkv *leasingKV) deleteKey(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
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
	txnDel := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(v3.OpDelete(key, opts...))
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

func (lkv *leasingKV) acquireandAdd(ctx context.Context, key string) chan struct{} {
	resp, _ := lkv.acquireLease(ctx, key)
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

func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	var wc chan struct{}
	for ctx.Err() == nil {
		if len(opts) > 0 && (string((v3.OpGet(key, opts...)).RangeBytes()) != string((v3.OpGet(key, opts...)).KeyBytes())) {
			getResp, err := lkv.cl.Get(ctx, key, opts...)
			if err != nil {
				close(wc)
				return nil, err
			}
			for i := range getResp.Kvs {
				if _, ok := lkv.leases.entries[string(getResp.Kvs[i].Key)]; !ok {
					wc = lkv.acquireandAdd(ctx, string(getResp.Kvs[i].Key))
				}
			}
			maxRev := maxRev(getResp)
			txn1 := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(key).WithPrefix(), "<", maxRev+1),
				v3.Compare(v3.CreateRevision(lkv.pfx+key).WithPrefix(), ">", 0))
			txn1 = txn1.Then(v3.OpDelete(key, v3.WithPrefix()), v3.OpDelete(lkv.pfx+key, v3.WithPrefix()))
			delRangeResp, err := txn1.Commit()
			if err != nil {
				for i := range getResp.Kvs {
					if _, ok := lkv.leases.entries[string(getResp.Kvs[i].Key)]; ok {
						lkv.leases.deleteKeyInCache(string(getResp.Kvs[i].Key))
					}
				}
				close(wc)
				return nil, err
			}
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
		respDel, err := lkv.deleteKey(ctx, key, opts...)
		if respDel != nil || err != nil {
			return respDel, err
		}

		if lkv.leases.inCache(key) == nil {
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(v3.OpDelete(key, opts...))
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()
			if err != nil {
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
		allOps := append(txn.opst, txn.opse...)
		flag := true
		for flag == true {
			elseOps, cmps := txn.cmpUpdate(allOps)
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
				for i = 0; i < len(allOps); i++ {
					key := string(allOps[i].KeyBytes())
					if li := txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx)); li == nil {
						if mapResp[key] && (allOps[i].IsPut() || allOps[i].IsDelete()) {
							err := txn.revokeLease(key)
							if err != nil {
								return nil, err
							}
						}
					}
				}
			}
			if resp.Succeeded {
				for i = 0; i < len(allOps); i++ {
					key := string(allOps[i].KeyBytes())
					li := txn.lkv.leases.inCache(key)
					if li != nil && allOps[i].IsPut() {
						txn.lkv.leases.updateCacheResp(key, string(allOps[i].ValueBytes()), resp.Header)
					}
					if li != nil && allOps[i].IsDelete() {
						txn.lkv.deleteKey(txn.ctx, key)
					}
				}
				flag = false
				txnResp = txn.extractResp(resp)
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
		resp, err := lkv.Get(ctx, string(op.KeyBytes()))
		if err == nil {
			return resp.OpResponse(), nil
		}
		if err != nil {
			return v3.OpResponse{}, err
		}
	}
	if op.IsPut() {
		resp, err := lkv.Put(ctx, string(op.KeyBytes()), string(op.ValueBytes()))
		if err == nil {
			return resp.OpResponse(), nil
		}
		if err != nil {
			return v3.OpResponse{}, err
		}
	}
	if op.IsDelete() {
		resp, err := lkv.Delete(ctx, string(op.KeyBytes()))
		if err == nil {
			return resp.OpResponse(), nil
		}
		if err != nil {
			return v3.OpResponse{}, err
		}
	}

	return v3.OpResponse{}, nil
}
