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
	"fmt"
	"strings"
	"sync"

	v3 "github.com/coreos/etcd/clientv3"
	concurrency "github.com/coreos/etcd/clientv3/concurrency"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
)

type leasingKV struct {
	cl       *v3.Client
	kv       v3.KV
	pfx      string
	session  *concurrency.Session
	leases   leaseCache
	ctx      context.Context
	cancel   context.CancelFunc
	header   *server.ResponseHeader
	maxRev   int64
	sessionc chan struct{}
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
	sessionc := make(chan struct{})
	close(sessionc)
	cctx, cancel := context.WithCancel(cl.Ctx())
	kv := leasingKV{cl: cl, kv: cl.KV, pfx: leasingprefix, session: s, leases: leaseCache{entries: make(map[string]*leaseInfo)}, ctx: cctx, cancel: cancel, maxRev: 1, sessionc: sessionc}
	go kv.monitorSession(cl, 0)
	return &kv, nil
}

// NewleasingKVTTL wraps a KV instance so that all requests are wired through a leasing protocol and uses a TTL to keep session alive
func NewleasingKVTTL(cl *v3.Client, leasingprefix string, ttl int) (v3.KV, error) {
	s, err := concurrency.NewSession(cl, concurrency.WithTTL(ttl))
	if err != nil {
		return nil, err
	}
	sessionc := make(chan struct{})
	close(sessionc)
	cctx, cancel := context.WithCancel(cl.Ctx())
	kv := leasingKV{cl: cl, kv: cl.KV, pfx: leasingprefix, session: s, leases: leaseCache{entries: make(map[string]*leaseInfo)}, ctx: cctx, cancel: cancel, maxRev: 1, sessionc: sessionc}
	go kv.monitorSession(cl, ttl)
	return &kv, nil
}

func (lkv *leasingKV) monitorSession(cl *v3.Client, ttl int) {
	for {
		<-lkv.session.Done()
		select {
		case <-lkv.ctx.Done():
			return
		default:
			lkv.leases.mu.Lock()
			lkv.sessionc = make(chan struct{})
			for k := range lkv.leases.entries {
				delete(lkv.leases.entries, k)
			}
			lkv.leases.mu.Unlock()
			s, err := startNewSession(cl, ttl)
			if err != nil {
				continue
			}
			lkv.leases.mu.Lock()
			lkv.session = s
			close(lkv.sessionc)
			lkv.leases.mu.Unlock()
		}
	}
}

func startNewSession(cl *v3.Client, ttl int) (*concurrency.Session, error) {
	var s *concurrency.Session
	var err error
	if ttl > 0 {
		s, err = concurrency.NewSession(cl, concurrency.WithTTL(ttl))
	} else {
		s, err = concurrency.NewSession(cl)
	}
	return s, err
}

func (lkv *leasingKV) checkOpenChannel() bool {
	lkv.leases.mu.Lock()
	defer lkv.leases.mu.Unlock()
	select {
	case <-lkv.session.Done():
	default:
		return true
	}
	return false
}

func (lc *leaseCache) inCache(key string) *leaseInfo {
	lc.mu.Lock()
	li := lc.entries[key]
	lc.mu.Unlock()
	return li
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
	txnUpd := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(op)
	respUpd, errUpd := txnUpd.Commit()
	if errUpd != nil {
		lkv.leases.deleteKeyInCache(key)
		close(wc)
		return nil, errUpd
	}
	if respUpd.Succeeded && lkv.leases.inCache(key) != nil {
		lkv.leases.updateCacheResp(key, val, respUpd.Header)
	}
	close(wc)
	if !respUpd.Succeeded {
		return nil, nil
	}
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
		<-lkv.sessionc
		respPut, err := lkv.updateCache(ctx, key, val, op)
		if respPut != nil || err != nil {
			return respPut, err
		}
		if lkv.leases.inCache(key) == nil {
			txn := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(op)
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
	r, err := lkv.Do(ctx, v3.OpPut(key, val, opts...))
	if err != nil {
		return nil, err
	}
	return r.Put(), nil
}

func (lkv *leasingKV) monitorLease(ctx context.Context, key string, resp *v3.TxnResponse, rev int64) {
	//var wc chan struct{}
	nCtx, cancel := context.WithCancel(lkv.ctx)
	defer cancel()
	for nCtx.Err() == nil {
		/*	if li := lkv.leases.inCache(key); li != nil {
			lkv.leases.mu.Lock()
			li.waitc = make(chan struct{})
			wc = li.waitc
			lkv.leases.mu.Unlock()
		}*/
		wch := lkv.cl.Watch(nCtx, lkv.pfx+key, v3.WithRev(resp.Header.Revision+1))
		for resp := range wch {
			for _, ev := range resp.Events {
				if string(ev.Kv.Value) == "REVOKE" {
					txn := lkv.kv.Txn(nCtx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(v3.OpDelete(lkv.pfx + key))
					delResp, err := txn.Commit()
					if delResp.Succeeded {
						lkv.leases.deleteKeyInCache(key)
						fmt.Println("deleted", key)
						//	close(wc)
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
	if li != nil {
		lc.mu.Lock()
		wc := li.waitc
		lc.mu.Unlock()
		return li, wc
	}
	return li, nil
}

func (lc *leaseCache) getCachedCopy(ctx context.Context, key string, op v3.Op) *v3.GetResponse {
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
	var kvsnil bool
	if len(resp.Kvs) == 0 || op.IsCountOnly() || (op.IsMaxModRev() != 0 && op.IsMaxModRev() <= resp.Kvs[0].ModRevision) ||
		(op.IsMaxCreateRev() != 0 && op.IsMaxCreateRev() <= resp.Kvs[0].CreateRevision) ||
		(op.IsMinModRev() != 0 && op.IsMinModRev() >= resp.Kvs[0].ModRevision) ||
		(op.IsMinCreateRev() != 0 && op.IsMinCreateRev() >= resp.Kvs[0].CreateRevision) {
		kvs = nil
	}
	if len(resp.Kvs) > 0 && !kvsnil {
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

func (lkv *leasingKV) leaseID() v3.LeaseID {
	lkv.leases.mu.Lock()
	defer lkv.leases.mu.Unlock()
	return lkv.session.Lease()
}

func (lkv *leasingKV) acquireLease(ctx context.Context, key string, op v3.Op) (*v3.TxnResponse, error) {
	txn := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
	txn = txn.Then((op), v3.OpPut(lkv.pfx+key, "", v3.WithLease(lkv.leaseID())))
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
		return lkv.kv.Get(ctx, key, v3.WithSerializable())
	}
	//fmt.Println("server rpc acquireLease")
	resp, err := lkv.acquireLease(ctx, key, op)
	//	fmt.Println("resp", resp)
	if err != nil {
		return nil, err
	}
	if resp.Succeeded {
		getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
		getResp.Header = respHeaderPopulate(resp.Header)
		lkv.appendMap(getResp, key)
		go lkv.monitorLease(ctx, key, resp, resp.Header.Revision)
		gresp = lkv.leases.getCachedCopy(ctx, key, op)
	} else {
		gresp = (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	}
	return gresp, nil
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if lkv.checkOpenChannel() {
		if len(opts) > 0 && len(v3.OpGet(key, opts...).RangeBytes()) > 0 || !(v3.OpGet(key, opts...).IsKeysOnly()) &&
			(len(opts) > 0 && v3.OpGet(key, opts...).Rev() > 0) {
			r, err := lkv.kv.Do(ctx, v3.OpGet(key, opts...))
			if err != nil {
				return nil, err
			}
			return r.Get(), nil
		}
		r, err := lkv.Do(ctx, v3.OpGet(key, opts...))
		if err != nil {
			return nil, err
		}
		return r.Get(), nil
	}
	return lkv.kv.Get(ctx, key, opts...)
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
	txnDel := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(op)
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

func (lkv *leasingKV) allocateChannel(txnResp *v3.TxnResponse, key string) chan struct{} {
	getResp := (*v3.GetResponse)(txnResp.Responses[0].GetResponseRange())
	getResp.Header = respHeaderPopulate(txnResp.Header)
	lkv.leases.mu.Lock()
	lkv.leases.entries[key] = &leaseInfo{response: getResp,
		revision: getResp.Header.Revision}
	lkv.leases.entries[key].waitc = make(chan struct{})
	lkv.leases.mu.Unlock()
	return lkv.leases.entries[key].waitc
}

const (
	mod    int = 2
	create int = 1
)

func maxRevision(getResp *v3.GetResponse, revType int) int64 {
	var maxRev int64
	switch revType {
	case create:
		for i := range getResp.Kvs {
			if maxRev < getResp.Kvs[i].CreateRevision {
				maxRev = getResp.Kvs[i].CreateRevision
			}
		}
	case mod:
		for i := range getResp.Kvs {
			if maxRev < getResp.Kvs[i].ModRevision {
				maxRev = getResp.Kvs[i].ModRevision
			}
		}
	}
	return maxRev
}

func (lkv *leasingKV) deleteRange(ctx context.Context, key string, op v3.Op) (*v3.DeleteResponse, error) {
	var acqResp *v3.TxnResponse
	var maxRev int64
	for ctx.Err() == nil {
		fmt.Println("start")
		endKey, leasingendKey := string(op.RangeBytes()), v3.GetPrefixRangeEnd(lkv.pfx+key)
		var wc [](chan struct{})
		getResp, err := lkv.kv.Get(ctx, key, v3.WithRange(endKey))
		//fmt.Println("getResp", getResp)
		if err != nil {
			return nil, err
		}

		for i := range getResp.Kvs {
			gkey := string(getResp.Kvs[i].Key)
			if _, ok := lkv.leases.entries[gkey]; !ok {
				txn1 := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+gkey), "=", 0))
				txn1 = txn1.Then(v3.OpPut(lkv.pfx+gkey, "", v3.WithLease(lkv.leaseID())))
				txn1 = txn1.Else(v3.OpPut(lkv.pfx+gkey, "REVOKE", v3.WithIgnoreLease()))
				revokeResp, err := txn1.Commit()
				if err != nil {
					return nil, err
				}
				if !revokeResp.Succeeded {
					lkv.watchforLKDel(ctx, gkey, revokeResp.Header.Revision)
				}
				acqResp, err = lkv.acquireLease(ctx, gkey, v3.OpGet(gkey))
				if err != nil {
					return nil, err
				}
				wc = append(wc, lkv.allocateChannel(acqResp, gkey))
			}
		}

		lkv.leases.mu.Lock()
		fmt.Println("lkv after acquire", lkv)
		lkv.leases.mu.Unlock()

		getresp, _ := lkv.kv.Get(ctx, key, v3.WithRange(endKey))
		fmt.Println("getresp", getresp)
		maxRev = maxRevision(getresp, mod)
		fmt.Println("maxModRev", maxRev)

		lkvresp, _ := lkv.cl.Get(ctx, lkv.pfx+key, v3.WithRange(leasingendKey))
		fmt.Println("lkresp", lkvresp)
		maxRevlk := maxRevision(lkvresp, create)
		fmt.Println("maxCreateRevlk", maxRevlk)

		txn1 := lkv.kv.Txn(ctx).If(v3.Compare(v3.ModRevision(key).WithRange(endKey), "<", maxRev+1),
			v3.Compare(v3.CreateRevision(lkv.pfx+key).WithRange(leasingendKey), "<", maxRevlk+1))
		txn1 = txn1.Then(v3.OpDelete(key, v3.WithRange(endKey)), v3.OpDelete(lkv.pfx+key, v3.WithRange(leasingendKey)))
		delRangeResp, err := txn1.Commit()
		if err != nil {
			for i := range wc {
				close(wc[i])
			}
			return nil, err
		}
		if !delRangeResp.Succeeded {
			for i := range wc {
				close(wc[i])
			}
			continue
		}
		fmt.Println("success")
		for i := range getResp.Kvs {
			if _, ok := lkv.leases.entries[string(getResp.Kvs[i].Key)]; ok {
				lkv.leases.deleteKeyInCache(string(getResp.Kvs[i].Key))
			}
		}
		for i := range wc {
			close(wc[i])
		}
		delResp := (*v3.DeleteResponse)(delRangeResp.Responses[0].GetResponseDeleteRange())
		delResp.Header = respHeaderPopulate(delRangeResp.Header)
		return delResp, nil
	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) delete(ctx context.Context, key string, op v3.Op) (*v3.DeleteResponse, error) {
	<-lkv.sessionc
	if len(op.RangeBytes()) > 0 {
		return lkv.deleteRange(ctx, key, op)
	}
	for ctx.Err() == nil {
		respDel, err := lkv.deleteKey(ctx, key, op)
		if respDel != nil || err != nil {
			return respDel, err
		}
		if lkv.leases.inCache(key) == nil {
			txn := lkv.kv.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(op)
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

func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	r, err := lkv.Do(ctx, v3.OpDelete(key, opts...))
	if err != nil {
		return nil, err
	}
	return r.Del(), nil
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
	return &txnLeasing{lkv.kv.Txn(ctx), lkv, ctx, make([]v3.Cmp, 0), make([]v3.Op, 0), make([]v3.Op, 0)}
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
	for txn.ctx.Err() == nil {
		var txnResp *v3.TxnResponse
		cacheBool, serverTxnBool := txn.allCmpsfromCache()
		if !serverTxnBool {
			opArray := txn.gatherAllOps(txn.defOpArray(cacheBool))
			if ok, txnResp, err := txn.noOps(opArray, cacheBool); ok {
				if err != nil {
					return nil, err
				}
				return txnResp, nil
			}
			for _, ch := range txn.lkv.leases.blockKeys(opArray, waitReleaseChan) {
				select {
				case <-ch:
				case <-txn.ctx.Done():
					return nil, txn.ctx.Err()
				}
			}
			txn.lkv.leases.mu.Lock()
			responseArray, ok := txn.cacheOpArray(opArray)
			txn.lkv.leases.mu.Unlock()
			if ok {
				if txn.lkv.checkOpenChannel() {
					cacheResp, _ := txn.lkv.allInCache(responseArray, cacheBool)
					return cacheResp, nil
				}
				return txn.lkv.cl.Txn(txn.ctx).If(txn.cs...).Then(txn.opst...).Else(txn.opse...).Commit()
			}
			serverTxnBool = !serverTxnBool
		}
		<-txn.lkv.sessionc
		allOps := append(txn.opst, txn.opse...)
		txnOps := txn.gatherAllOps(allOps)
		wcs := txn.lkv.leases.blockKeys(txnOps, acquireChan)
		for {
			elseOps, cmps := txn.cmpUpdate(txnOps)
			resp, err := txn.lkv.kv.Txn(txn.ctx).If(cmps...).Then(v3.OpTxn(txn.cs, txn.opst, txn.opse)).Else(elseOps...).Commit()
			if err != nil {
				for i := range cmps {
					key := string(cmps[i].Key)
					if txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx)) != nil {
						txn.lkv.leases.deleteKeyInCache(strings.TrimPrefix(key, txn.lkv.pfx))
					}
				}
				return nil, err
			}
			if resp.Succeeded {
				txnResp = txn.extractResp(resp)
				txn.modifyCacheTxn(txnResp)
				for _, wc := range wcs {
					close(wc)
				}
				break
			}
			err = txn.NonOwnerRevoke(resp, elseOps, txnOps)
			if err != nil {
				return nil, err
			}
		}
		return txnResp, nil
	}
	return nil, txn.ctx.Err()
}

func (lkv *leasingKV) Compact(ctx context.Context, rev int64, opts ...v3.CompactOption) (*v3.CompactResponse, error) {
	return lkv.kv.Compact(ctx, rev, opts...)
}

func (lkv *leasingKV) Do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	return lkv.do(ctx, op)
}

func (lkv *leasingKV) do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	if op.IsGet() {
		if lkv.checkOpenChannel() {
			resp, err := lkv.get(ctx, string(op.KeyBytes()), op)
			if err == nil {
				return resp.OpResponse(), nil
			}
			return v3.OpResponse{}, err
		}
		return lkv.kv.Do(ctx, op)
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
