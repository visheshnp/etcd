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
	"bytes"
	"strings"
	"sync"

	"fmt"

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
	cctx, cancel := context.WithCancel(cl.Ctx())
	return &leasingKV{cl: cl, pfx: leasingprefix, session: s, leases: leaseCache{entries: make(map[string]*leaseInfo)}, ctx: cctx, cancel: cancel, maxRev: 1}, nil
}

func (lkv *leasingKV) Compact(ctx context.Context, rev int64, opts ...v3.CompactOption) (*v3.CompactResponse, error) {
	return lkv.cl.Compact(ctx, rev, opts...)
}

func (lkv *leasingKV) Do(ctx context.Context, op v3.Op) (v3.OpResponse, error) { panic("Stub") }

func (lc *leaseCache) inCache(key string) *leaseInfo {
	lc.mu.Lock()
	li := lc.entries[key]
	lc.mu.Unlock()
	return li
}

func (lc *leaseCache) deleteKeyInCache(key string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	li := lc.entries[key]
	if li != nil {
		delete(lc.entries, key)
	}
}

func (lc *leaseCache) updateCacheResp(key, val string, respHeader *server.ResponseHeader) {
	lc.mu.Lock()
	mapResp := lc.entries[key].response
	if len(mapResp.Kvs) == 0 {
		myKV := &mvccpb.KeyValue{
			Value:   []byte(val),
			Key:     []byte(key),
			Version: 0,
		}
		mapResp.Kvs = append(mapResp.Kvs, myKV)
		mapResp.More = false
		mapResp.Count = 1
		mapResp.Kvs[0].CreateRevision = respHeader.Revision
	}

	if len(mapResp.Kvs) > 0 {
		if mapResp.Kvs[0].ModRevision < respHeader.Revision {
			mapResp.Header = respHeader
			mapResp.Kvs[0].Value = []byte(val)
			mapResp.Kvs[0].ModRevision = respHeader.Revision
		}
		mapResp.Kvs[0].Version++
	}
	lc.mu.Unlock()
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
		wc = li.waitc
		rev = li.revision
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

func (lkv *leasingKV) watchforLKDel(ctx context.Context, key string, rev int64) bool {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wch := lkv.cl.Watch(cctx, lkv.pfx+key, v3.WithRev(rev+1))
	for resp := range wch {
		for _, ev := range resp.Events {
			if ev.Type == v3.EventTypeDelete {
				return true
			}
		}
	}
	return false
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
	//fmt.Println(ops)
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

func (lc *leaseCache) cacheOps(key string, opts ...v3.OpOption) bool {
	ops := v3.OpGet(key, opts...)
	count := 0
	cmps := make([]bool, 0)
	fmt.Println("rangebytes", len(ops.RangeBytes()))
	if len(opts) > 0 {
		cmps = append(cmps, ops.IsKeysOnly(), ops.IsCountOnly(), ops.IsMaxModRev() > 0, ops.IsMinModRev() > 0, ops.IsSort(), ops.IsMinCreateRev() > 0,
			ops.IsMaxCreateRev() > 0, ops.IsLimit() > 0, (ops.IsSerializable() && lc.inCache(key) != nil))
		for i := range cmps {
			if cmps[i] == true {
				count++
			}
		}
		if count != len(opts) {
			return false
		}
		boolCmps := ops.IsKeysOnly() || ops.IsCountOnly() || ops.IsMaxModRev() > 0 || ops.IsMinModRev() > 0 || ops.IsSort() || ops.IsMinCreateRev() > 0 ||
			ops.IsMaxCreateRev() > 0 || (ops.IsSerializable() && lc.inCache(key) != nil)
		if ops.IsLimit() >= 0 && len(opts) == 1 {
			return true
		}
		return boolCmps
	}
	return true
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if len(opts) > 0 && len(v3.OpGet(key, opts...).RangeBytes()) > 0 {
		return lkv.cl.Get(ctx, key, opts...)
	}
	var gresp *v3.GetResponse
	if lkv.leases.cacheOps(key, opts...) {
		getResp, err := lkv.leases.getCachedCopy(ctx, key, opts...), ctx.Err()
		if getResp != nil || err != nil {
			return getResp, err
		}
	}
	if len(opts) > 0 && v3.OpGet(key, opts...).IsSerializable() {
		return lkv.cl.Get(ctx, key, opts...)
	}
	resp, err := lkv.acquireLease(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	if !(v3.OpGet(key, opts...).IsKeysOnly()) {
		if len(opts) > 0 && (v3.OpGet(key, opts...).Rev()) < resp.Header.Revision {
			return lkv.cl.Get(ctx, key, opts...)
		}
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
	if li := lkv.leases.inCache(key); li != nil {
		rev = li.revision
	}
	if rev == 0 {
		return nil, nil
	}
	txnDel := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev)).Then(v3.OpDelete(key, opts...))
	respDel, errDel := txnDel.Commit()
	if errDel != nil {
		return nil, errDel
	}
	if respDel.Succeeded {
		lkv.leases.deleteKeyInCache(key)
		return (*v3.DeleteResponse)(respDel.Responses[0].GetResponseDeleteRange()), nil
	}
	return nil, nil
}

/* for delete range
func (lkv *leasingKV) acquireandAdd(ctx context.Context, key string) {
	resp := lkv.acquireLease(ctx, key)
	getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	getResp.Header = headerPopulate(resp.Header)
	lkv.leases.mu.Lock()
	lkv.leases.entries[key] = &leaseInfo{response: getResp,
		revision: resp.Header.Revision}
	defer lkv.leases.mu.Unlock()
}
*/
func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	for ctx.Err() == nil {
		respDel, err := lkv.deleteKey(ctx, key, opts...)
		if respDel != nil || err != nil {
			return respDel, err
		}
		/*// delete range - withprefix
		if len(opts) > 0 {
			ops := v3.OpGet(key, opts...)
			// check if opts is of type withprefix
			if string(ops.RangeBytes()) != string(ops.KeyBytes()) {
				getResp, _ := lkv.cl.Get(ctx, key, opts...)
				for i := range getResp.Kvs {
					if _, ok := lkv.leases.entries[string(getResp.Kvs[i].Key)]; !ok {
						lkv.acquireandAdd(ctx, string(getResp.Kvs[i].Key))
					}
				}
					//should make more modifications: - make delete range atomic using txns
					check for modrevision on last key in the range and delete - need more support
					for i := range getResp.Kvs {
						//lkv.deleteKey(ctx, string(getResp.Kvs[i].Key))
					}
			}
		}*/
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
	cs   []v3.Cmp
	opst []v3.Op
	opse []v3.Op
}

func (lkv *leasingKV) Txn(ctx context.Context) v3.Txn {
	return &txnLeasing{lkv.cl.Txn(ctx), lkv, make([]v3.Cmp, 0), make([]v3.Op, 0), make([]v3.Op, 0)}
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

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func (txn *txnLeasing) applyCmps() []bool {
	boolArray, notInCacheArray := make([]bool, 0), make([]v3.Cmp, 0)
	if txn.cs != nil {
		for itr := range txn.cs {
			if li := txn.lkv.leases.inCache(string(txn.cs[itr].Key)); li != nil {
				cacheResp := li.response
				var result int
				switch txn.cs[itr].Target {
				case server.Compare_VALUE:
					tv, _ := txn.cs[itr].TargetUnion.(*server.Compare_Value)
					if tv != nil {
						result = bytes.Compare(cacheResp.Kvs[0].Value, tv.Value)
					}
				case server.Compare_CREATE:
					tv, _ := txn.cs[itr].TargetUnion.(*server.Compare_CreateRevision)
					if tv != nil {
						result = compareInt64(cacheResp.Kvs[0].CreateRevision, tv.CreateRevision)
					}
				case server.Compare_MOD:
					tv, _ := txn.cs[itr].TargetUnion.(*server.Compare_ModRevision)
					if tv != nil {
						result = compareInt64(cacheResp.Kvs[0].ModRevision, tv.ModRevision)
					}
				case server.Compare_VERSION:
					tv, _ := txn.cs[itr].TargetUnion.(*server.Compare_Version)
					if tv != nil {
						result = compareInt64(cacheResp.Kvs[0].Version, tv.Version)
					}
				}
				resultbool := true
				switch txn.cs[itr].Result {
				case server.Compare_EQUAL:
					if result != 0 {
						resultbool = false
						break
					}
				case server.Compare_NOT_EQUAL:
					if result == 0 {
						resultbool = false
						break
					}
				case server.Compare_GREATER:
					if result != 1 {
						resultbool = false
						break
					}
				case server.Compare_LESS:
					if result != -1 {
						resultbool = false
						break
					}
				}
				boolArray = append(boolArray, resultbool)
			}
			if txn.lkv.leases.inCache(string(txn.cs[itr].Key)) == nil {
				notInCacheArray = append(notInCacheArray, txn.cs[itr])
			}
		}
		if len(txn.cs) == len(boolArray) {
			return boolArray
		}
		nicResp, _ := txn.lkv.cl.Txn((txn.lkv.cl.Ctx())).If(notInCacheArray...).Then().Commit()
		boolArray = append(boolArray, nicResp.Succeeded)
	}
	fmt.Println(boolArray)
	return boolArray
}

func returnRev(respArray []*server.ResponseOp) int64 {
	var rev int64
	for i := range respArray {
		if (*v3.GetResponse)(respArray[i].GetResponseRange()) != nil {
			if (*v3.GetResponse)(respArray[i].GetResponseRange()).Header.Revision > rev {
				rev = (*v3.GetResponse)(respArray[i].GetResponseRange()).Header.Revision
			}
		}
		if (*v3.PutResponse)(respArray[i].GetResponsePut()) != nil {
			if (*v3.PutResponse)(respArray[i].GetResponsePut()).Header.Revision > rev {
				rev = (*v3.PutResponse)(respArray[i].GetResponsePut()).Header.Revision
			}
		}
		if (*v3.DeleteResponse)(respArray[i].GetResponseDeleteRange()) != nil {
			if (*v3.DeleteResponse)(respArray[i].GetResponseDeleteRange()).Header.Revision > rev {
				rev = (*v3.DeleteResponse)(respArray[i].GetResponseDeleteRange()).Header.Revision
			}
		}
	}
	return rev
}

func (txn *txnLeasing) revokeLease(key string) bool {
	txn1 := txn.lkv.cl.Txn((txn.lkv.cl.Ctx())).If(v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), ">", 0))
	txn1 = txn1.Then(v3.OpPut(txn.lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
	revokeResp, _ := txn1.Commit()
	if revokeResp.Succeeded {
		return txn.lkv.watchforLKDel((txn.lkv.cl.Ctx()), key, revokeResp.Header.Revision)
	}
	return false
}

func (txn *txnLeasing) boolCmps() bool {
	boolArray := txn.applyCmps()
	if len(boolArray) == 0 {
		return true
	}
	if len(boolArray) > 1 {
		for i := range boolArray {
			if boolArray[i] == false {
				return false
			}
		}
	}
	return boolArray[0]
}

func (txn *txnLeasing) populateOpArray(opArray []v3.Op) ([]*server.ResponseOp, []v3.Op, []int, []v3.Cmp, []v3.Op) {
	elseOps, thenOps, pos := make([]v3.Op, 0), make([]v3.Op, 0), make([]int, 0)
	respOp, ifCmps, responseArray := &server.ResponseOp{}, make([]v3.Cmp, 0), make([]*server.ResponseOp, len(opArray))
	var rev int64
	for i := range opArray {
		key := string(opArray[i].KeyBytes())
		li := txn.lkv.leases.inCache(key)
		if li != nil {
			rev = li.revision
		}
		if li != nil && opArray[i].IsGet() {
			respOp = &server.ResponseOp{
				Response: &server.ResponseOp_ResponseRange{(*server.RangeResponse)(txn.lkv.leases.getCachedCopy(txn.lkv.ctx, key))},
			}
			responseArray[i] = respOp
		}
		if opArray[i].IsPut() || opArray[i].IsDelete() {
			ifCmps = append(ifCmps, v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), "=", rev))
			thenOps = append(thenOps, opArray[i])
			pos = append(pos, i)
			elseOps = append(elseOps, v3.OpGet(txn.lkv.pfx+key))
			responseArray[i] = nil
		}
		if li == nil && opArray[i].IsGet() {
			thenOps = append(thenOps, opArray[i])
			elseOps = append(elseOps, v3.OpGet(txn.lkv.pfx+key))
			pos = append(pos, i)
			responseArray[i] = nil
		}
	}
	return responseArray, thenOps, pos, ifCmps, elseOps
}

func (lkv *leasingKV) allInCache(responseArray []*server.ResponseOp, boolvar bool) (*v3.TxnResponse, error) {
	lkv.leases.mu.Lock()
	resp := (*v3.GetResponse)(responseArray[0].GetResponseRange())
	respHeader := &server.ResponseHeader{
		ClusterId: resp.Header.ClusterId,
		Revision:  lkv.header.Revision,
		MemberId:  resp.Header.MemberId,
		RaftTerm:  resp.Header.RaftTerm,
	}
	lkv.leases.mu.Unlock()
	return &v3.TxnResponse{
		Header:    respHeader,
		Succeeded: boolvar,
		Responses: responseArray,
	}, nil
}

func (txn *txnLeasing) computeCS() []v3.Cmp {
	compCS := make([]v3.Cmp, 0)
	if txn.cs != nil {
		for itr := range txn.cs {
			if li := txn.lkv.leases.inCache(string(txn.cs[itr].Key)); li != nil {
				compCS = append(compCS, txn.cs[itr])
			}
		}
	}
	txn.cs = nil
	for i := range compCS {
		txn.cs = append(txn.cs, compCS[i])
	}
	return compCS
}

func (txn *txnLeasing) recomputeCS() []v3.Cmp {
	recompCS := make([]v3.Cmp, 0)
	if txn.cs != nil {
		for itr := range txn.cs {
			if li := txn.lkv.leases.inCache(string(txn.cs[itr].Key)); li == nil {
				recompCS = append(recompCS, txn.cs[itr])
			}
		}
	}
	return recompCS
}

func (txn *txnLeasing) defOpArray(boolvar bool) []v3.Op {
	opArray := make([]v3.Op, 0)
	if boolvar && len(txn.opst) != 0 {
		opArray = append(opArray, txn.opst...)
	}
	if !boolvar && len(txn.opse) != 0 {
		opArray = append(opArray, txn.opse...)
	}
	return opArray
}

func (txn *txnLeasing) Commit() (*v3.TxnResponse, error) {
	var txnResp *v3.TxnResponse
	respHeader := &server.ResponseHeader{}
	var i int
	boolvar := txn.boolCmps()
	opArray := txn.defOpArray(boolvar)
	if len(opArray) == 0 {
		txn.lkv.leases.mu.Lock()
		txnResp = &v3.TxnResponse{
			Header:    txn.lkv.header,
			Succeeded: boolvar,
		}
		txn.lkv.leases.mu.Unlock()
		return txnResp, nil
	}
	responseArray, thenOps, pos, ifCmps, elseOps := txn.populateOpArray(opArray)
	if len(thenOps) == 0 && len(elseOps) == 0 {
		return txn.lkv.allInCache(responseArray, boolvar)
	}
	flag := true
	allCmps := make([]v3.Cmp, 0)
	txn.computeCS()
	for flag == true {
		allCmps = append(ifCmps, txn.cs...)
		resp, err := txn.lkv.cl.Txn((txn.lkv.cl.Ctx())).If(allCmps...).Then(thenOps...).Else(elseOps...).Commit()
		if err != nil {
			return nil, err
		}
		if !resp.Succeeded {
			for i = 0; i < len(ifCmps); i++ {
				key := string(ifCmps[i].Key)
				var rev int64
				if li := txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx)); li != nil {
					rev = li.revision
				}
				ifCmps[i] = v3.Compare(v3.CreateRevision(key), "=", rev)
			}
			recompCS := txn.recomputeCS()
			txn.cs = nil
			for i = range recompCS {
				txn.cs = append(txn.cs, recompCS[i])
			}
			allCmps = nil
			for i = 0; i < len(thenOps); i++ {
				key := string(thenOps[i].KeyBytes())
				response := (*v3.GetResponse)(resp.Responses[i].GetResponseRange())
				if li := txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx)); li == nil {
					if len(response.Kvs) != 0 && (thenOps[i].IsPut() || thenOps[i].IsDelete()) {
						txn.revokeLease(key)
					}
				}
			}
		}
		if resp.Succeeded {
			for i = 0; i < len(thenOps); i++ {
				key := string(thenOps[i].KeyBytes())
				li := txn.lkv.leases.inCache(key)
				if li != nil && thenOps[i].IsPut() {
					txn.lkv.leases.updateCacheResp(key, string(thenOps[i].ValueBytes()), resp.Header)
				}
				if li != nil && thenOps[i].IsDelete() {
					txn.lkv.deleteKey(txn.lkv.cl.Ctx(), key)
				}
				responseArray[pos[i]] = resp.Responses[i]
			}
			flag = false
			respHeader = &server.ResponseHeader{
				ClusterId: resp.Header.ClusterId,
				MemberId:  resp.Header.MemberId,
				RaftTerm:  resp.Header.RaftTerm,
			}
		}
	}
	respHeader.Revision = returnRev(responseArray)
	txnResp = &v3.TxnResponse{
		Header:    respHeader,
		Succeeded: boolvar,
		Responses: responseArray,
	}
	return txnResp, nil
}
