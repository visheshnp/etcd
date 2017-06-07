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
	"fmt"

	"golang.org/x/net/context"

	"sync"

	"strings"

	v3 "github.com/coreos/etcd/clientv3"
	concurrency "github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type leasingKV struct {
	cl      *v3.Client
	pfx     string
	session *concurrency.Session
	leases  leaseCache
	ctx     context.Context
	cancel  context.CancelFunc
}

type leaseCache struct {
	entries map[string]*leaseInfo
	mu      sync.Mutex
}

type leaseInfo struct {
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
	return &leasingKV{cl: cl, pfx: leasingprefix, session: s, leases: leaseCache{entries: make(map[string]*leaseInfo)}, ctx: cctx, cancel: cancel}, nil
}

func (lkv *leasingKV) Compact(ctx context.Context, rev int64, opts ...v3.CompactOption) (*v3.CompactResponse, error) {
	return lkv.cl.Compact(ctx, rev, opts...)
}

func (lkv *leasingKV) Do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	panic("Stub")
}

//update cache's getresponse
func (lc *leaseCache) update(key, val string, respHeader *server.ResponseHeader) {

	var mapResp *v3.GetResponse
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if _, ok := lc.entries[key]; ok {
		mapResp = lc.entries[key].response
	}

	if mapResp == nil {
		return
	}
	//initialize KV struct and append to response if key doesn't exist
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

	// if key present, just update value and revision
	if len(mapResp.Kvs) > 0 {
		if mapResp.Kvs[0].ModRevision < respHeader.Revision {
			mapResp.Header = respHeader
			mapResp.Kvs[0].Value = []byte(val)
			mapResp.Kvs[0].ModRevision = respHeader.Revision
		}
		//lc.entries[key].response.Kvs[0].Version = 0 - tackle later on delete
		mapResp.Kvs[0].Version++
	}
}

//populates header from the TxnResponse
func headerPopulate(respHeader *server.ResponseHeader) *server.ResponseHeader {
	header := &server.ResponseHeader{
		ClusterId: respHeader.ClusterId,
		MemberId:  respHeader.MemberId,
		Revision:  respHeader.Revision,
		RaftTerm:  respHeader.RaftTerm,
	}
	return header
}

func (lkv *leasingKV) updateKey(ctx context.Context, key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {
	var rev int64
	lkv.leases.mu.Lock()
	if li := lkv.leases.entries[key]; li != nil {
		rev = li.revision
	}
	lkv.leases.mu.Unlock()
	if rev == 0 {
		return nil, nil
	}

	//if leasing key revision matches with client's rev in map
	txnUpd := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev))
	txnUpd = txnUpd.Then(v3.OpPut(key, val, opts...))
	respUpd, errUpd := txnUpd.Commit()

	if errUpd != nil {
		//	lkv.leases.mu.Lock()
		//	delete(lkv.leases.entries, key)
		//	lkv.leases.mu.Unlock()
		return nil, errUpd
	}

	if !respUpd.Succeeded {
		return nil, nil
	}

	lkv.leases.mu.Lock()
	_, isKey := lkv.leases.entries[key]
	lkv.leases.mu.Unlock()

	if isKey {
		lkv.leases.update(key, val, respUpd.Header)
	}
	putResp := (*v3.PutResponse)(respUpd.Responses[0].GetResponsePut())
	lkv.leases.mu.Lock()
	Header := headerPopulate(respUpd.Header)
	putResp.Header = Header
	lkv.leases.mu.Unlock()
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
		//if leasing key already exist in map, then update key
		respPut, err := lkv.updateKey(ctx, key, val, opts...)
		if respPut != nil || err != nil {
			return respPut, err
		}

		lkv.leases.mu.Lock()
		_, isPresent := lkv.leases.entries[key]
		lkv.leases.mu.Unlock()

		if !isPresent {
			//lk doesn't exist
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
			txn = txn.Then(v3.OpPut(key, val, opts...)) //no leasing key assosciated with key, so put normally
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()

			if err != nil {
				return nil, err
			}

			if resp.Succeeded {
				lkv.leases.mu.Lock()
				header := headerPopulate(resp.Header)
				lkv.leases.mu.Unlock()
				resput := resp.Responses[0].GetResponsePut()
				response := (*v3.PutResponse)(resput)
				response.Header = header
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
					txn := lkv.cl.Txn(nCtx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev))
					txn = txn.Then(v3.OpDelete(lkv.pfx + key))
					delResp, err := txn.Commit()

					if delResp.Succeeded {
						lkv.leases.mu.Lock()
						delete(lkv.leases.entries, key) //delete from map as well
						defer lkv.leases.mu.Unlock()
						return
					}

					if err != nil || !delResp.Succeeded { //retry
						continue
					}

				}
			}
		}
	}
}

//return copy of getresponse from cache
func (lc *leaseCache) getCachedCopy(key string) *v3.GetResponse {
	var resp *v3.GetResponse
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if _, ok := lc.entries[key]; ok {
		resp = lc.entries[key].response
	}

	if resp == nil {
		return nil
	}

	header := headerPopulate(resp.Header)

	var keyCopy []byte
	var valCopy []byte
	var kvs []*mvccpb.KeyValue

	if len(resp.Kvs) > 0 {
		keyCopy = make([]byte, len(resp.Kvs[0].Key))
		valCopy = make([]byte, len(resp.Kvs[0].Value))
		copy(keyCopy, resp.Kvs[0].Key)
		copy(valCopy, resp.Kvs[0].Value)

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

	if len(resp.Kvs) == 0 {
		kvs = nil
	}

	copyResp := &v3.GetResponse{
		Header: header,
		Kvs:    kvs,
		More:   resp.More,
		Count:  resp.Count,
	}
	return copyResp
}

func (lkv *leasingKV) appendMap(getresp *v3.GetResponse, key string) {
	//update response map with latest val,rev of key
	lkv.leases.mu.Lock()
	lkv.leases.entries[key] = &leaseInfo{response: getresp,
		revision: getresp.Header.Revision}
	lkv.leases.mu.Unlock()
}

func (lkv *leasingKV) acquireLease(ctx context.Context, key string, opts ...v3.OpOption) *v3.TxnResponse {
	//assigns lk and insert into map
	txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
	txn = txn.Then(v3.OpGet(key, opts...), v3.OpPut(lkv.pfx+key, "", v3.WithLease(lkv.session.Lease())))

	//returns val when no leasing key is assosciated
	txn = txn.Else(v3.OpGet(key, opts...))
	resp, err := txn.Commit()

	if err != nil {
		panic("Error in transaction")
	}
	return resp
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if len(key) == 0 {
		return nil, rpctypes.ErrEmptyKey
	}

	var gresp *v3.GetResponse
	//return cached value if lk exists
	getResp, err := lkv.leases.getCachedCopy(key), ctx.Err()
	if getResp != nil || err != nil {
		return getResp, err
	}

	//if range of keys exist
	ops := v3.OpGet(key, opts...)
	if len(opts) > 0 {
		size := ops.RangeBytes()
		if len(size) > 0 {
			return lkv.cl.Get(ctx, key, opts...)
		}
	}
	resp := lkv.acquireLease(ctx, key, opts...)

	if resp.Succeeded {
		if len(opts) > 0 {
			ops := v3.OpGet(key, opts...)
			//check for historical revision
			if ops.Rev() < resp.Header.Revision {
				return lkv.cl.Get(ctx, key, opts...)
			}
		}

		getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
		header := headerPopulate(resp.Header)
		getResp.Header = header
		lkv.appendMap(getResp, key)
		rev := resp.Header.Revision

		//go routine - waiting for revoke message
		go lkv.monitorLease(ctx, key, resp, rev)
		gresp = lkv.leases.getCachedCopy(key)
	}
	if !resp.Succeeded {
		gresp = (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	}
	return gresp, nil
}

func (lkv *leasingKV) deleteKey(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	//if leasing key revision matches with client's rev in map
	var rev int64
	lkv.leases.mu.Lock()
	if li := lkv.leases.entries[key]; li != nil {
		rev = li.revision
	}
	lkv.leases.mu.Unlock()
	if rev == 0 {
		return nil, nil
	}

	txnDel := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev))
	txnDel = txnDel.Then(v3.OpDelete(key, opts...))
	respDel, errDel := txnDel.Commit()

	if errDel != nil {
		return nil, errDel
	}

	if respDel.Succeeded {
		lkv.leases.mu.Lock()
		defer lkv.leases.mu.Unlock()
		delete(lkv.leases.entries, key)
		delresp := (*v3.DeleteResponse)(respDel.Responses[0].GetResponseDeleteRange())
		return delresp, nil
	}
	return nil, nil
}

/* for delete range
func (lkv *leasingKV) acquireandAdd(ctx context.Context, key string) {
	resp := lkv.acquireLease(ctx, key)
	header := headerPopulate(resp.Header)
	getResp := (*v3.GetResponse)(resp.Responses[0].GetResponseRange())
	getResp.Header = header
	lkv.leases.mu.Lock()
	lkv.leases.entries[key] = &leaseInfo{response: getResp,
		revision: resp.Header.Revision}
	defer lkv.leases.mu.Unlock()
}
*/
func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	//fmt.Printf("+++++++++++++DELETE OUT%#v\n", lkv.leases.entries)

	for ctx.Err() == nil {
		//if leasing key already exist in map, then delete key

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

				//acquire lease and add to map

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

		lkv.leases.mu.Lock()
		_, isPresent := lkv.leases.entries[key]
		lkv.leases.mu.Unlock()

		if !isPresent {
			//lk doesn't exist
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
			txn = txn.Then(v3.OpDelete(key, opts...)) //no leasing key assosciated with key, so del normally

			//if non owner
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()

			if err != nil {
				return nil, err
			}

			if resp.Succeeded {
				response := (*v3.DeleteResponse)(resp.Responses[0].GetResponseDeleteRange())
				return response, nil
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

//perform comparisons for IF statement
func (txn *txnLeasing) applyCmps() []bool {
	boolArray := make([]bool, 0)
	nicArray := make([]v3.Cmp, 0)
	boolArray, nicArray = nil, nil
	if txn.cs != nil {
		for itr := range txn.cs {

			txn.lkv.leases.mu.Lock()
			li, ok := txn.lkv.leases.entries[string(txn.cs[itr].Key)]
			txn.lkv.leases.mu.Unlock()

			if ok {
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

			if !ok {
				nicArray = append(nicArray, txn.cs[itr])
			}
		}

		if len(txn.cs) == len(boolArray) {
			return boolArray
		}

		txn1 := txn.lkv.cl.Txn((txn.lkv.cl.Ctx())).If(nicArray...)
		txn1 = txn1.Then()
		nicResp, _ := txn1.Commit()

		if !nicResp.Succeeded {
			boolArray = append(boolArray, false)
		}
		if nicResp.Succeeded {
			boolArray = append(boolArray, true)
		}

	}
	return boolArray
}

//return header with largest rev number
func returnRev(respArray []*server.ResponseOp) int64 {
	rev := int64(0)
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

func (lc *leaseCache) inCache(key string) *leaseInfo {
	lc.mu.Lock()
	li := lc.entries[key]
	lc.mu.Unlock()
	return li
}

func (txn *txnLeasing) revokeLease(key string) bool {
	txn1 := txn.lkv.cl.Txn((txn.lkv.cl.Ctx())).If(v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), ">", 0))
	txn1 = txn1.Then(v3.OpPut(txn.lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
	revokeResp, _ := txn1.Commit()

	if revokeResp.Succeeded {
		return txn.lkv.watchforLKDel((txn.lkv.cl.Ctx()), key, revokeResp.Header.Revision)
	}

	if !revokeResp.Succeeded {
		return false
	}
	return true
}

func (txn *txnLeasing) clientTxn(allCmps []v3.Cmp, thenOps []v3.Op, elseOps []v3.Op) (*v3.TxnResponse, error) {
	txn1 := txn.lkv.cl.Txn((txn.lkv.cl.Ctx())).If(allCmps...)
	txn1 = txn1.Then(thenOps...)
	txn1 = txn1.Else(elseOps...)
	resp, err := txn1.Commit()

	return resp, err
}

func (txn *txnLeasing) boolCmps() bool {
	boolArray := txn.applyCmps()

	if len(boolArray) == 1 {
		return boolArray[0]
	}

	// more than one comparison
	if len(boolArray) > 1 {
		for i := range boolArray {
			if boolArray[i] == false {
				return false
			}
		}
	}
	return true
}

func (txn *txnLeasing) populateOpArray(opArray []v3.Op) ([]*server.ResponseOp, []v3.Op, []int, []v3.Cmp, []v3.Op) {
	elseOps := make([]v3.Op, 0)
	thenOps := make([]v3.Op, 0)
	pos := make([]int, 0)
	respOp := &server.ResponseOp{}
	ifCmps := make([]v3.Cmp, 0)
	responseArray := make([]*server.ResponseOp, len(opArray))
	for i := range opArray {
		key := string(opArray[i].KeyBytes())
		li := txn.lkv.leases.inCache(key)
		if li != nil && opArray[i].IsGet() {
			respOp = &server.ResponseOp{
				Response: &server.ResponseOp_ResponseRange{(*server.RangeResponse)(txn.lkv.leases.getCachedCopy(key))},
			}
			responseArray[i] = respOp
		}
		//ifCmps - if either opPut or not in cache then populate thenOps and ifCmps
		rev := int64(0)
		txn.lkv.leases.mu.Lock()
		if li != nil {
			rev = li.revision
		}
		txn.lkv.leases.mu.Unlock()

		if opArray[i].IsPut() || opArray[i].IsDelete() {
			ifCmps = append(ifCmps, v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), "=", rev))
			thenOps = append(thenOps, opArray[i]) // opArray that has to go to store
			pos = append(pos, i)
			elseOps = append(elseOps, v3.OpGet(txn.lkv.pfx+key))
			responseArray[i] = nil
		}

		if li == nil && opArray[i].IsGet() {
			thenOps = append(thenOps, opArray[i])
			elseOps = append(elseOps, v3.OpGet(txn.lkv.pfx+key)) // opArray that has to go to store
			pos = append(pos, i)
			responseArray[i] = nil
		}

	}
	return responseArray, thenOps, pos, ifCmps, elseOps
}

func allInCache(responseArray []*server.ResponseOp, boolvar bool) (*v3.TxnResponse, error) {
	var txnResp *v3.TxnResponse
	rev := returnRev(responseArray)
	resp := (*v3.GetResponse)(responseArray[0].GetResponseRange())
	respHeader := &server.ResponseHeader{
		ClusterId: resp.Header.ClusterId,
		Revision:  rev,
		MemberId:  resp.Header.MemberId,
		RaftTerm:  resp.Header.RaftTerm,
	}
	txnResp = &v3.TxnResponse{
		Header:    respHeader,
		Succeeded: boolvar,
		Responses: responseArray,
	}
	return txnResp, nil
}

func (txn *txnLeasing) computeCS() []v3.Cmp {
	compCS := make([]v3.Cmp, 0)
	if txn.cs != nil {
		for itr := range txn.cs {
			li := txn.lkv.leases.inCache(string(txn.cs[itr].Key))
			if li != nil {
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
			li := txn.lkv.leases.inCache(string(txn.cs[itr].Key))
			if li == nil {
				recompCS = append(recompCS, txn.cs[itr])
			}
		}
	}
	return recompCS
}

func (txn *txnLeasing) defOpArray(boolvar bool) []v3.Op {
	var opArray []v3.Op
	//copy either opst or opse into opArray
	if boolvar {
		opArray = append(opArray, txn.opst...)
	}
	//opse
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

	fmt.Println("boolvar", boolvar)
	fmt.Println("opse", txn.opse)
	fmt.Println("opst", txn.opst)
	fmt.Println("cs", txn.cs)

	//if cond is false, and no else
	if !boolvar && len(txn.opse) == 0 {
		txnResp = &v3.TxnResponse{
			Succeeded: boolvar,
		}
		return txnResp, nil
	}

	opArray := txn.defOpArray(boolvar)
	responseArray, thenOps, pos, ifCmps, elseOps := txn.populateOpArray(opArray)

	//empty ops
	if len(opArray) == 0 {
		tresp, terr := txn.lkv.cl.Txn(txn.lkv.cl.Ctx()).If().Then().Commit()
		if terr != nil {
			return nil, terr
		}
		return tresp, nil
	}

	//if all in cache, return before performing lkv Txn via base client
	if len(thenOps) == 0 {
		return allInCache(responseArray, boolvar)
	}

	//Perform Txn
	flag := true
	allCmps := make([]v3.Cmp, 0)

	txn.computeCS()
	//repeat until success
	for flag == true {
		allCmps = append(ifCmps, txn.cs...)
		resp, err := txn.clientTxn(allCmps, thenOps, elseOps)

		if err != nil {
			return nil, err
		}

		if !resp.Succeeded {
			//update cmps
			for i = 0; i < len(ifCmps); i++ {
				key := string(ifCmps[i].Key)
				li := txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx))

				var rev int64
				if li != nil {
					rev = li.revision
				}
				ifCmps[i] = v3.Compare(v3.CreateRevision(key), "=", rev)
			}

			//recompute cs
			recompCS := txn.recomputeCS()
			txn.cs = nil
			for i = range recompCS {
				txn.cs = append(txn.cs, recompCS[i])
			}
			allCmps = nil

			//non-owner sends REVOKE // Invalidate owner
			for i = 0; i < len(thenOps); i++ {
				key := string(thenOps[i].KeyBytes())
				response := (*v3.GetResponse)(resp.Responses[i].GetResponseRange())
				li := txn.lkv.leases.inCache(key)

				if li == nil && len(response.Kvs) != 0 && (thenOps[i].IsPut() || thenOps[i].IsDelete()) {
					txn.revokeLease(key)
				}
			}
		}

		if resp.Succeeded {
			for i = 0; i < len(thenOps); i++ {
				key := string(thenOps[i].KeyBytes())
				li := txn.lkv.leases.inCache(key)
				if li != nil && thenOps[i].IsPut() {
					txn.lkv.leases.update(key, string(thenOps[i].ValueBytes()), resp.Header)
				}

				if li != nil && thenOps[i].IsDelete() {
					txn.lkv.deleteKey(txn.lkv.cl.Ctx(), key)
				}
				//populate responseArray
				responseArray[pos[i]] = resp.Responses[i]
			}
			flag = false
			//populate respHeader
			respHeader = &server.ResponseHeader{
				ClusterId: resp.Header.ClusterId,
				MemberId:  resp.Header.MemberId,
				RaftTerm:  resp.Header.RaftTerm,
			}
		}
	}

	// latest rev
	respHeader.Revision = returnRev(responseArray)

	//populate txnResp and return
	txnResp = &v3.TxnResponse{
		Header:    respHeader,
		Succeeded: boolvar,
		Responses: responseArray,
	}

	fmt.Printf("txnresp %+v \n\n", txnResp)
	return txnResp, nil
}
