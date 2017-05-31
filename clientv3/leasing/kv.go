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
	flag    int64
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
	return &leasingKV{cl: cl, pfx: leasingprefix, session: s, leases: leaseCache{entries: make(map[string]*leaseInfo), flag: 0}, ctx: cctx, cancel: cancel}, nil
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
		//fmt.Println("Entering goroutine")

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

	} else {
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
	boolArray := make([]bool, len(txn.cs))
	boolArray = nil
	if txn.cs != nil {
		for itr := range txn.cs {
			if _, ok := txn.lkv.leases.entries[string(txn.cs[itr].Key)]; ok {
				cacheResp := txn.lkv.leases.entries[string(txn.cs[itr].Key)].response
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
		}
	}
	return boolArray
}

//return header with largest rev number
func returnHeader(getRespArray []*v3.GetResponse, putRespArray []*v3.PutResponse) *server.ResponseHeader {
	respHeader := &server.ResponseHeader{}
	rev := int64(0)
	for i := range getRespArray {
		if getRespArray[i].Header.Revision > rev {
			rev = getRespArray[i].Header.Revision
			respHeader = getRespArray[i].Header
		}
	}

	for i := range putRespArray {
		if putRespArray[i].Header.Revision > rev {
			rev = putRespArray[i].Header.Revision
			respHeader = putRespArray[i].Header
		}
	}
	return respHeader
}

func (txn *txnLeasing) Commit() (*v3.TxnResponse, error) {
	boolvar := true
	boolArray := txn.applyCmps()

	if len(boolArray) == 1 {
		boolvar = boolArray[0]
	}

	// more than one comparison
	if len(boolArray) > 1 {
		for i := range boolArray {
			if boolArray[i] == false {
				boolvar = false
				break
			}
		}
	}

	var txnResp *v3.TxnResponse
	getRespArray := make([]*v3.GetResponse, 0)
	putRespArray := make([]*v3.PutResponse, 0)
	var i int
	responseArray := make([]*server.ResponseOp, 0)
	respOp := &server.ResponseOp{}
	opArray := make([]v3.Op, 0)

	//copy either opst or opse into opArray
	if boolvar {
		opArray = append([]v3.Op(nil), txn.opst...)
		//fmt.Println(opArray)
	}

	if !boolvar && len(txn.opse) != 0 {
		opArray = append([]v3.Op(nil), txn.opse...)
		//copy(opArray, txn.opse)
	}

	//append responseOp's for get/put operations to responsearray
	for i = range opArray {
		if (opArray[i]).IsGet() { //opget
			//check if present in cache
			if _, ok := txn.lkv.leases.entries[string(opArray[i].KeyBytes())]; ok {
				getRespArray = append(getRespArray, (txn.lkv).leases.getCachedCopy(string(opArray[i].KeyBytes())))
			}

			respOp = &server.ResponseOp{
				Response: &server.ResponseOp_ResponseRange{(*server.RangeResponse)(getRespArray[i])},
			}
		}
		//opput
		if (opArray[i]).IsPut() {
			var putresp *v3.PutResponse
			//if in cache
			if _, ok := txn.lkv.leases.entries[string(opArray[i].KeyBytes())]; ok {
				pr, err := txn.lkv.updateKey(txn.lkv.cl.Ctx(), string(opArray[i].KeyBytes()), string(opArray[i].ValueBytes()))
				putresp = pr

				if err != nil {
					return nil, err
				}
			}

			//not in cache - has to revoke (nonowner)
			if _, ok := txn.lkv.leases.entries[string(opArray[i].KeyBytes())]; !ok {
				pr, err := txn.lkv.Put(txn.lkv.cl.Ctx(), string(opArray[i].KeyBytes()), string(opArray[i].ValueBytes()))
				putresp = pr

				if err != nil {
					return nil, err
				}
			}

			putRespArray = append(putRespArray, putresp)
			respOp = &server.ResponseOp{
				Response: &server.ResponseOp_ResponsePut{(*server.PutResponse)(putRespArray[i])},
			}
		}
		responseArray = append(responseArray, respOp)
	}

	// latest rev - response details
	respHeader := returnHeader(getRespArray, putRespArray)
	header := headerPopulate(respHeader)
	//populate txnresp and return
	txnResp = &v3.TxnResponse{
		Header:    header,
		Succeeded: boolvar,
		Responses: responseArray,
	}

	fmt.Printf("txnresp %+v \n", txnResp)
	return txnResp, nil
}
