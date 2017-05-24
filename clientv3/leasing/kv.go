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

	//initialize KV struct and append to response if key doesn't exist
	if len(lc.entries[key].response.Kvs) == 0 {
		lc.mu.Lock()
		myKV := &mvccpb.KeyValue{
			Value:   []byte(val),
			Key:     []byte(key),
			Version: 0,
		}
		lc.entries[key].response.Kvs = append(lc.entries[key].response.Kvs, myKV)
		//fmt.Println(myKV.Value)
		lc.entries[key].response.More = false
		lc.entries[key].response.Count = 1
		lc.entries[key].response.Kvs[0].CreateRevision = respHeader.Revision
		lc.mu.Unlock()
	}

	// if key present, just update value and revision (//race condition tackle)
	if len(lc.entries[key].response.Kvs) > 0 {
		lc.mu.Lock()
		//fmt.Printf("revnum %+v \n", respHeader.Revision)
		if lc.entries[key].response.Kvs[0].ModRevision < respHeader.Revision {
			lc.entries[key].response.Header = respHeader
			lc.entries[key].response.Kvs[0].Value = []byte(val)
			lc.entries[key].response.Kvs[0].ModRevision = respHeader.Revision
		}
		//lc.entries[key].response.Kvs[0].Version = 0 - tackle later on delete
		lc.entries[key].response.Kvs[0].Version++
		lc.mu.Unlock()
	}
}

func (lkv *leasingKV) updateKey(ctx context.Context, key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {
	//if leasing key revision matches with client's rev in map
	txnUpd := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", lkv.leases.entries[key].revision))
	txnUpd = txnUpd.Then(v3.OpPut(key, val, opts...))
	respUpd, errUpd := txnUpd.Commit()

	if errUpd != nil {
		return nil, errUpd
	}

	if respUpd.Succeeded {
		lkv.leases.update(key, val, respUpd.Header)
		putresp := (*v3.PutResponse)(respUpd.Responses[0].GetResponsePut())
		return putresp, nil
	}
	return nil, nil
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
		if _, ok := lkv.leases.entries[key]; ok {
			putresp, err := lkv.updateKey(ctx, key, val, opts...)
			return putresp, err
		}

		//lk doesn't exist
		if _, ok := lkv.leases.entries[key]; !ok {
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
			txn = txn.Then(v3.OpPut(key, val, opts...)) //no leasing key assosciated with key, so put normally
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()

			if err != nil {
				return nil, err
			}

			if resp.Succeeded {
				resput := resp.Responses[0].GetResponsePut()
				response := (*v3.PutResponse)(resput)
				return response, err
			}
			lkv.watchforLKDel(ctx, key, resp.Header.Revision)
		}

	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) monitorLease(ctx context.Context, key string, resp *v3.TxnResponse, rev int64) {
	nctx, cancel := context.WithCancel(lkv.ctx)
	defer cancel()

	for nctx.Err() == nil {
		wch := lkv.cl.Watch(nctx, lkv.pfx+key, v3.WithRev(resp.Header.Revision+1))
		fmt.Println("Entering goroutine")

		for resp := range wch {
			for _, ev := range resp.Events {

				if string(ev.Kv.Value) == "REVOKE" {
					txn := lkv.cl.Txn(nctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev))
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

func (lc *leaseCache) getCachedCopy(key string) *v3.GetResponse {
	resp := lc.entries[key].response
	header := &server.ResponseHeader{
		ClusterId: resp.Header.ClusterId,
		MemberId:  resp.Header.MemberId,
		Revision:  resp.Header.Revision,
		RaftTerm:  resp.Header.RaftTerm,
	}
	var keyslice []byte
	var valslice []byte
	var kvsslice []*mvccpb.KeyValue

	if len(resp.Kvs) > 0 {
		keyslice = make([]byte, len(resp.Kvs[0].Key))
		valslice = make([]byte, len(resp.Kvs[0].Value))
		copy(keyslice, resp.Kvs[0].Key)
		copy(valslice, resp.Kvs[0].Value)

		kvsslice = []*mvccpb.KeyValue{
			&mvccpb.KeyValue{
				Key:            keyslice,
				CreateRevision: resp.Kvs[0].CreateRevision,
				ModRevision:    resp.Kvs[0].ModRevision,
				Version:        resp.Kvs[0].Version,
				Value:          valslice,
				Lease:          resp.Kvs[0].Lease,
			},
		}
	}

	if len(resp.Kvs) == 0 {
		kvsslice = nil
	}

	copyResp := &v3.GetResponse{
		Header: header,
		Kvs:    kvsslice,
		More:   resp.More,
		Count:  resp.Count,
	}
	return copyResp
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if len(key) == 0 {
		return nil, rpctypes.ErrEmptyKey
	}

	var gresp *v3.GetResponse
	//return cached value if lk exists
	if _, ok := lkv.leases.entries[key]; ok {
		return lkv.leases.getCachedCopy(key), ctx.Err()
	}

	//if range of keys exist
	ops := v3.OpGet(key, opts...)
	if len(opts) > 0 {
		size := ops.RangeBytes()
		if len(size) > 0 {
			return lkv.cl.Get(ctx, key, opts...)
		}
	}

	//assigns lk
	txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
	txn = txn.Then(v3.OpGet(key), v3.OpPut(lkv.pfx+key, "", v3.WithLease(lkv.session.Lease())))
	//returns val when no leasing key is assosciated
	txn = txn.Else(v3.OpGet(key))
	resp, err := txn.Commit()

	if err != nil {
		panic("Error in transaction")
	}

	if resp.Succeeded {
		//if historical version is queried, pass through to client and don't update map
		//	fmt.Printf("opts %+v \n", ops)
		if len(opts) > 0 {
			revnum := ops.Rev()
			//check for older revisions
			if revnum < resp.Header.Revision {
				return lkv.cl.Get(ctx, key, opts...)
			}
		}

		//update response map with latest val,rev of key
		lkv.leases.mu.Lock()
		lkv.leases.entries[key] = &leaseInfo{response: (*v3.GetResponse)(resp.Responses[0].GetResponseRange()),
			revision: resp.Header.Revision}
		defer lkv.leases.mu.Unlock()

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
	txnDel := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", lkv.leases.entries[key].revision))
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

func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	for ctx.Err() == nil {
		//if leasing key already exist in map, then delete key
		//fmt.Println(lkv.leases.entries)
		if _, ok := lkv.leases.entries[key]; ok {
			delresp, err := lkv.deleteKey(ctx, key, opts...)
			return delresp, err
		}

		// delete range - withprefix
		if len(opts) > 0 {
			ops := v3.OpGet(key, opts...)
			// check if opts is of type withprefix
			if string(ops.RangeBytes()) != string(ops.KeyBytes()) {
				for keyMap := range lkv.leases.entries {
					if strings.Contains(keyMap, key) {
						lkv.leases.mu.Lock()
						delete(lkv.leases.entries, keyMap)
						lkv.cl.Delete(ctx, keyMap, opts...)
						lkv.leases.mu.Unlock()
					}
				}
			}
			return nil, ctx.Err()
		}

		//lk doesn't exist
		if _, ok := lkv.leases.entries[key]; !ok {
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
			txn = txn.Then(v3.OpDelete(key, opts...)) //no leasing key assosciated with key, so del normally
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()

			if err != nil {
				return nil, err
			}

			if resp.Succeeded {
				resdel := resp.Responses[0].GetResponseDeleteRange()
				response := (*v3.DeleteResponse)(resdel)
				return response, nil
			}
			lkv.watchforLKDel(ctx, key, resp.Header.Revision)
		}
	}
	return nil, ctx.Err()
}

type txnLeasing struct {
	v3.Txn
	lkv *leasingKV
}

func (lkv *leasingKV) Txn(ctx context.Context) v3.Txn {
	return &txnLeasing{lkv.cl.Txn(ctx), lkv}
}
