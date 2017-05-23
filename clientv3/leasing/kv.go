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
	"reflect"

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

func (lkv *leasingKV) Delete(ctx context.Context, key string, opts ...v3.OpOption) (*v3.DeleteResponse, error) {
	panic("Stub")
}

func (lkv *leasingKV) Do(ctx context.Context, op v3.Op) (v3.OpResponse, error) {
	panic("Stub")
}

func (lkv *leasingKV) Txn(ctx context.Context) v3.Txn {
	panic("Stub")
}

//update cache's getresponse
func (lc *leaseCache) update(key, val string, respHeader *server.ResponseHeader) {
	lc.mu.Lock()
	//initialize KV struct and append to response if key doesn't exist
	if len(lc.entries[key].response.Kvs) == 0 {
		myKV := &mvccpb.KeyValue{
			Value:   []byte(val),
			Key:     []byte(key),
			Version: 0,
		}
		lc.entries[key].response.Kvs = append(lc.entries[key].response.Kvs, myKV)
		fmt.Println(myKV.Value)
		lc.entries[key].response.More = false
		lc.entries[key].response.Count = 1
		lc.entries[key].response.Kvs[0].CreateRevision = respHeader.Revision
	}

	// if key present, just update value and revision (//race condition tackle)
	if len(lc.entries[key].response.Kvs) > 0 {
		lc.entries[key].response.Header = respHeader
		lc.entries[key].response.Kvs[0].Value = []byte(val)
		lc.entries[key].response.Kvs[0].ModRevision = respHeader.Revision

		//lc.entries[key].response.Kvs[0].Version = 0 - tackle later on delete
		lc.entries[key].response.Kvs[0].Version++

	}
	lc.mu.Unlock()
}

func (lkv *leasingKV) updateKey(ctx context.Context, key, val string, opts ...v3.OpOption) *v3.PutResponse {
	//if leasing key revision matches with client's rev in map
	txnUpd := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", lkv.leases.entries[key].revision))
	txnUpd = txnUpd.Then(v3.OpPut(key, val, opts...))
	respUpd, errUpd := txnUpd.Commit()

	if errUpd != nil {
		panic("Error in transaction")
	}

	if respUpd.Succeeded {
		lkv.leases.update(key, val, respUpd.Header)
		putresp := (*v3.PutResponse)(respUpd.Responses[0].GetResponsePut())
		return putresp
	}
	return nil
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
			putresp := lkv.updateKey(ctx, key, val, opts...)
			return putresp, ctx.Err()
		}

		//lk doesn't exist
		if _, ok := lkv.leases.entries[key]; !ok {
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
			txn = txn.Then(v3.OpPut(key, val, opts...)) //no leasing key assosciated with key, so put normally
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()

			if err != nil {
				panic("Error in transaction")
			}

			if resp.Succeeded {
				resput := resp.Responses[0].GetResponsePut()
				response := (*v3.PutResponse)(resput)
				return response, nil
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

func (lc *leaseCache) CloneValue(source interface{}, destin interface{}) {
	x := reflect.ValueOf(source)
	if x.Kind() == reflect.Ptr {
		starX := x.Elem()
		y := reflect.New(starX.Type())
		starY := y.Elem()
		starY.Set(starX)
		reflect.ValueOf(destin).Elem().Set(y.Elem())
	} else {
		destin = x.Interface()
	}
}

func (lc *leaseCache) getCachedCopy(key string) *v3.GetResponse {
	resp := lc.entries[key].response
	var copyResp *v3.GetResponse = &v3.GetResponse{}
	lc.CloneValue(resp, copyResp)
	return copyResp
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {
	if len(key) == 0 {
		return nil, rpctypes.ErrEmptyKey
	}

	var gresp *v3.GetResponse
	//return cached value if lk exists
	if _, ok := lkv.leases.entries[key]; ok {
		gresp = lkv.leases.getCachedCopy(key)
		return gresp, nil
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
			revnum := ops.RevisionNum()
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
