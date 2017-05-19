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

	"golang.org/x/net/context"

	"sync"

	v3 "github.com/coreos/etcd/clientv3"
	concurrency "github.com/coreos/etcd/clientv3/concurrency"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type leasingKV struct {
	cl           *v3.Client
	pfx          string
	session      *concurrency.Session
	leaseInfomap map[string]*leaseInfo
	ctx          context.Context
	cancel       context.CancelFunc
	mu           *sync.Mutex
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
	return &leasingKV{cl: cl, pfx: leasingprefix, session: s, leaseInfomap: make(map[string]*leaseInfo), ctx: cctx, cancel: cancel, mu: new(sync.Mutex)}, nil
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

func (lkv *leasingKV) InitKV(key, val string) *mvccpb.KeyValue {
	myKV := &mvccpb.KeyValue{
		Value: []byte(val),
		Key:   []byte(key),
	}
	return myKV
}

func (lkv *leasingKV) Put(ctx context.Context, key, val string, opts ...v3.OpOption) (*v3.PutResponse, error) {

	for ctx.Err() == nil {

		//if already exist in map, then update key
		if _, ok := lkv.leaseInfomap[lkv.pfx+key]; ok {

			txnUpd := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", lkv.leaseInfomap[lkv.pfx+key].revision))
			txnUpd = txnUpd.Then(v3.OpPut(key, val))
			respUpd, errUpd := txnUpd.Commit()

			if errUpd != nil {
				panic("Error in transaction")
			}

			if respUpd.Succeeded {
				lkv.mu.Lock()
				//if key doesn't exist
				if len(lkv.leaseInfomap[lkv.pfx+key].response.Kvs) == 0 {
					myKV := lkv.InitKV(key, val)
					lkv.leaseInfomap[lkv.pfx+key].response.Kvs = append(lkv.leaseInfomap[lkv.pfx+key].response.Kvs, myKV)
				}

				// if key present, just update value ( what else to update, revisions?)
				if len(lkv.leaseInfomap[lkv.pfx+key].response.Kvs) > 0 {
					lkv.leaseInfomap[lkv.pfx+key].response.Kvs[0].Value = []byte(val)

				}
				lkv.mu.Unlock()
				break

			}

		} else { // OTHER client
			//new client - no leasing key
			txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
			txn = txn.Then(v3.OpPut(key, val)) //should this be happening? check if other clients have same leasing key?
			//has leasing key, wants to put new value - sends revoke to clients holding leasing key
			txn = txn.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
			resp, err := txn.Commit()

			if err != nil {
				panic("Error in transaction")
			}

			if resp.Succeeded {
				lkv.mu.Lock()
				lkv.InitKV(key, val)
				lkv.mu.Unlock()

				resput := resp.Responses[0].GetResponsePut()
				response := (*v3.PutResponse)(resput)
				return response, nil
			}

			cctx, cancel := context.WithCancel(ctx)
			wch := lkv.cl.Watch(cctx, lkv.pfx+key, v3.WithRev(resp.Header.Revision+1))
			fmt.Println("Entering loop")
			for resp := range wch {
				fmt.Printf("%+v\n", resp)
				for _, ev := range resp.Events {
					if ev.Type == v3.EventTypeDelete {
						cancel()
						// New client should put its updated value, after having other client revoke lease? should not be breaking.
						break
					}
				}
			}
		}

	}
	return nil, ctx.Err()
}

func (lkv *leasingKV) Get(ctx context.Context, key string, opts ...v3.OpOption) (*v3.GetResponse, error) {

	if len(key) == 0 {
		return nil, rpctypes.ErrEmptyKey
	}

	//return cached value
	if li, ok := lkv.leaseInfomap[lkv.pfx+key]; ok {
		return li.response, nil
	}

	txn := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0))
	//assigns leasing key
	txn = txn.Then(v3.OpGet(key), v3.OpPut(lkv.pfx+key, "", v3.WithLease(lkv.session.Lease())))
	//should we be doing this? ( already have cached value)
	txn = txn.Else(v3.OpGet(key))
	resp, err := txn.Commit()

	if err != nil {
		panic("Error in transaction")
	}

	if resp.Succeeded {
		lkv.mu.Lock()
		lkv.leaseInfomap[lkv.pfx+key] = &leaseInfo{response: (*v3.GetResponse)(resp.Responses[0].GetResponseRange()), revision: resp.Header.Revision}
		defer lkv.mu.Unlock()
		rev := resp.Header.Revision

		//go routine - waiting for revoke message
		go func() {
			nctx, cancel := context.WithCancel(lkv.ctx)
			defer cancel()
			wch := lkv.cl.Watch(nctx, lkv.pfx+key, v3.WithRev(resp.Header.Revision+1))
			fmt.Println("Entering goroutine")

			for resp := range wch {
				fmt.Printf("%+v\n", resp)
				for _, ev := range resp.Events {
					fmt.Printf("%+v\n", ev)
					if string(ev.Kv.Value) == "REVOKE" { //if val is entered as revoke
						txn := lkv.cl.Txn(nctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", rev))
						txn = txn.Then(v3.OpDelete(lkv.pfx + key))
						delResp, err := txn.Commit()

						if err != nil {
							//panic("Error")

						}

						if delResp.Succeeded {
							fmt.Println("Delete success")
							lkv.mu.Lock()
							delete(lkv.leaseInfomap, lkv.pfx+key) //delete from map as well
							defer lkv.mu.Unlock()
							return
						}

						//if delResp.!Succeeded {

						//}

					}
				}

			}
		}()
	}

	resprange := resp.Responses[0].GetResponseRange()
	response := (*v3.GetResponse)(resprange)
	return response, nil
}
