package leasing

import (
	"bytes"
	"context"
	"strings"

	v3 "github.com/coreos/etcd/clientv3"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

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

func (txn *txnLeasing) allCmpsfromCache() (bool, bool) {
	serverTxnBool, cacheBool, cacheCount := false, true, 0
	for _, cmp := range txn.cs {
		if len(string(cmp.RangeEnd)) > 0 {
			return false, true
		}
	}
	if txn.cs != nil {
		for itr := range txn.cs {
			if li := txn.lkv.leases.checkInCache(string(txn.cs[itr].Key)); li != nil {
				cacheResp := li.response
				var result int
				if len(cacheResp.Kvs) != 0 {
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
				if resultbool == false {
					cacheBool = resultbool
				}
				cacheCount++
			}
		}
		if len(txn.cs) != cacheCount {
			serverTxnBool = true
		}
	}
	return cacheBool, serverTxnBool
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

func (lkv *leasingKV) revokeLease(ctx context.Context, key string) error {
	var err error
	txn1 := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(v3.OpGet(key))
	revokeResp, err := txn1.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease())).Commit()
	if err != nil {
		return err
	}
	if !revokeResp.Succeeded {
		lkv.watchforLKDel(ctx, key, revokeResp.Header.Revision)
	}
	return err
}

func (txn *txnLeasing) noOps(opArray []v3.Op, cacheBool bool) (bool, *v3.TxnResponse, error) {
	var txnResp *v3.TxnResponse
	noOp := len(opArray) == 0
	if noOp {
		if txn.lkv.header != nil {
			txn.lkv.leases.mu.Lock()
			txnResp = &v3.TxnResponse{
				Header:    txn.lkv.header,
				Succeeded: cacheBool,
			}
			txn.lkv.leases.mu.Unlock()
			return noOp, txnResp, nil
		}
		resp, err := txn.lkv.cl.Txn(txn.ctx).If().Then().Commit()
		if err != nil {
			return noOp, nil, err
		}
		return noOp, resp, nil
	}
	return noOp, nil, nil
}

func (txn *txnLeasing) cacheOpArray(opArray []v3.Op) ([]*server.ResponseOp, bool) {
	respOp, responseArray, opCount := &server.ResponseOp{}, make([]*server.ResponseOp, len(opArray)), 0
	for i := range opArray {
		key := string(opArray[i].KeyBytes())
		if len(string(opArray[i].RangeBytes())) > 0 {
			return responseArray, false
		}
		if txn.lkv.leases.entries[key] != nil && opArray[i].IsGet() {
			respOp = &server.ResponseOp{
				Response: &server.ResponseOp_ResponseRange{(*server.RangeResponse)(txn.lkv.leases.entries[key].response)},
			}
			responseArray[i] = respOp
			opCount++
		}
	}
	return responseArray, opCount == len(opArray)
}

func (txn *txnLeasing) cmpUpdate(opArray []v3.Op) ([]v3.Op, []v3.Cmp) {
	isPresent, elseOps, cmps := make(map[string]bool), make([]v3.Op, 0), make([]v3.Cmp, 0)
	var rev int64
	for i := range opArray {
		key := string(opArray[i].KeyBytes())
		li := txn.lkv.leases.checkInCache(key)
		if li != nil {
			rev = li.revision
		} else {
			rev = 0
		}
		if opArray[i].IsGet() {
			continue
		}
		if !isPresent[txn.lkv.pfx+key] {
			cmps, elseOps = append(cmps, v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), "=", rev)), append(elseOps, v3.OpGet(txn.lkv.pfx+key))
			isPresent[txn.lkv.pfx+key] = true
		}
	}
	return elseOps, cmps
}

func (lkv *leasingKV) allInCache(responseArray []*server.ResponseOp, boolvar bool) (*v3.TxnResponse, error) {
	resp := (*v3.GetResponse)(responseArray[0].GetResponseRange())
	return &v3.TxnResponse{
		Header: &server.ResponseHeader{
			ClusterId: resp.Header.ClusterId,
			Revision:  lkv.header.Revision,
			MemberId:  resp.Header.MemberId,
			RaftTerm:  resp.Header.RaftTerm,
		},
		Succeeded: boolvar,
		Responses: responseArray,
	}, nil
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

func (txn *txnLeasing) extractResp(resp *v3.TxnResponse) *v3.TxnResponse {
	var txnResp *v3.TxnResponse
	responseArray := make([]*server.ResponseOp, 0)
	for i := range resp.Responses[0].GetResponseTxn().Responses {
		responseArray = append(responseArray, resp.Responses[0].GetResponseTxn().Responses[i])
	}
	txnResp = &v3.TxnResponse{
		Header:    resp.Header,
		Succeeded: resp.Responses[0].GetResponseTxn().Succeeded,
		Responses: responseArray,
	}
	return txnResp
}

func (txn *txnLeasing) modifyCacheTxn(txnResp *v3.TxnResponse) {
	var temp []v3.Op
	if txnResp.Succeeded && len(txn.opst) != 0 {
		temp = txn.gatherOps(txnResp.Responses[0], txn.opst)
	}
	if !txnResp.Succeeded && len(txn.opse) != 0 {
		temp = txn.gatherOps(txnResp.Responses[0], txn.opse)
	}
	txn.lkv.leases.mu.Lock()
	for i := range temp {
		li := txn.lkv.leases.entries[string(temp[i].KeyBytes())]
		if li != nil && temp[i].IsPut() {
			liResp := li.response
			if liResp.Kvs[0].ModRevision < txnResp.Header.Revision {
				kvs := []*mvccpb.KeyValue{
					&mvccpb.KeyValue{
						Key:            temp[i].KeyBytes(),
						CreateRevision: liResp.Kvs[0].CreateRevision,
						ModRevision:    txnResp.Header.Revision,
						Version:        liResp.Kvs[0].Version,
						Value:          temp[i].ValueBytes(),
						Lease:          liResp.Kvs[0].Lease,
					},
				}
				liResp = &v3.GetResponse{
					Header: respHeaderPopulate(txnResp.Header),
					Kvs:    kvs,
					More:   liResp.More,
					Count:  liResp.Count,
				}
			}
			liResp.Kvs[0].Version++
			li.response = liResp
		}
		if li != nil && temp[i].IsDelete() {
			delete(txn.lkv.leases.entries, string(temp[i].KeyBytes()))
		}
	}
	txn.lkv.leases.mu.Unlock()
}

func (txn *txnLeasing) gatherOps(resp *server.ResponseOp, myOps []v3.Op) []v3.Op {
	allOps := make([]v3.Op, 0)
	if len(myOps) == 0 {
		return allOps
	}
	for i := range myOps {
		if !myOps[i].IsTxn() {
			allOps = append(allOps, myOps[i])
		}
		if myOps[i].IsTxn() {
			_, thenOps, elseOps := myOps[i].Txn()
			if resp.GetResponseTxn().Succeeded {
				allOps = append(allOps, txn.gatherOps(resp.GetResponseTxn().Responses[i], thenOps)...)
			} else {
				allOps = append(allOps, txn.gatherOps(resp.GetResponseTxn().Responses[i], elseOps)...)
			}
		}
	}
	return allOps
}

func (txn *txnLeasing) gatherAllOps(myOps []v3.Op) []v3.Op {
	allOps := make([]v3.Op, 0)
	if len(myOps) == 0 {
		return allOps
	}
	for i := range myOps {
		if !myOps[i].IsTxn() {
			allOps = append(allOps, myOps[i])
		}
		if myOps[i].IsTxn() {
			_, thenOps, elseOps := myOps[i].Txn()
			ops := append(thenOps, elseOps...)
			allOps = append(allOps, txn.gatherAllOps(ops)...)
		}
	}
	return allOps
}

func (txn *txnLeasing) NonOwnerRevoke(resp *v3.TxnResponse, elseOps []v3.Op, txnOps []v3.Op) error {
	mapResp := make(map[string]bool)
	for i := range elseOps {
		key := string(elseOps[i].KeyBytes())
		if len((*v3.GetResponse)(resp.Responses[i].GetResponseRange()).Kvs) != 0 {
			mapResp[(strings.TrimPrefix(key, txn.lkv.pfx))] = true
		}
	}
	for i := range txnOps {
		key := string(txnOps[i].KeyBytes())
		if li := txn.lkv.leases.checkInCache(strings.TrimPrefix(key, txn.lkv.pfx)); li == nil {
			if mapResp[key] && (txnOps[i].IsPut() || txnOps[i].IsDelete()) {
				return txn.lkv.revokeLease(txn.ctx, key)
			}
		}
	}
	return nil
}

const (
	acquireChan     int = 1
	waitReleaseChan int = 2
)

func (lc *leaseCache) blockKeys(ops []v3.Op, chanModify int) []chan struct{} {
	var wcs [](chan struct{})
	lc.mu.Lock()
	switch chanModify {
	case waitReleaseChan:
		for _, op := range ops {
			key := string(op.KeyBytes())
			li := lc.entries[key]
			if li != nil && op.IsGet() {
				wcs = append(wcs, li.waitc)
			}
		}
	case acquireChan:
		for _, op := range ops {
			if op.IsPut() || op.IsDelete() {
				key := string(op.KeyBytes())
				li := lc.entries[key]
				if li != nil {
					li.waitc = make(chan struct{})
					wcs = append(wcs, li.waitc)
				}
			}
		}

	}
	lc.mu.Unlock()
	return wcs
}
