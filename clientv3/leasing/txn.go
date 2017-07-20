package leasing

import (
	"bytes"
	"context"
	"strings"

	v3 "github.com/coreos/etcd/clientv3"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
)

type txnLeasing struct {
	v3.Txn
	lkv  *leasingKV
	ctx  context.Context
	cs   []v3.Cmp
	opst []v3.Op
	opse []v3.Op
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

func (txn *txnLeasing) allCmpsfromCache() (bool, bool) {
	serverTxnBool, cacheBool, cacheCount := false, true, 0
	if txn.cs == nil {
		return cacheBool, serverTxnBool
	}
	for _, cmp := range txn.cs {
		if len(string(cmp.RangeEnd)) > 0 {
			return false, true
		}
	}
	for itr := range txn.cs {
		li := txn.lkv.leases.checkInCache(string(txn.cs[itr].Key))
		if li == nil {
			return false, true
		}
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
	if len(txn.cs) != cacheCount {
		serverTxnBool = true
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
	txn1 := lkv.cl.Txn(ctx).If(v3.Compare(v3.CreateRevision(lkv.pfx+key), "=", 0)).Then(v3.OpGet(key))
	revokeResp, err := txn1.Else(v3.OpPut(lkv.pfx+key, "REVOKE", v3.WithIgnoreLease())).Commit()
	if err != nil {
		return err
	}
	if !revokeResp.Succeeded {
		lkv.waitForLKDel(ctx, key, revokeResp.Header.Revision)
	}
	return nil
}

func (txn *txnLeasing) noOps(opArray []v3.Op, cacheBool bool) (bool, *v3.TxnResponse, error) {
	noOp := len(opArray) == 0
	if noOp {
		if txn.lkv.header != nil {
			txn.lkv.leases.mu.Lock()
			txnResp := &v3.TxnResponse{
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

func (txn *txnLeasing) serveOpsFromCache(opArray []v3.Op) ([]*server.ResponseOp, bool) {
	respOp, responseArray := &server.ResponseOp{}, make([]*server.ResponseOp, len(opArray))
	for i := range opArray {
		key := string(opArray[i].KeyBytes())
		if len(string(opArray[i].RangeBytes())) > 0 || txn.lkv.leases.entries[key] == nil || opArray[i].IsPut() || opArray[i].IsDelete() {
			return responseArray, false
		}
		respOp = &server.ResponseOp{
			Response: &server.ResponseOp_ResponseRange{(*server.RangeResponse)(txn.lkv.leases.entries[key].response)},
		}
		responseArray[i] = respOp
	}
	return responseArray, true
}

func (txn *txnLeasing) cmpUpdate(opArray []v3.Op) ([]v3.Op, []v3.Cmp, error) {
	isPresent, elseOps, cmps, rev := make(map[string]bool), make([]v3.Op, 0), make([]v3.Cmp, 0), int64(0)
	for i, op := range opArray {
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
			if len(op.RangeBytes()) == 0 {
				cmps, elseOps = append(cmps, v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), "=", rev)), append(elseOps, v3.OpGet(txn.lkv.pfx+key))
				isPresent[txn.lkv.pfx+key] = true
			} else {
				_, maxRevLK, err := txn.lkv.maxRevLK(txn.ctx, string(op.KeyBytes()), op)
				if err != nil {
					return nil, nil, err
				}
				getResp, err := txn.lkv.kv.Get(txn.ctx, key, v3.WithRange(string(op.RangeBytes())))
				if err != nil {
					return nil, nil, err
				}
				maxRev, leasingendKey := maxModRev(getResp), v3.GetPrefixRangeEnd(txn.lkv.pfx+key)
				cmps = append(cmps, v3.Compare(v3.ModRevision(key).WithRange(string(op.RangeBytes())), "<", maxRev+1))
				cmps = append(cmps, v3.Compare(v3.CreateRevision(txn.lkv.pfx+key).WithRange(leasingendKey), "<", maxRevLK+1))
			}
		}
	}
	return elseOps, cmps, nil
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
	responseArray := make([]*server.ResponseOp, 0)
	for i := range resp.Responses[0].GetResponseTxn().Responses {
		responseArray = append(responseArray, resp.Responses[0].GetResponseTxn().Responses[i])
	}
	return &v3.TxnResponse{
		Header:    resp.Header,
		Succeeded: resp.Responses[0].GetResponseTxn().Succeeded,
		Responses: responseArray,
	}
}

func (txn *txnLeasing) modifyCacheAfterTxn(txnResp *v3.TxnResponse) {
	var temp []v3.Op
	if txnResp.Succeeded && len(txn.opst) != 0 {
		temp = txn.gatherOps(txnResp.Responses[0], txn.opst)
	}
	if !txnResp.Succeeded && len(txn.opse) != 0 {
		temp = txn.gatherOps(txnResp.Responses[0], txn.opse)
	}
	txn.lkv.leases.mu.Lock()
	for i := range temp {
		if len(string(temp[i].RangeBytes())) > 0 {
			for k := range txn.lkv.leases.entries {
				if strings.HasPrefix(k, string(temp[i].KeyBytes())) {
					delete(txn.lkv.leases.entries, k)
				}
			}
		}
		li := txn.lkv.leases.entries[string(temp[i].KeyBytes())]
		if li == nil {
			continue
		}
		if temp[i].IsPut() {
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
		if temp[i].IsDelete() {
			delete(txn.lkv.leases.entries, string(temp[i].KeyBytes()))
		}
	}
	txn.lkv.leases.mu.Unlock()
}

func (txn *txnLeasing) gatherOps(resp *server.ResponseOp, myOps []v3.Op) []v3.Op {
	allOps := make([]v3.Op, 0)
	for i := range myOps {
		if !myOps[i].IsTxn() {
			allOps = append(allOps, myOps[i])
		} else {
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
	for i := range myOps {
		if !myOps[i].IsTxn() {
			allOps = append(allOps, myOps[i])
		} else {
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

func (lc *leaseCache) blockKeysWaitChan(ops []v3.Op) []chan struct{} {
	var wcs [](chan struct{})
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, op := range ops {
		if li := lc.entries[string(op.KeyBytes())]; li != nil && op.IsGet() {
			wcs = append(wcs, li.waitc)
		}
	}
	return wcs
}

func (lkv *leasingKV) waitChanAcquire(ctx context.Context, ops []v3.Op) ([]chan struct{}, error) {
	var wcs [](chan struct{})
	for _, op := range ops {
		if op.IsGet() {
			continue
		}
		if len(string(op.RangeBytes())) > 0 {
			resp, err := lkv.kv.Get(ctx, string(op.KeyBytes()), v3.WithRange(string(op.RangeBytes())))
			if err != nil {
				return nil, err
			}
			for i := range resp.Kvs {
				if wc, _ := lkv.leases.openWaitChannel(string(resp.Kvs[i].Key)); wc != nil {
					wcs = append(wcs, wc)
				}
			}
		}
		if wc, _ := lkv.leases.openWaitChannel(string(op.KeyBytes())); wc != nil {
			wcs = append(wcs, wc)
		}
	}
	return wcs, nil
}

func (txn *txnLeasing) cacheTxn(serverTxnBool bool, cacheBool bool) (*v3.TxnResponse, error) {
	opArray := txn.gatherAllOps(txn.defOpArray(cacheBool))
	if ok, txnResp, err := txn.noOps(opArray, cacheBool); ok {
		if err != nil {
			return nil, err
		}
		return txnResp, nil
	}
	for _, ch := range txn.lkv.leases.blockKeysWaitChan(opArray) {
		select {
		case <-ch:
		case <-txn.ctx.Done():
			return nil, txn.ctx.Err()
		}
	}
	txn.lkv.leases.mu.Lock()
	responseArray, ok := txn.serveOpsFromCache(opArray)
	txn.lkv.leases.mu.Unlock()
	if ok {
		if !txn.lkv.checkOpenSession() {
			return txn.lkv.cl.Txn(txn.ctx).If(txn.cs...).Then(txn.opst...).Else(txn.opse...).Commit()
		}
		if txn.lkv.checkCtxCancel(txn.ctx) {
			return nil, txn.ctx.Err()
		}
		cacheResp, _ := txn.lkv.allInCache(responseArray, cacheBool)
		return cacheResp, nil
	}
	return nil, nil
}

func (txn *txnLeasing) serverTxn(txnOps []v3.Op, wcs []chan struct{}) (*v3.TxnResponse, error) {
	var txnResp *v3.TxnResponse
	for {
		elseOps, cmps, err := txn.cmpUpdate(txnOps)
		if err != nil {
			return nil, err
		}
		resp, err := txn.lkv.kv.Txn(txn.ctx).If(cmps...).Then(v3.OpTxn(txn.cs, txn.opst, txn.opse)).Else(elseOps...).Commit()
		if err != nil {
			for i := range cmps {
				txn.lkv.leases.deleteKeyInCache(strings.TrimPrefix(string(cmps[i].Key), txn.lkv.pfx))
			}
			return nil, err
		}
		if resp.Succeeded {
			txnResp = txn.extractResp(resp)
			txn.modifyCacheAfterTxn(txnResp)
			closeWaitChannel(wcs)
			break
		}
		err = txn.NonOwnerRevoke(resp, elseOps, txnOps)
		if err != nil {
			return nil, err
		}
	}
	return txnResp, nil
}
