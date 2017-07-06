package leasing

import (
	"bytes"
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
	if txn.cs != nil {
		for itr := range txn.cs {
			if li := txn.lkv.leases.inCache(string(txn.cs[itr].Key)); li != nil {
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

func (txn *txnLeasing) revokeLease(key string) error {
	var err error
	txn1 := txn.lkv.cl.Txn(txn.ctx).If(v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), ">", 0))
	txn1 = txn1.Then(v3.OpPut(txn.lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
	revokeResp, err := txn1.Commit()
	if err != nil {
		return err
	}
	if revokeResp.Succeeded {
		txn.lkv.watchforLKDel(txn.ctx, key, revokeResp.Header.Revision)
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
		return noOp, resp, err
	}
	return noOp, nil, nil
}

func (txn *txnLeasing) cacheOpArray(opArray []v3.Op) ([]*server.ResponseOp, bool) {
	respOp, responseArray, opCount := &server.ResponseOp{}, make([]*server.ResponseOp, len(opArray)), 0
	for i := range opArray {
		key := string(opArray[i].KeyBytes())
		li := txn.lkv.leases.inCache(key)
		if li != nil && opArray[i].IsGet() {
			respOp = &server.ResponseOp{
				Response: &server.ResponseOp_ResponseRange{(*server.RangeResponse)(txn.lkv.leases.getCachedCopy(txn.ctx, key,
					v3.OpGet(key)))},
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
		li := txn.lkv.leases.inCache(key)
		if li != nil {
			rev = li.revision
		}
		if li == nil {
			rev = 0
		}
		if opArray[i].IsGet() {
			continue
		}
		if !isPresent[txn.lkv.pfx+key] {
			cmps = append(cmps, v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), "=", rev))
			elseOps = append(elseOps, v3.OpGet(txn.lkv.pfx+key))
			isPresent[txn.lkv.pfx+key] = true
		}
	}
	return elseOps, cmps
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
	respHeader, responseArray := &server.ResponseHeader{}, make([]*server.ResponseOp, 0)
	for i := range resp.Responses[0].GetResponseTxn().Responses {
		responseArray = append(responseArray, resp.Responses[0].GetResponseTxn().Responses[i])
	}
	respHeader.Revision = returnRev(responseArray)
	respHeader = &server.ResponseHeader{
		ClusterId: resp.Header.ClusterId,
		MemberId:  resp.Header.MemberId,
		RaftTerm:  resp.Header.RaftTerm,
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
		temp = txn.gatherAllOps(txn.opst)
	}
	if !txnResp.Succeeded && len(txn.opse) != 0 {
		temp = txn.gatherAllOps(txn.opse)
	}
	for i := range temp {
		key := string(temp[i].KeyBytes())
		li := txn.lkv.leases.inCache(key)
		if li != nil && temp[i].IsPut() {
			txn.lkv.leases.updateCacheValue(key, string(temp[i].ValueBytes()), txnResp.Header)
		}
		if li != nil && temp[i].IsDelete() {
			txn.lkv.deleteKey(txn.ctx, key, v3.OpDelete(txn.lkv.pfx+key))
		}
	}
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
		if li := txn.lkv.leases.inCache(strings.TrimPrefix(key, txn.lkv.pfx)); li == nil {
			if mapResp[key] && (txnOps[i].IsPut() || txnOps[i].IsDelete()) {
				return txn.revokeLease(key)
			}
		}
	}
	return nil
}

func (lc *leaseCache) updateCacheValue(key, val string, respHeader *server.ResponseHeader) {
	var wc chan struct{}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.entries[key].waitc = make(chan struct{})
	wc = lc.entries[key].waitc
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
	close(wc)
}
