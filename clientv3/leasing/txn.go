package leasing

import (
	"bytes"

	v3 "github.com/coreos/etcd/clientv3"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
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

func (txn *txnLeasing) applyCmps() ([]bool, error) {
	boolArray, notInCacheArray := make([]bool, 0), make([]v3.Cmp, 0)
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
				boolArray = append(boolArray, resultbool)
			}
			if txn.lkv.leases.inCache(string(txn.cs[itr].Key)) == nil {
				notInCacheArray = append(notInCacheArray, txn.cs[itr])
			}
		}
		if len(txn.cs) == len(boolArray) {
			return boolArray, nil
		}
		nicResp, err := txn.lkv.cl.Txn(txn.ctx).If(notInCacheArray...).Then().Commit()
		if err != nil {
			return nil, err
		}
		boolArray = append(boolArray, nicResp.Succeeded)
	}
	return boolArray, nil
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
	txn1 := txn.lkv.cl.Txn(txn.ctx).If(v3.Compare(v3.CreateRevision(txn.lkv.pfx+key), ">", 0))
	txn1 = txn1.Then(v3.OpPut(txn.lkv.pfx+key, "REVOKE", v3.WithIgnoreLease()))
	revokeResp, err := txn1.Commit()
	if err != nil {
		return err
	}
	if revokeResp.Succeeded {
		txn.lkv.watchforLKDel(txn.ctx, key, revokeResp.Header.Revision)
		return nil
	}
	return nil
}

func (txn *txnLeasing) boolCmps() (bool, error) {
	boolArray, err := txn.applyCmps()
	if err != nil && boolArray == nil {
		return false, err
	}
	if len(boolArray) == 0 {
		return true, nil
	}
	if len(boolArray) > 1 {
		for i := range boolArray {
			if boolArray[i] == false {
				return false, nil
			}
		}
	}
	return boolArray[0], nil
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
				Response: &server.ResponseOp_ResponseRange{(*server.RangeResponse)(txn.lkv.leases.getCachedCopy(txn.ctx, key))},
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
