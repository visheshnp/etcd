package leasing

import (
	"sync"
	"time"

	v3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	server "github.com/coreos/etcd/etcdserver/etcdserverpb"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
)

type leaseCache struct {
	monitorRevocation map[string]time.Time
	entries           map[string]*leaseInfo
	mu                sync.Mutex
	flag              int
}

type leaseInfo struct {
	waitc    chan struct{}
	response *v3.GetResponse
	revision int64
}

func (lc *leaseCache) checkInCache(key string) *leaseInfo {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	return lc.entries[key]
}

func (lc *leaseCache) deleteKeyInCache(key string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if li := lc.entries[key]; li != nil {
		delete(lc.entries, key)
	}
}

func (lc *leaseCache) openWaitChannel(key string) (chan struct{}, int64) {
	li := lc.checkInCache(key)
	if li != nil {
		lc.mu.Lock()
		li.waitc = make(chan struct{})
		wc, rev := li.waitc, li.revision
		lc.mu.Unlock()
		return wc, rev
	}
	return nil, 0
}

func (lc *leaseCache) updateResp(key, val string, respHeader *server.ResponseHeader) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
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
	if mapResp.Kvs[0].ModRevision < respHeader.Revision {
		mapResp.Header, mapResp.Kvs[0].Value = respHeader, []byte(val)
		mapResp.Kvs[0].ModRevision = respHeader.Revision
	}
	mapResp.Kvs[0].Version++
}

const (
	leasingRevokeBackoff time.Duration = time.Second * time.Duration(2)
)

func (lc *leaseCache) trackRevokedLK(key string) {
	lc.mu.Lock()
	lc.monitorRevocation[key] = time.Now()
	lc.mu.Unlock()
}

func (lc *leaseCache) returnWaitChannel(key string) (*leaseInfo, chan struct{}) {
	li := lc.checkInCache(key)
	if li != nil {
		lc.mu.Lock()
		wc := li.waitc
		lc.mu.Unlock()
		return li, wc
	}
	return li, nil
}

func (lc *leaseCache) returnCachedResp(ctx context.Context, key string, op v3.Op) *v3.GetResponse {
	var resp *v3.GetResponse
	li, wc := lc.returnWaitChannel(key)
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
	var kvsnil bool
	if len(resp.Kvs) == 0 || op.IsCountOnly() || (op.MaxModRev() != 0 && op.MaxModRev() <= resp.Kvs[0].ModRevision) ||
		(op.MaxCreateRev() != 0 && op.MaxCreateRev() <= resp.Kvs[0].CreateRevision) ||
		(op.MinModRev() != 0 && op.MinModRev() >= resp.Kvs[0].ModRevision) ||
		(op.MinCreateRev() != 0 && op.MinCreateRev() >= resp.Kvs[0].CreateRevision) {
		kvsnil = true
	}
	var kvs []*mvccpb.KeyValue
	if len(resp.Kvs) > 0 && !kvsnil {
		keyCopy = make([]byte, len(resp.Kvs[0].Key))
		copy(keyCopy, resp.Kvs[0].Key)
		if !op.IsKeysOnly() {
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

func (lkv *leasingKV) checkOpenSession() bool {
	lkv.leases.mu.Lock()
	defer lkv.leases.mu.Unlock()
	select {
	case <-lkv.session.Done():
	default:
		return true
	}
	return false
}

func (lkv *leasingKV) addToCache(getresp *v3.GetResponse, key string) {
	lkv.leases.mu.Lock()
	waitc := make(chan struct{})
	close(waitc)
	lkv.leases.entries[key] = &leaseInfo{waitc: waitc, response: getresp, revision: getresp.Header.Revision}
	if lkv.maxRev < getresp.Header.Revision {
		lkv.header = getresp.Header
	}
	lkv.leases.mu.Unlock()
}

func (lkv *leasingKV) leaseID() v3.LeaseID {
	lkv.leases.mu.Lock()
	defer lkv.leases.mu.Unlock()
	return lkv.session.Lease()
}

func (lkv *leasingKV) initializeSession(s *concurrency.Session) {
	lkv.leases.mu.Lock()
	lkv.session = s
	close(lkv.sessionc)
	lkv.leases.mu.Unlock()
}

func maxCreateRev(getResp *v3.GetResponse) int64 {
	var maxRev int64
	for i := range getResp.Kvs {
		if maxRev < getResp.Kvs[i].CreateRevision {
			maxRev = getResp.Kvs[i].CreateRevision
		}
	}
	return maxRev
}

func maxModRev(getResp *v3.GetResponse) int64 {
	var maxRev int64
	for i := range getResp.Kvs {
		if maxRev < getResp.Kvs[i].ModRevision {
			maxRev = getResp.Kvs[i].ModRevision
		}
	}
	return maxRev
}

func startNewSession(cl *v3.Client, ttl int) (*concurrency.Session, error) {
	var s *concurrency.Session
	var err error
	if ttl > 0 {
		s, err = concurrency.NewSession(cl, concurrency.WithTTL(ttl))
	} else {
		s, err = concurrency.NewSession(cl)
	}
	return s, err
}

func respHeaderPopulate(respHeader *server.ResponseHeader) *server.ResponseHeader {
	return &server.ResponseHeader{
		ClusterId: respHeader.ClusterId,
		MemberId:  respHeader.MemberId,
		Revision:  respHeader.Revision,
		RaftTerm:  respHeader.RaftTerm,
	}
}

func closeWaitChannel(wc []chan struct{}) {
	for i := range wc {
		close(wc[i])
	}
}

func (lc *leaseCache) clearRevocationMap(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			lc.mu.Lock()
			for k, lr := range lc.monitorRevocation {
				if time.Now().Sub(lr.Add(leasingRevokeBackoff)) > 0 {
					delete(lc.monitorRevocation, k)
				}
			}
			lc.mu.Unlock()
		}
	}
}
