// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// //     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/leasing"
	"github.com/coreos/etcd/integration"
	"github.com/coreos/etcd/pkg/testutil"
)

func TestLeasingPutGet1(t *testing.T) {
	defer testutil.AfterTest(t)

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	c1 := clus.Client(0)
	c2 := clus.Client(1)
	//c3 := clus.Client(2)
	lKV1, err := leasing.NewleasingKV(c1, "foo/")
	lKV2, err := leasing.NewleasingKV(c2, "foo/")
	//lKV3, err := leasing.NewleasingKV(c3, "foo/")

	resp1, err := lKV1.Get(context.TODO(), "abc")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := lKV1.Put(context.TODO(), "abc", "def"); err != nil {
		t.Fatal(err)
	}

	resp2, err := lKV2.Get(context.TODO(), "abc")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := lKV1.Put(context.TODO(), "abc", "ghi"); err != nil {
		t.Fatal(err)
	}

	resp3, err := lKV2.Get(context.TODO(), "abc")
	if err != nil {
		t.Fatal(err)
	}

	if len(resp1.Kvs) != 0 {
		t.Errorf("expected value=%q, got value=%q", "0", len(resp1.Kvs))
	}

	if string(resp2.Kvs[0].Key) != "abc" {
		t.Errorf("expected key=%q, got key=%q", "abc", resp2.Kvs[0].Key)
	}

	if string(resp2.Kvs[0].Value) != "def" {
		t.Errorf("expected value=%q, got value=%q", "bar", resp2.Kvs[0].Value)
	}

	if string(resp3.Kvs[0].Key) != "abc" {
		t.Errorf("expected key=%q, got key=%q", "abc", resp3.Kvs[0].Key)
	}

	if string(resp3.Kvs[0].Value) != "ghi" {
		t.Errorf("expected value=%q, got value=%q", "bar", resp3.Kvs[0].Value)
	}

}

func TestLeasingPutGet2(t *testing.T) {
	defer testutil.AfterTest(t)

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	c := clus.Client(0)
	lKV, err := leasing.NewleasingKV(c, "foo/")

	_, err = lKV.Get(context.TODO(), "abc")
	if err != nil {
		t.Fatal(err)
	}
}

// TestLeasingInterval checks the leasing KV fetches key intervals.
func TestLeasingInterval(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"abc/a", "abc/b", "abc/c"}
	for _, k := range keys {
		if _, err := clus.Client(0).Put(context.TODO(), k, "v"); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := lkv.Get(context.TODO(), "abc/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 3 {
		t.Fatalf("expected keys %+v, got response keys %+v", keys, resp.Kvs)
	}
}

// TestLeasingPutInvalidateNew checks the leasing KV updates its cache on a Put to a new key.
func TestLeasingPutInvalidateNew(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Put(context.TODO(), "k", "v"); err != nil {
		t.Fatal(err)
	}

	lkvResp, err := lkv.Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}
	cResp, cerr := clus.Client(0).Get(context.TODO(), "k")
	if cerr != nil {
		t.Fatal(cerr)
	}
	if !reflect.DeepEqual(lkvResp, cResp) {
		t.Fatalf(`expected %+v, got response %+v`, cResp, lkvResp)
	}
}

// TestLeasingPutInvalidateExisting checks the leasing KV updates its cache on a Put to an existing key.
func TestLeasingPutInvalidatExisting(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Put(context.TODO(), "k", "v"); err != nil {
		t.Fatal(err)
	}

	lkvResp, err := lkv.Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}
	cResp, cerr := clus.Client(0).Get(context.TODO(), "k")
	if cerr != nil {
		t.Fatal(cerr)
	}
	if !reflect.DeepEqual(lkvResp, cResp) {
		t.Fatalf(`expected %+v, got response %+v`, cResp, lkvResp)
	}
}

// TestLeasingGetSerializable checks the leasing KV can make serialized requests
// when the etcd cluster is partitioned.
/*
func TestLeasingGetSerializable(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 2})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := clus.Client(0).Put(context.TODO(), "cached", "abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Get(context.TODO(), "cached"); err != nil {
		t.Fatal(err)
	}

	clus.Members[1].Stop(t)

	// leasing key ownership should have "cached" locally served
	cachedResp, err := lkv.Get(context.TODO(), "cached", clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	if len(cachedResp.Kvs) != 1 || string(cachedResp.Kvs[0].Value) != "abc" {
		t.Fatalf(`expected "cached"->"abc", got response %+v`, cachedResp)
	}

	// don't necessarily try to acquire leasing key ownership for new key
	resp, err := lkv.Get(context.TODO(), "uncached", clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf(`expected no keys, got response %+v`, resp)
	}
}
*/
// TestLeasingPrevKey checks the cache respects the PrevKV flag on puts.
func TestLeasingPrevKey(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 2})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}
	// fetch without prevkv to acquire leasing key
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	// fetch prevkv via put
	resp, err := lkv.Put(context.TODO(), "k", "def", clientv3.WithPrevKV())
	if err != nil {
		t.Fatal(err)
	}
	if resp.PrevKv == nil || string(resp.PrevKv.Value) != "abc" {
		t.Fatalf(`expected PrevKV.Value="abc", got response %+v`, resp)
	}
}

// TestLeasingRevGet checks the cache respects Get by Revision.
func TestLeasingRevGet(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	putResp, err := clus.Client(0).Put(context.TODO(), "k", "abc")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k", "def"); err != nil {
		t.Fatal(err)
	}

	// check historic revision
	getResp, gerr := lkv.Get(context.TODO(), "k", clientv3.WithRev(putResp.Header.Revision))
	if gerr != nil {
		t.Fatal(gerr)
	}
	if len(getResp.Kvs) != 1 || string(getResp.Kvs[0].Value) != "abc" {
		t.Fatalf(`expeted "k"->"abc" at rev=%d, got response %+v`, putResp.Header.Revision, getResp)
	}
	// check current revision
	getResp, gerr = lkv.Get(context.TODO(), "k")
	if gerr != nil {
		t.Fatal(gerr)
	}
	if len(getResp.Kvs) != 1 || string(getResp.Kvs[0].Value) != "def" {
		t.Fatalf(`expeted "k"->"abc" at rev=%d, got response %+v`, putResp.Header.Revision, getResp)
	}
}

// TestLeasingConcurrentPut ensures that a get after concurrent puts returns
// the recently put data.
func TestLeasingConcurrentPut(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	// force key into leasing key cache
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}

	// concurrently put through leasing client
	numPuts := 16
	putc := make(chan *clientv3.PutResponse, numPuts)
	for i := 0; i < numPuts; i++ {
		go func() {
			resp, perr := lkv.Put(context.TODO(), "k", "abc")
			if perr != nil {
				t.Fatal(perr)
			}
			putc <- resp
		}()
	}
	// record maximum revision from puts
	maxRev := int64(0)
	for i := 0; i < numPuts; i++ {
		if resp := <-putc; resp.Header.Revision > maxRev {
			maxRev = resp.Header.Revision
		}
	}

	// confirm Get gives most recently put revisions
	getResp, gerr := lkv.Get(context.TODO(), "k")
	if gerr != nil {
		t.Fatal(err)
	}
	if mr := getResp.Kvs[0].ModRevision; mr != maxRev {
		t.Errorf("expected ModRevision %d, got %d", maxRev, mr)
	}
	if ver := getResp.Kvs[0].Version; ver != int64(numPuts) {
		t.Errorf("expected Version %d, got %d", numPuts, ver)
	}
}

func TestLeasingDisconnectedGet(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "cached", "abc"); err != nil {
		t.Fatal(err)
	}
	// get key so it's cached
	if _, err := lkv.Get(context.TODO(), "cached"); err != nil {
		t.Fatal(err)
	}

	clus.Members[0].Stop(t)

	// leasing key ownership should have "cached" locally served
	cachedResp, err := lkv.Get(context.TODO(), "cached")
	if err != nil {
		t.Fatal(err)
	}
	if len(cachedResp.Kvs) != 1 || string(cachedResp.Kvs[0].Value) != "abc" {
		t.Fatalf(`expected "cached"->"abc", got response %+v`, cachedResp)
	}
}

func TestLeasingDeleteOwner(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}

	// get+own / delete / get
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Delete(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	resp, err := lkv.Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.Kvs) != 0 {
		t.Fatalf(`expected "k" to be deleted, got response %+v`, resp)
	}
	// try to double delete
	if _, err := lkv.Delete(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
}

func TestLeasingDeleteNonOwner(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv1, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	lkv2, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}
	// acquire ownership
	if _, err := lkv1.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	// delete via non-owner
	if _, err := lkv2.Delete(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}

	// key should be removed from lkv1
	resp, err := lkv1.Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf(`expected "k" to be deleted, got response %+v`, resp)
	}
}

func TestLeasingOverwriteResponse(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}

	resp, err := lkv.Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}

	resp.Kvs[0].Key[0] = 'z'
	resp.Kvs[0].Value[0] = 'z'

	resp, err = lkv.Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}

	if string(resp.Kvs[0].Key) != "k" {
		t.Errorf(`expected key "k", got %q`, string(resp.Kvs[0].Key))
	}
	if string(resp.Kvs[0].Value) != "abc" {
		t.Errorf(`expected value "abc", got %q`, string(resp.Kvs[0].Key))
	}
}

func TestLeasingOwnerPutResponse(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}
	gresp, gerr := lkv.Get(context.TODO(), "k")
	if gerr != nil {
		t.Fatal(gerr)
	}
	presp, err := lkv.Put(context.TODO(), "k", "def")
	if err != nil {
		t.Fatal(err)
	}
	if presp == nil {
		t.Fatal("expected put response, got nil")
	}

	clus.Members[0].Stop(t)

	gresp, gerr = lkv.Get(context.TODO(), "k")
	if gerr != nil {
		t.Fatal(gerr)
	}
	if gresp.Kvs[0].ModRevision != presp.Header.Revision {
		t.Errorf("expected mod revision %d, got %d", presp.Header.Revision, gresp.Kvs[0].ModRevision)
	}
	if gresp.Kvs[0].Version != 2 {
		t.Errorf("expected version 2, got version %d", gresp.Kvs[0].Version)
	}
}

func TestLeasingTxnOwnerGet2(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k2", "123"); err != nil {
		t.Fatal(err)
	}

	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Get(context.TODO(), "k2"); err != nil {
		t.Fatal(err)
	}

	// served through cache
	clus.Members[0].Stop(t)

	tresp, terr := lkv.Txn(context.TODO()).Then(clientv3.OpGet("k"), clientv3.OpGet("k2")).Commit()
	if terr != nil {
		t.Fatal(terr)
	}
	if len(tresp.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(tresp.Responses))
	}
	if rr := tresp.Responses[0].GetResponseRange(); string(rr.Kvs[0].Value) != "abc" {
		t.Errorf(`expected value "abc", got %q`, string(rr.Kvs[0].Value))
	}
	if rr := tresp.Responses[1].GetResponseRange(); string(rr.Kvs[0].Value) != "123" {
		t.Errorf(`expected value "123", got %q`, string(rr.Kvs[0].Value))
	}
}

func TestLeasingTxnOwnerIf(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}

	// served through cache
	clus.Members[0].Stop(t)

	tests := []struct {
		cmps       []clientv3.Cmp
		wSucceeded bool
		wResponses int
	}{
		// success
		{
			cmps:       []clientv3.Cmp{clientv3.Compare(clientv3.Value("k"), "=", "abc")},
			wSucceeded: true,
			wResponses: 1,
		},
		{
			cmps:       []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision("k"), "=", 2)},
			wSucceeded: true,
			wResponses: 1,
		},
		{
			cmps:       []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision("k"), "=", 2)},
			wSucceeded: true,
			wResponses: 1,
		},
		{
			cmps:       []clientv3.Cmp{clientv3.Compare(clientv3.Version("k"), "=", 1)},
			wSucceeded: true,
			wResponses: 1,
		},
		// failure
		{
			cmps: []clientv3.Cmp{clientv3.Compare(clientv3.Value("k"), ">", "abc")},
		},
		{
			cmps: []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision("k"), ">", 2)},
		},
		{
			cmps:       []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision("k"), "=", 2)},
			wSucceeded: true,
			wResponses: 1,
		},
		{
			cmps: []clientv3.Cmp{clientv3.Compare(clientv3.Version("k"), ">", 1)},
		},
		{
			cmps: []clientv3.Cmp{clientv3.Compare(clientv3.Value("k"), "<", "abc")},
		},
		{
			cmps: []clientv3.Cmp{clientv3.Compare(clientv3.CreateRevision("k"), "<", 2)},
		},
		{
			cmps: []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision("k"), "<", 2)},
		},
		{
			cmps: []clientv3.Cmp{clientv3.Compare(clientv3.Version("k"), "<", 1)},
		},
		{
			cmps: []clientv3.Cmp{
				clientv3.Compare(clientv3.Version("k"), "=", 1),
				clientv3.Compare(clientv3.Version("k"), "<", 1),
			},
		},
	}

	for i, tt := range tests {
		tresp, terr := lkv.Txn(context.TODO()).If(tt.cmps...).Then(clientv3.OpGet("k")).Commit()
		if terr != nil {
			t.Fatal(terr)
		}
		if tresp.Succeeded != tt.wSucceeded {
			t.Errorf("#%d: expected succeded %v, got %v", i, tt.wSucceeded, tresp.Succeeded)
		}
		if len(tresp.Responses) != tt.wResponses {
			t.Errorf("#%d: expected %d responses, got %d", i, tt.wResponses, len(tresp.Responses))
		}
	}
}

func TestLeasingTxnNonOwnerPut(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	lkv2, err := leasing.NewleasingKV(clus.Client(0), "pfx/")

	if _, err := clus.Client(0).Put(context.TODO(), "k", "abc"); err != nil {
		t.Fatal(err)
	}
	if _, err := clus.Client(0).Put(context.TODO(), "k2", "123"); err != nil {
		t.Fatal(err)
	}
	// cache in lkv
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Get(context.TODO(), "k2"); err != nil {
		t.Fatal(err)
	}
	// invalidate via lkv2 txn
	tresp, terr := lkv2.Txn(context.TODO()).Then(clientv3.OpPut("k", "def"), clientv3.OpPut("k2", "456")).Commit()
	if terr != nil {
		t.Fatal(terr)
	}
	if !tresp.Succeeded || len(tresp.Responses) != 2 {
		t.Fatalf("expected txn success, got %+v", tresp)
	}
	// check cache was invalidated
	gresp, gerr := lkv.Get(context.TODO(), "k")
	if gerr != nil {
		t.Fatal(err)
	}
	if len(gresp.Kvs) != 1 || string(gresp.Kvs[0].Value) != "def" {
		t.Errorf(`expected value "def", got %+v`, gresp)
	}
	gresp, gerr = lkv.Get(context.TODO(), "k2")
	if gerr != nil {
		t.Fatal(gerr)
	}
	if len(gresp.Kvs) != 1 || string(gresp.Kvs[0].Value) != "456" {
		t.Errorf(`expected value "def", got %+v`, gresp)
	}
}

func TestLeasingOwnerPutError(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}

	clus.Members[0].Stop(t)
	ctx, cancel := context.WithTimeout(context.TODO(), 100*time.Millisecond)
	defer cancel()
	if resp, err := lkv.Put(ctx, "k", "v"); err == nil {
		t.Fatalf("expected error, got response %+v", resp)
	}
}

func TestLeasingOwnerDeleteError(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}

	clus.Members[0].Stop(t)
	ctx, cancel := context.WithTimeout(context.TODO(), 100*time.Millisecond)
	defer cancel()
	if resp, err := lkv.Delete(ctx, "k"); err == nil {
		t.Fatalf("expected error, got response %+v", resp)
	}
}

func TestLeasingNonOwnerPutError(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	clus.Members[0].Stop(t)
	ctx, cancel := context.WithTimeout(context.TODO(), 100*time.Millisecond)
	defer cancel()
	if resp, err := lkv.Put(ctx, "k", "v"); err == nil {
		t.Fatalf("expected error, got response %+v", resp)
	}
}

func TestLeasingOwnerDeleteRange(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 8; i++ {
		if _, err := clus.Client(0).Put(context.TODO(), fmt.Sprintf("key/%d", i), "123"); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := lkv.Get(context.TODO(), "key/1"); err != nil {
		t.Fatal(err)
	}

	delResp, delErr := lkv.Delete(context.TODO(), "key/", clientv3.WithPrefix())
	if delErr != nil {
		t.Fatal(delErr)
	}

	// confirm keys are invalidated from cache and deleted on etcd
	for i := 0; i < 8; i++ {
		resp, err := lkv.Get(context.TODO(), fmt.Sprintf("key/%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Kvs) != 0 {
			t.Fatalf("expected no keys on key/%d, got %+v", i, resp)
		}
	}

	// confirm keys were deleted atomically

	w := clus.Client(0).Watch(
		clus.Client(0).Ctx(),
		"key/",
		clientv3.WithRev(delResp.Header.Revision),
		clientv3.WithPrefix())

	if wresp := <-w; len(wresp.Events) != 8 {
		t.Fatalf("expected %d delete events,got %d", 8, len(wresp.Events))
	}
}

func TestLeasingPutGetDeleteConcurrent(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkvs := make([]clientv3.KV, 16)
	for i := range lkvs {
		lkv, err := leasing.NewleasingKV(clus.Client(0), "pfx/")
		if err != nil {
			t.Fatal(err)
		}
		lkvs[i] = lkv
	}

	getdel := func(kv clientv3.KV) {
		if _, err := kv.Put(context.TODO(), "k", "abc"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
		if _, err := kv.Get(context.TODO(), "k"); err != nil {
			t.Fatal(err)
		}
		if _, err := kv.Delete(context.TODO(), "k"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}

	var wg sync.WaitGroup
	wg.Add(16)
	for i := 0; i < 16; i++ {
		go func() {
			defer wg.Done()
			for _, kv := range lkvs {
				getdel(kv)
			}
		}()
	}
	wg.Wait()

	resp, err := lkvs[0].Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.Kvs) > 0 {
		t.Fatalf("expected no kvs, got %+v", resp.Kvs)
	}

	resp, err = clus.Client(0).Get(context.TODO(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) > 0 {
		t.Fatalf("expected no kvs, got %+v", resp.Kvs)
	}
}
