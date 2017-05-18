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
	"math/rand"
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

	resp3, err := lKV3.Get(context.TODO(), "abc")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := lKV2.Put(context.TODO(), "abc", "ghi"); err != nil {
		t.Fatal(err)
	}

	resp3, err = lKV3.Get(context.TODO(), "abc")
	if err != nil {
		t.Fatal(err)
	}

	if string(resp1.Kvs[0].Key) != "abc" {
		t.Errorf("expected key=%q, got key=%q", "abc", resp1.Kvs[0].Key)
	}

	if string(resp1.Kvs[0].Value) != "def" {
		t.Errorf("expected value=%q, got value=%q", "bar", resp1.Kvs[0].Value)
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

func TestLeasingGet2(t *testing.T) {
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

	keys := []string{"abc/a", "abc/b", "abc/a/a"}
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

	// load into cache
	if resp, err = lkv.Get(context.TODO(), "abc/a"); err != nil {
		t.Fatal(err)
	}

	// get when prefix is also a cached key
	if resp, err = lkv.Get(context.TODO(), "abc/a", clientv3.WithPrefix()); err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 2 {
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

	// don't necessarily try to acquire leasing key ownership for new key
	resp, err := lkv.Get(context.TODO(), "uncached", clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Kvs) != 0 {
		t.Fatalf(`expected no keys, got response %+v`, resp)
	}

	clus.Members[0].Stop(t)

	// leasing key ownership should have "cached" locally served
	cachedResp, err := lkv.Get(context.TODO(), "cached", clientv3.WithSerializable())
	if err != nil {
		t.Fatal(err)
	}
	if len(cachedResp.Kvs) != 1 || string(cachedResp.Kvs[0].Value) != "abc" {
		t.Fatalf(`expected "cached"->"abc", got response %+v`, cachedResp)
	}
}

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

// TestLeasingGetKeysOnly checks only keys are returnd with keys only
func TestLeasingGetKeysOnly(t *testing.T) {
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
	if _, err := lkv.Get(context.TODO(), "k", clientv3.WithKeysOnly()); err != nil {
		t.Fatal(err)
	}

	clus.Members[0].Stop(t)

	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
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
	tresp, terr := lkv2.Txn(context.TODO()).Then(
		clientv3.OpPut("k", "def"),
		clientv3.OpPut("k2", "456"),
		clientv3.OpPut("k3", "999"), // + a key not in any cache
	).Commit()
	if terr != nil {
		t.Fatal(terr)
	}
	if !tresp.Succeeded || len(tresp.Responses) != 3 {
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
	// check puts were applied and are all in the same revision
	w := clus.Client(0).Watch(
		clus.Client(0).Ctx(),
		"k",
		clientv3.WithRev(tresp.Header.Revision),
		clientv3.WithPrefix())
	wresp := <-w
	c := 0
	evs := []clientv3.Event{}
	for _, ev := range wresp.Events {
		evs = append(evs, *ev)
		if ev.Kv.ModRevision == tresp.Header.Revision {
			c++
		}
	}
	if c != 3 {
		t.Fatalf("expected 3 put events, got %+v", evs)
	}
}

// TestLeasingTxnRandIfThen randomly leases keys two separate clients, then
// issues a random If/{Then,Else} transaction on those keys to one client.
func TestLeasingTxnRandIfThenOrElse(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv1, err1 := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err1 != nil {
		t.Fatal(err1)
	}
	lkv2, err2 := leasing.NewleasingKV(clus.Client(0), "pfx/")
	if err2 != nil {
		t.Fatal(err2)
	}

	keyCount := 16
	dat := make([]*clientv3.PutResponse, keyCount)
	for i := 0; i < keyCount; i++ {
		k, v := fmt.Sprintf("k-%d", i), fmt.Sprintf("%d", i)
		dat[i], err1 = clus.Client(0).Put(context.TODO(), k, v)
		if err1 != nil {
			t.Fatal(err1)
		}
	}

	// nondeterministically populate leasing caches
	var wg sync.WaitGroup
	getc := make(chan struct{}, keyCount)
	getRandom := func(kv clientv3.KV) {
		defer wg.Done()
		for i := 0; i < keyCount/2; i++ {
			k := fmt.Sprintf("k-%d", rand.Intn(keyCount))
			if _, err := kv.Get(context.TODO(), k); err != nil {
				t.Fatal(err)
			}
			getc <- struct{}{}
		}
	}
	wg.Add(2)
	defer wg.Wait()
	go getRandom(lkv1)
	go getRandom(lkv2)

	// random list of comparisons, all true
	cmps := randCmps("k-", dat)
	// random list of puts/gets; unique keys
	ops := []clientv3.Op{}
	usedIdx := make(map[int]struct{})
	for i := 0; i < keyCount; i++ {
		idx := rand.Intn(keyCount)
		if _, ok := usedIdx[idx]; ok {
			continue
		}
		usedIdx[idx] = struct{}{}
		k := fmt.Sprintf("k-%d", idx)
		switch rand.Intn(2) {
		case 0:
			ops = append(ops, clientv3.OpGet(k))
		case 1:
			ops = append(ops, clientv3.OpPut(k, "a"))
			// TODO: add delete
		}
	}
	// random lengths
	cmps = cmps[:rand.Intn(len(cmps))]
	ops = ops[:rand.Intn(len(ops))]

	// wait for some gets to populate the leasing caches before committing
	for i := 0; i < keyCount/2; i++ {
		<-getc
	}

	// randomly choose between then and else blocks
	var thenOps, elseOps []clientv3.Op
	useThen := rand.Intn(2) == 0
	if useThen {
		thenOps = ops
	} else {
		// force failure
		cmp := clientv3.Compare(clientv3.Version(fmt.Sprintf("k-%d", rand.Intn(keyCount))), "=", 0)
		cmps = append(cmps, cmp)
		elseOps = ops
	}

	tresp, terr := lkv1.Txn(context.TODO()).If(cmps...).Then(thenOps...).Else(elseOps...).Commit()
	if terr != nil {
		t.Fatal(terr)
	}
	// cmps always succeed
	if tresp.Succeeded != useThen {
		t.Fatalf("expected succeeded=%v, got tresp=%+v", useThen, tresp)
	}
	// get should match what was put
	checkPuts := func(s string, kv clientv3.KV) {
		for _, op := range ops {
			if !op.IsPut() {
				continue
			}
			resp, rerr := kv.Get(context.TODO(), string(op.KeyBytes()))
			if rerr != nil {
				t.Fatal(rerr)
			}
			if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "a" {
				t.Fatalf(`%s: expected value="a", got %+v`, s, resp.Kvs)
			}
		}
	}
	checkPuts("client(0)", clus.Client(0))
	checkPuts("lkv1", lkv1)
	checkPuts("lkv2", lkv2)
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

// TestLeasingReconnectRevoke checks that revocation works if
// disconnected when trying to submit revoke txn.
func TestLeasingReconnectOwnerRevoke(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	lkv1, err1 := leasing.NewleasingKV(clus.Client(0), "foo/")
	if err1 != nil {
		t.Fatal(err1)
	}
	lkv2, err2 := leasing.NewleasingKV(clus.Client(1), "foo/")
	if err2 != nil {
		t.Fatal(err2)
	}

	if _, err := lkv1.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}

	cctx, cancel := context.WithCancel(context.TODO())
	sdonec, pdonec := make(chan struct{}), make(chan struct{})
	// make lkv1 connection choppy so txns fail
	go func() {
		defer close(sdonec)
		for cctx.Err() == nil {
			clus.Members[0].Stop(t)
			time.Sleep(100 * time.Millisecond)
			clus.Members[0].Restart(t)
		}
	}()
	go func() {
		defer close(pdonec)
		if _, err := lkv2.Put(cctx, "k", "v"); err != nil {
			t.Fatal(err)
		}
	}()
	select {
	case <-pdonec:
		cancel()
		<-sdonec
	case <-time.After(3 * time.Second):
		cancel()
		<-sdonec
		<-pdonec
		t.Fatal("took to long to revoke and put")
	}
}

// TestLeasingReconnectOwnerPut checks a put error on an owner will
// not cause inconsistency between the server and the client.
func TestLeasingReconnectOwnerPut(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "foo/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lkv.Get(context.TODO(), "k"); err != nil {
		t.Fatal(err)
	}

	for i := 0; true; i++ {
		v := fmt.Sprintf("%d", i)
		donec := make(chan struct{})
		clus.Members[0].DropConnections()
		go func() {
			defer close(donec)
			for i := 0; i < 10; i++ {
				clus.Members[0].DropConnections()
				time.Sleep(time.Millisecond)
			}
		}()
		_, err = lkv.Put(context.TODO(), "k", v)
		<-donec
		if err != nil {
			break
		}
	}

	lresp, lerr := lkv.Get(context.TODO(), "k")
	if lerr != nil {
		t.Fatal(lerr)
	}
	cresp, cerr := clus.Client(0).Get(context.TODO(), "k")
	if cerr != nil {
		t.Fatal(cerr)
	}
	if !reflect.DeepEqual(lresp.Kvs, cresp.Kvs) {
		t.Fatalf("expected %+v, got %+v", cresp, lresp)
	}
}

// TestLeasingReconnectNonOwnerGet checks a get error on an owner will
// not cause inconsistency between the server and the client.
func TestLeasingReconnectNonOwnerGet(t *testing.T) {
	defer testutil.AfterTest(t)
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	lkv, err := leasing.NewleasingKV(clus.Client(0), "foo/")
	if err != nil {
		t.Fatal(err)
	}

	// populate a few keys so some leasing gets have keys
	for i := 0; i < 4; i++ {
		k := fmt.Sprintf("k-%d", i*2)
		if _, err = lkv.Put(context.TODO(), k, k[2:]); err != nil {
			t.Fatal(err)
		}
	}

	n := 0
	for i := 0; true; i++ {
		donec := make(chan struct{})
		clus.Members[0].DropConnections()
		go func() {
			defer close(donec)
			for i := 0; i < 10; i++ {
				clus.Members[0].DropConnections()
				time.Sleep(time.Millisecond)
			}
		}()
		_, err = lkv.Get(context.TODO(), fmt.Sprintf("k-%d", i))
		<-donec
		n++
		if err != nil {
			break
		}
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k-%d", i)
		lresp, lerr := lkv.Get(context.TODO(), k)
		if lerr != nil {
			t.Fatal(lerr)
		}
		cresp, cerr := clus.Client(0).Get(context.TODO(), k)
		if cerr != nil {
			t.Fatal(cerr)
		}
		if !reflect.DeepEqual(lresp.Kvs, cresp.Kvs) {
			t.Fatalf("expected %+v, got %+v", cresp, lresp)
		}
	}
}

func randCmps(pfx string, dat []*clientv3.PutResponse) (cmps []clientv3.Cmp) {
	for i := 0; i < len(dat); i++ {
		idx := rand.Intn(len(dat))
		k := fmt.Sprintf("%s%d", pfx, idx)
		rev := dat[idx].Header.Revision
		var cmp clientv3.Cmp
		switch rand.Intn(4) {
		case 0:
			cmp = clientv3.Compare(clientv3.CreateRevision(k), ">", rev-1)
		case 1:
			cmp = clientv3.Compare(clientv3.Version(k), "=", 1)
		case 2:
			cmp = clientv3.Compare(clientv3.CreateRevision(k), "=", rev)
		case 3:
			cmp = clientv3.Compare(clientv3.CreateRevision(k), "!=", rev+1)

		}
		cmps = append(cmps, cmp)
	}
	return cmps
}
