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
	"reflect"
	"testing"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/leasing"
	"github.com/coreos/etcd/integration"
	"github.com/coreos/etcd/pkg/testutil"
)

func TestLeasingGet(t *testing.T) {
	defer testutil.AfterTest(t)

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3})
	defer clus.Terminate(t)

	c1 := clus.Client(0)
	c2 := clus.Client(1)
	c3 := clus.Client(2)
	lKV1, err := leasing.NewleasingKV(c1, "foo/")
	lKV2, err := leasing.NewleasingKV(c2, "foo/")
	lKV3, err := leasing.NewleasingKV(c3, "foo/")

	if err != nil {
		t.Fatal(err)
	}

	/*if _, err := lKV1.Put(context.TODO(), "abc", "bar"); err != nil {
		t.Fatal(err)
	}*/

	resp1, err := lKV1.Get(context.TODO(), "abc")
	if err != nil {
		t.Fatal(err)
	}

	//clus.Members[0].InjectPartition(t, clus.Members[1:])

	if _, err := lKV2.Put(context.TODO(), "abc", "def"); err != nil {
		t.Fatal(err)
	}

	resp1, err = lKV1.Get(context.TODO(), "abc")
	if err != nil {
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
