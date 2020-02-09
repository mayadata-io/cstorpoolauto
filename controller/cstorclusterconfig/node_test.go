/*
Copyright 2019 The MayaData Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cstorclusterconfig

import (
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	autotypes "mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
)

func TestByCreationTimeLen(t *testing.T) {
	var tests = map[string]struct {
		nodes       []*unstructured.Unstructured
		expectCount int
	}{
		"empty nodes": {
			nodes:       []*unstructured.Unstructured{},
			expectCount: 0,
		},
		"nil nodes": {
			nodes:       nil,
			expectCount: 0,
		},
		"single node": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
			},
			expectCount: 1,
		},
		"many nodes": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
				&unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			expectCount: 2,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := ByCreationTime(mock.nodes).Len()
			if got != mock.expectCount {
				t.Fatalf("Expected count %d got %d", mock.expectCount, got)
			}
		})
	}
}

func TestByCreationTimeLess(t *testing.T) {
	var tests = map[string]struct {
		nodes  []*unstructured.Unstructured
		isLess bool
	}{
		"empty nodes": {
			nodes:  []*unstructured.Unstructured{},
			isLess: false,
		},
		"nil nodes": {
			nodes:  nil,
			isLess: false,
		},
		"two nodes with i-th timestamp == j-th timestamp": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: true,
		},
		"two nodes with i-th timestamp < j-th timestamp by year": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2007-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: true,
		},
		"two nodes with i-th timestamp < j-th timestamp by month": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-02-02T15:04:05Z",
						},
					},
				},
			},
			isLess: true,
		},
		"two nodes with i-th timestamp < j-th timestamp by day": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-03T15:04:05Z",
						},
					},
				},
			},
			isLess: true,
		},
		"two nodes with i-th timestamp < j-th timestamp by hour": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T16:04:05Z",
						},
					},
				},
			},
			isLess: true,
		},
		"two nodes with i-th timestamp < j-th timestamp by min": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:05:05Z",
						},
					},
				},
			},
			isLess: true,
		},
		"two nodes with i-th timestamp < j-th timestamp by sec": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:06Z",
						},
					},
				},
			},
			isLess: true,
		},
		"two nodes with i-th timestamp > j-th timestamp by year": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2007-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: false,
		},
		"two nodes with i-th timestamp > j-th timestamp by month": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-02-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: false,
		},
		"two nodes with i-th timestamp > j-th timestamp by day": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-03T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: false,
		},
		"two nodes with i-th timestamp > j-th timestamp by hour": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T16:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: false,
		},
		"two nodes with i-th timestamp > j-th timestamp by min": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:05:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: false,
		},
		"two nodes with i-th timestamp > j-th timestamp by second": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:06Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			isLess: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := ByCreationTime(mock.nodes).Less(0, 1)
			if got != mock.isLess {
				t.Fatalf("Expected %t got %t", mock.isLess, got)
			}
		})
	}
}

func TestSortByCreationTime(t *testing.T) {
	var tests = map[string]struct {
		nodes       []*unstructured.Unstructured
		expect      []*unstructured.Unstructured
		removeCount int
	}{
		"0 nodes": {
			nodes:  []*unstructured.Unstructured{},
			expect: []*unstructured.Unstructured{},
		},
		"1 nodes": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
		},
		"3 nodes": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2020",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2020",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
			},
		},
		"4 nodes": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2020",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2012",
							"creationTimestamp": "2012-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2012",
							"creationTimestamp": "2012-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2020",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
			},
		},
		"3 nodes & remove = 1": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2020",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
			},
			removeCount: 1,
		},
		"3 nodes & remove = 2": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2020",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			removeCount: 2,
		},
		"3 nodes & remove = 3": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2020",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2006",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "2010",
							"creationTimestamp": "2010-01-02T15:04:05Z",
						},
					},
				},
			},
			expect:      []*unstructured.Unstructured{},
			removeCount: 3,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			var got []*unstructured.Unstructured
			sort.Sort(ByCreationTime(mock.nodes))
			actualCount := len(mock.nodes)
			got = append(got, mock.nodes[:actualCount-mock.removeCount]...)

			for idx, expectNode := range mock.expect {
				gotNode := got[idx]
				if expectNode.GetName() != gotNode.GetName() {
					t.Fatalf(
						"Expected node name %s got %s at %d",
						expectNode.GetName(), gotNode.GetName(), idx,
					)
				}
				if expectNode.GetCreationTimestamp() != gotNode.GetCreationTimestamp() {
					t.Fatalf(
						"Expected node creation %s got %s at %d",
						expectNode.GetCreationTimestamp(), gotNode.GetCreationTimestamp(), idx,
					)
				}
			}
		})
	}
}

func TestByCreationTimeSwap(t *testing.T) {

}

func TestNodeListTryPickUptoCount(t *testing.T) {
	var tests = map[string]struct {
		nodes  []*unstructured.Unstructured
		pick   int64
		expect int
	}{
		"nodes is empty": {
			nodes:  []*unstructured.Unstructured{},
			pick:   2,
			expect: 0,
		},
		"nodes is nil": {
			nodes:  nil,
			pick:   3,
			expect: 0,
		},
		"node count == pick count": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
			},
			pick:   1,
			expect: 1,
		},
		"node count > pick count": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
				&unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			pick:   1,
			expect: 1,
		},
		"node count > pick count && pick count == 0": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
				&unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			pick:   0,
			expect: 0,
		},
		"node count < pick count": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
				&unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			pick:   4,
			expect: 2,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			l := NodeList(mock.nodes)
			got := l.TryPickUptoCount(mock.pick)
			if len(got) != mock.expect {
				t.Fatalf("Expected count %d got %d", mock.expect, len(got))
			}
		})
	}
}

func TestNodeListFindByNameAndUID(t *testing.T) {
	var tests = map[string]struct {
		nodes       []*unstructured.Unstructured
		name        string
		uid         types.UID
		isExpectNil bool
	}{
		"empty nodes": {
			nodes:       []*unstructured.Unstructured{},
			isExpectNil: true,
		},
		"nil nodes": {
			nodes:       nil,
			isExpectNil: true,
		},
		"node matches name & uid": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "open",
							"uid":  "101",
						},
					},
				},
			},
			name: "open",
			uid:  types.UID("101"),
		},
		"multiple nodes with one matching name & uid": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "open",
							"uid":  "101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "opensource",
							"uid":  "102",
						},
					},
				},
			},
			name: "open",
			uid:  types.UID("101"),
		},
		"multiple nodes with no matching name & uid": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "open",
							"uid":  "101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "opensource",
							"uid":  "102",
						},
					},
				},
			},
			name:        "openday",
			uid:         types.UID("101"),
			isExpectNil: true,
		},
		"node matches name & does not match uid": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "open",
							"uid":  "101",
						},
					},
				},
			},
			name:        "open",
			uid:         types.UID("102"),
			isExpectNil: true,
		},
		"node does not match name & matches uid": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "open",
							"uid":  "101",
						},
					},
				},
			},
			name:        "opensource",
			uid:         types.UID("101"),
			isExpectNil: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			l := NodeList(mock.nodes)
			got := l.FindByNameAndUID(mock.name, mock.uid)
			if mock.isExpectNil && got != nil {
				t.Fatalf("Expected nil got %+v", got)
			}
			if !mock.isExpectNil && got.GetName() != mock.name && got.GetUID() != mock.uid {
				t.Fatalf("Expected name uid %s / %s got %s / %s", mock.name, mock.uid, got.GetName(), got.GetUID())
			}
		})
	}
}

func TestNodeListRemoveRecentByCountFromPlannedNodes(t *testing.T) {
	var tests = map[string]struct {
		nodes           []*unstructured.Unstructured
		planNodes       []autotypes.CStorClusterPlanNode
		removeCount     int64
		expectPlanNodes []autotypes.CStorClusterPlanNode
		isErr           bool
	}{
		"node not found": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "junk-101",
					UID:  "junk-101",
				},
			},
			isErr: true,
		},
		"node not found - empty list": {
			nodes: []*unstructured.Unstructured{},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "junk-101",
					UID:  "junk-101",
				},
			},
			isErr: true,
		},
		"list node 1 = plan node 1 & remove 1": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			removeCount:     1,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{},
			isErr:           false,
		},
		"list node 1 = plan node 1 & remove 0": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			removeCount: 0,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			isErr: false,
		},
		"list node 2 = plan node 1 & remove 1": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-201",
							"uid":               "node-201",
							"creationTimestamp": "2006-02-02T15:04:05Z",
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			removeCount:     1,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{},
			isErr:           false,
		},
		"list node 2 = plan node 2 & remove 1": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-2006",
							"uid":               "node-2006",
							"creationTimestamp": "2006-01-02T15:04:05Z", // old
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-2010",
							"uid":               "node-2010",
							"creationTimestamp": "2010-01-02T15:04:05Z", // new
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-2006",
					UID:  "node-2006",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-2010",
					UID:  "node-2010",
				},
			},
			removeCount: 1,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-2006",
					UID:  "node-2006",
				},
			},
			isErr: false,
		},
		"list node 2 = plan node 2 & remove 2": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z", // old
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-201",
							"uid":               "node-201",
							"creationTimestamp": "2020-01-02T15:04:05Z", // new
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			removeCount:     2,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{},
			isErr:           false,
		},
		"same age - list node 2 = plan node 2 & remove 1": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-201",
							"uid":               "node-201",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			removeCount: 1,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			isErr: false,
		},
		"same age - list node 3 = plan node 3 & remove 2": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-201",
							"uid":               "node-201",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-301",
							"uid":               "node-301",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			removeCount: 2,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			isErr: false,
		},
		"same age - list node 3 = plan node 3 & remove 1": {
			nodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-201",
							"uid":               "node-201",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":              "node-301",
							"uid":               "node-301",
							"creationTimestamp": "2006-01-02T15:04:05Z",
						},
					},
				},
			},
			planNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			removeCount: 1,
			expectPlanNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			l := NodeList(mock.nodes)
			got, err :=
				l.RemoveRecentByCountFromPlannedNodes(mock.removeCount, mock.planNodes)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if len(got) != len(mock.expectPlanNodes) {
				t.Fatalf(
					"Expected node count %d got %d", len(mock.expectPlanNodes), len(got),
				)
			}
			for idx, expectNode := range mock.expectPlanNodes {
				gotNodeName := got[idx].Name
				gotNodeUID := got[idx].UID
				if expectNode.Name != gotNodeName {
					t.Fatalf(
						"Expected name %s got %s at index %d", expectNode.Name, gotNodeName, idx)
				}
				if expectNode.UID != gotNodeUID {
					t.Fatalf(
						"Expected uid %s got %s at index %d", expectNode.UID, gotNodeUID, idx)
				}
			}
		})
	}
}

func TestNodePickByCountAndNotInPlannedNodes(t *testing.T) {
	var tests = map[string]struct {
		allNodes     []*unstructured.Unstructured
		excludeNodes []autotypes.CStorClusterPlanNode
		includeCount int64
		expectNodes  []autotypes.CStorClusterPlanNode
		isErr        bool
	}{
		"all=0 && exclude=0 && include=0": {
			allNodes:     []*unstructured.Unstructured{},
			excludeNodes: []autotypes.CStorClusterPlanNode{},
			includeCount: 0,
			isErr:        false,
		},
		"all=0 && exclude=0 && include=1": {
			allNodes:     []*unstructured.Unstructured{},
			excludeNodes: []autotypes.CStorClusterPlanNode{},
			includeCount: 1,
			isErr:        true,
		},
		"all=1 && exclude=1 && include=1": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			excludeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			includeCount: 1,
			isErr:        true,
		},
		"all=1 && exclude=0 && include=1": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			excludeNodes: []autotypes.CStorClusterPlanNode{},
			includeCount: 1,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			isErr: false,
		},
		"all=2 && exclude=1 && include=1": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			excludeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			includeCount: 1,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			isErr: false,
		},
		"all=2 && exclude=0 && include=2": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			excludeNodes: []autotypes.CStorClusterPlanNode{},
			includeCount: 2,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			isErr: false,
		},
		"all=2 && exclude=2 && include=0": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			excludeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			includeCount: 0,
			expectNodes:  []autotypes.CStorClusterPlanNode{},
			isErr:        false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			l := NodeList(mock.allNodes)
			got, err :=
				l.PickByCountAndNotInPlannedNodes(mock.includeCount, mock.excludeNodes)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if len(got) != len(mock.expectNodes) {
				t.Fatalf(
					"Expected node count %d got %d", len(mock.expectNodes), len(got),
				)
			}
			expectNodeList := autotypes.CStorClusterPlanNodeList(mock.expectNodes)
			for _, gotNode := range got {
				if !expectNodeList.Contains(gotNode.Name, gotNode.UID) {
					t.Fatalf("Node not found in expect list: [%+v]", gotNode)
				}
			}
		})
	}
}

func TestNodePickByCountAndIncludeAllPlannedNodes(t *testing.T) {
	var tests = map[string]struct {
		allNodes     []*unstructured.Unstructured
		includeNodes []autotypes.CStorClusterPlanNode
		includeCount int64
		expectNodes  []autotypes.CStorClusterPlanNode
		isErr        bool
	}{
		"nodes=1 && include=1 && count=1": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			includeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			includeCount: 1,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			isErr: false,
		},
		"nodes=0 && include=1 && count=1": {
			allNodes: []*unstructured.Unstructured{},
			includeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			includeCount: 1,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			isErr: false,
		},
		"nodes=1 && include=1 && count=2": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			includeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			includeCount: 2,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			isErr: false,
		},
		"nodes=1 && include=2 && count=2": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			includeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			includeCount: 2,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			isErr: false,
		},
		"nodes=1 && include=2 && count=3": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			includeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			includeCount: 3,
			expectNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			isErr: false,
		},
		"nodes=1 && include=2 && count=1": {
			allNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			includeNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			includeCount: 1,
			isErr:        true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			l := NodeList(mock.allNodes)
			got, err :=
				l.PickByCountAndIncludeAllPlannedNodes(mock.includeCount, mock.includeNodes)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if len(got) != len(mock.expectNodes) {
				t.Fatalf("Expected node count %d got %d", len(mock.expectNodes), len(got))
			}
			expectNodeList := autotypes.CStorClusterPlanNodeList(mock.expectNodes)
			for _, gotNode := range got {
				if !expectNodeList.Contains(gotNode.Name, gotNode.UID) {
					t.Fatalf("Node not found in expect list: [%+v]", gotNode)
				}
			}
		})
	}
}

func TestNodePlannerGetAllNodes(t *testing.T) {
	var tests = map[string]struct {
		resources []*unstructured.Unstructured
		want      []*unstructured.Unstructured
	}{
		"0 node of 0 resources": {
			resources: []*unstructured.Unstructured{},
			want:      []*unstructured.Unstructured{},
		},
		"0 node of 1 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "pod-101",
							"uid":  "pod-101",
						},
					},
				},
			},
			want: []*unstructured.Unstructured{},
		},
		"1 node of 1 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			want: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
		},
		"1 node of 2 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "pod-101",
							"uid":  "pod-101",
						},
					},
				},
			},
			want: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
		},
		"2 nodes of 3 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "pod-101",
							"uid":  "pod-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			want: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &NodePlanner{
				Resources: mock.resources,
			}
			got := p.GetAllNodes()
			if len(got) != len(mock.want) {
				t.Fatalf("Expected count %d got %d", len(mock.want), len(got))
			}
			if !unstruct.FromList(got).ContainsAll(mock.want) {
				t.Fatalf(
					"Expected no diff got \n%s", cmp.Diff(got, mock.want),
				)
			}
		})
	}
}

func TestNodePlannerGetAllNodeCount(t *testing.T) {
	var tests = map[string]struct {
		resources []*unstructured.Unstructured
		expect    int64
	}{
		"0 node of 0 resources": {
			resources: []*unstructured.Unstructured{},
			expect:    0,
		},
		"0 node of 1 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "pod-101",
							"uid":  "pod-101",
						},
					},
				},
			},
			expect: 0,
		},
		"1 node of 1 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
			},
			expect: 1,
		},
		"1 node of 2 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "pod-101",
							"uid":  "pod-101",
						},
					},
				},
			},
			expect: 1,
		},
		"2 nodes of 3 resources": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "pod-101",
							"uid":  "pod-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			expect: 2,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &NodePlanner{
				Resources: mock.resources,
			}
			got := p.GetAllNodeCount()
			if got != mock.expect {
				t.Fatalf("Expected count %d got %d", mock.expect, got)
			}
		})
	}
}

func TestNodePlannerGetAllowedNodes(t *testing.T) {
	var tests = map[string]struct {
		resources    []*unstructured.Unstructured
		nodeSelector metac.ResourceSelector
		expect       []*unstructured.Unstructured
		isErr        bool
	}{
		"0 node selector && 1 node": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
		},
		"0 node selector && 4 nodes": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-301",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-401",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-301",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-401",
						},
					},
				},
			},
		},
		"matching node name selector && 1 node": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
			nodeSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"metadata.name": "node-101",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
		},
		"matching node name selector && 2 nodes": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
			nodeSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"metadata.name": "node-101",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
		},
		"matching node kind selector && 2 nodes": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
			nodeSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"kind": "Node",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
		},
		"matching node kind && apiversion selector && 2 nodes": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1beta1",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
			nodeSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"kind":       "Node",
							"apiVersion": "v1",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
		},
		"matching node kind || apiversion selector && 2 nodes": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1beta1",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
			nodeSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"kind": "Node",
						},
					},
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"apiVersion": "v1",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1beta1",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
		},
		"matching node apiVersion || name selector && 2 nodes": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1beta1",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
			nodeSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"metadata.name": "node-201",
						},
					},
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"apiVersion": "v1",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1beta1",
						"metadata": map[string]interface{}{
							"name": "node-201",
						},
					},
				},
			},
		},
		"matching node name selector && 1 nil node of 2 nodes": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
				nil,
			},
			nodeSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"metadata.name": "node-201",
						},
					},
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"apiVersion": "v1",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind":       "Node",
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &NodePlanner{
				Resources:    mock.resources,
				NodeSelector: mock.nodeSelector,
			}
			_, err := p.GetAllowedNodes()
			got := p.allowedNodes
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if !unstruct.FromList(got).ContainsAll(mock.expect) {
				t.Fatalf("Expected no diff got \n%s", cmp.Diff(got, mock.expect))
			}
		})
	}
}

func TestNodePlannerGetAllowedNodesOrCached(t *testing.T) {
	var tests = map[string]struct {
		cached    []*unstructured.Unstructured
		resources []*unstructured.Unstructured
		expect    []*unstructured.Unstructured
		isErr     bool
	}{
		"cached": {
			cached: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
			resources: []*unstructured.Unstructured{
				nil,
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
		},
		"no cache": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
			expect: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &NodePlanner{
				Resources:    mock.resources,
				allowedNodes: mock.cached,
			}
			got, err := p.GetAllowedNodesOrCached()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if !unstruct.FromList(got).ContainsAll(mock.expect) {
				t.Fatalf("Expected no diff got:\n%s", cmp.Diff(got, mock.expect))
			}
		})
	}
}

func TestNodePlannerGetAllowedNodeCountOrCached(t *testing.T) {
	var tests = map[string]struct {
		cached    []*unstructured.Unstructured
		resources []*unstructured.Unstructured
		expect    int64
		isErr     bool
	}{
		"cached": {
			cached: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
			expect: 1,
		},
		"no cache": {
			resources: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
						},
					},
				},
			},
			expect: 1,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &NodePlanner{
				Resources:    mock.resources,
				allowedNodes: mock.cached,
			}
			got, err := p.GetAllowedNodeCountOrCached()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if got != mock.expect {
				t.Fatalf("Expected count %d got %d", mock.expect, got)
			}
		})
	}
}

func TestNodePlannerPlan(t *testing.T) {
	var tests = map[string]struct {
		allowedNodes  []*unstructured.Unstructured
		observedNodes []autotypes.CStorClusterPlanNode
		minPoolCount  resource.Quantity
		maxPoolCount  resource.Quantity
		expect        []autotypes.CStorClusterPlanNode
		isErr         bool
	}{
		//
		// no change
		//
		"allowed nodes=3 && observed=2 && min=2 && max=4": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-301",
							"uid":  "node-301",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			minPoolCount: resource.MustParse("2"),
			maxPoolCount: resource.MustParse("4"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
		},
		//
		// add one keep one
		//
		"allowed nodes=2 && observed=1 && min=2 && max=2": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			minPoolCount: resource.MustParse("2"),
			maxPoolCount: resource.MustParse("2"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
		},
		//
		// min nodes not available
		//
		"allowed nodes=2 && observed=1 && min=3 && max=3": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			minPoolCount: resource.MustParse("3"),
			maxPoolCount: resource.MustParse("3"),
			isErr:        true,
		},
		//
		// remove one keep one
		//
		"allowed nodes=1 && observed=2 && min=1 && max=3": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			minPoolCount: resource.MustParse("1"),
			maxPoolCount: resource.MustParse("3"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			isErr: false,
		},
		//
		// replace all nodes
		//
		"allowed nodes=2 && observed=1 diff && min=2 && max=3": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-301",
							"uid":  "node-301",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			minPoolCount: resource.MustParse("2"),
			maxPoolCount: resource.MustParse("3"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			isErr: false,
		},
		//
		// add nodes
		//
		"allowed nodes=5 && observed=0 diff && min=3 && max=5": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-301",
							"uid":  "node-301",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-401",
							"uid":  "node-401",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-501",
							"uid":  "node-501",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{},
			minPoolCount:  resource.MustParse("3"),
			maxPoolCount:  resource.MustParse("5"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			isErr: false,
		},
		//
		// pool instances within min & max
		//
		"allowed nodes=3 && observed=3 && min=2 && max=5": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-101",
							"uid":  "node-101",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-201",
							"uid":  "node-201",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name": "node-301",
							"uid":  "node-301",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			minPoolCount: resource.MustParse("2"),
			maxPoolCount: resource.MustParse("5"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			isErr: false,
		},
		//
		// reduce the pool instances
		//
		"allowed nodes=3 && observed=3 && creation time && min=2 && max=2": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name":              "node-201",
							"uid":               "node-201",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name":              "node-301",
							"uid":               "node-301",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			minPoolCount: resource.MustParse("2"),
			maxPoolCount: resource.MustParse("2"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
			},
			isErr: false,
		},
		//
		// remove newer pool instances
		//
		"allowed nodes=3 && observed=3 && creation time diff && min=2 && max=2": {
			allowedNodes: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name":              "node-101",
							"uid":               "node-101",
							"creationTimestamp": "2020-01-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name":              "node-201",
							"uid":               "node-201",
							"creationTimestamp": "2020-02-02T15:04:05Z",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Node",
						"metadata": map[string]interface{}{
							"name":              "node-301",
							"uid":               "node-301",
							"creationTimestamp": "2020-03-02T15:04:05Z",
						},
					},
				},
			},
			observedNodes: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-301",
					UID:  "node-301",
				},
			},
			minPoolCount: resource.MustParse("2"),
			maxPoolCount: resource.MustParse("2"),
			expect: []autotypes.CStorClusterPlanNode{
				autotypes.CStorClusterPlanNode{
					Name: "node-201",
					UID:  "node-201",
				},
				autotypes.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &NodePlanner{
				allowedNodes: mock.allowedNodes,
			}
			got, err := p.Plan(NodePlannerConfig{
				ObservedNodes: mock.observedNodes,
				MinPoolCount:  mock.minPoolCount,
				MaxPoolCount:  mock.maxPoolCount,
			})
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !autotypes.CStorClusterPlanNodeList(got).ContainsAll(mock.expect) {
				t.Fatalf("Expected no diff got:\n%s", cmp.Diff(got, mock.expect))
			}
		})
	}
}
