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
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
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

}
