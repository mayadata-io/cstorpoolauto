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

package types

import (
	"testing"

	"k8s.io/apimachinery/pkg/types"
)

func TestMakeNodeSlice(t *testing.T) {
	var tests = map[string]struct {
		nodes       []CStorClusterPlanNode
		expectCount int
	}{
		"Nodes = nil": {
			expectCount: 0,
		},
		"Node count = 1 && valid nodes": {
			nodes: []CStorClusterPlanNode{
				CStorClusterPlanNode{
					Name: "node1",
					UID:  types.UID("123"),
				},
			},
			expectCount: 1,
		},
		"Node count = 1 && in-valid nodes": {
			nodes: []CStorClusterPlanNode{
				CStorClusterPlanNode{
					Name: "",
					UID:  types.UID(""),
				},
			},
			expectCount: 1,
		},
		"Node count = 2 && valid nodes": {
			nodes: []CStorClusterPlanNode{
				CStorClusterPlanNode{
					Name: "node1",
					UID:  types.UID("123"),
				},
				CStorClusterPlanNode{
					Name: "node2",
					UID:  types.UID("123-123"),
				},
			},
			expectCount: 2,
		},
	}
	for name, mock := range tests {
		t.Run(name, func(t *testing.T) {
			got := MakeListMapOfPlanNodes(mock.nodes)
			if len(got) != mock.expectCount {
				t.Fatalf(
					"Expected count %d got %d", mock.expectCount, len(got),
				)
			}
		})
	}
}
