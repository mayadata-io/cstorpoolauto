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

package cstorpoolcluster

import (
	"cstorpoolauto/types"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestPlannerIsReadyByNodeCount(t *testing.T) {
	var tests = map[string]struct {
		planner *Planner
		isReady bool
	}{
		"node count == observed storageset count": {
			planner: &Planner{
				CStorClusterPlan: &types.CStorClusterPlan{
					Spec: types.CStorClusterPlanSpec{
						Nodes: []types.CStorClusterPlanNode{
							types.CStorClusterPlanNode{},
							types.CStorClusterPlanNode{},
						},
					},
				},
				ObservedStorageSets: []*unstructured.Unstructured{
					&unstructured.Unstructured{},
					&unstructured.Unstructured{},
				},
			},
			isReady: true,
		},
		"node count < observed storageset count": {
			planner: &Planner{
				CStorClusterPlan: &types.CStorClusterPlan{
					Spec: types.CStorClusterPlanSpec{
						Nodes: []types.CStorClusterPlanNode{
							types.CStorClusterPlanNode{},
						},
					},
				},
				ObservedStorageSets: []*unstructured.Unstructured{
					&unstructured.Unstructured{},
					&unstructured.Unstructured{},
				},
			},
			isReady: false,
		},
		"node count > observed storageset count": {
			planner: &Planner{
				CStorClusterPlan: &types.CStorClusterPlan{
					Spec: types.CStorClusterPlanSpec{
						Nodes: []types.CStorClusterPlanNode{
							types.CStorClusterPlanNode{},
							types.CStorClusterPlanNode{},
						},
					},
				},
				ObservedStorageSets: []*unstructured.Unstructured{
					&unstructured.Unstructured{},
				},
			},
			isReady: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := mock.planner.isReadyByNodeCount()
			if got != mock.isReady {
				t.Fatalf("Want %t got %t", mock.isReady, got)
			}
		})
	}
}

func TestPlannerIsReadyByNodeDiskCount(t *testing.T) {
	mockloginfo := &types.CStorClusterPlan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "test",
		},
	}
	var tests = map[string]struct {
		planner *Planner
		isReady bool
	}{
		"desired disk count == observed disk count": {
			planner: &Planner{
				storageSetToDesiredDiskCount: map[string]resource.Quantity{
					"101": resource.MustParse("1"),
				},
				storageSetToBlockDevices: map[string][]string{
					"101": []string{"bd1"},
				},
			},
			isReady: true,
		},
		"desired disk count > observed disk count": {
			planner: &Planner{
				// TODO (@amitkumardas):
				// 	Use log as a field in Planner
				CStorClusterPlan: mockloginfo,
				storageSetToDesiredDiskCount: map[string]resource.Quantity{
					"101": resource.MustParse("2"),
				},
				storageSetToBlockDevices: map[string][]string{
					"101": []string{"bd1"},
				},
			},
			isReady: false,
		},
		"desired disk count < observed disk count": {
			planner: &Planner{
				storageSetToDesiredDiskCount: map[string]resource.Quantity{
					"101": resource.MustParse("2"),
				},
				storageSetToBlockDevices: map[string][]string{
					"101": []string{"bd1", "bd2", "bd3"},
				},
			},
			isReady: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := mock.planner.isReadyByNodeDiskCount()
			if got != mock.isReady {
				t.Fatalf("Want %t got %t", mock.isReady, got)
			}
		})
	}
}
