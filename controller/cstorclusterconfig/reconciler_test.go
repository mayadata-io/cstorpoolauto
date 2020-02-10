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
	"reflect"
	"testing"

	"mayadata.io/cstorpoolauto/types"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"
	"openebs.io/metac/controller/common"
	"openebs.io/metac/dynamic/apply"
)

func TestReconcilerSetMinPoolCountIfNotSet(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		NodePlanner        *NodePlanner
		isErr              bool
	}{
		"min pool count = nil && nodes = 0 && allowed nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 0
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 0, nil
				},
			},
			isErr: true,
		},
		"min pool count = 0 && nodes = 0 && allowed nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					MinPoolCount: resource.MustParse("0"),
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 0
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 0, nil
				},
			},
			isErr: true,
		},
		"min pool count = nil && nodes = 1 && allowed nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 0, nil
				},
			},
			isErr: true,
		},
		"min pool count = nil && nodes = 0 && allowed nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 0
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			isErr: true,
		},
		"min pool count = 1 && nodes = 0 && allowed nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					MinPoolCount: resource.MustParse("1"),
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 0
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 0, nil
				},
			},
			isErr: false,
		},
		"min pool count = 1 && nodes = 1 && allowed nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					MinPoolCount: resource.MustParse("1"),
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 0, nil
				},
			},
			isErr: false,
		},
		"min pool count = 1 && nodes = 0 && allowed nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					MinPoolCount: resource.MustParse("1"),
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 0
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			isErr: false,
		},
		"min pool count = -1 && nodes = 1 && allowed nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					MinPoolCount: resource.MustParse("-1"),
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
				NodePlanner:   mock.NodePlanner,
			}
			got := r.setMinPoolCountIfNotSet()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got %+v", got)
			}
		})
	}
}

func TestReconcilerSetMaxPoolCountIfNotSet(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		minPoolCount       int64
		isErr              bool
	}{
		"spec = nil": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			isErr:              false,
		},
		"max pool count = nil && min pool count = 2": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			minPoolCount:       2,
			isErr:              false,
		},
		"max pool count = 2 && min pool count = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					MaxPoolCount: resource.MustParse("2"),
				},
			},
			minPoolCount: 1,
			isErr:        false,
		},
		"max pool count = 1 && min pool count = 2": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					MaxPoolCount: resource.MustParse("1"),
				},
			},
			minPoolCount: 2,
			isErr:        true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
				minPoolCount:  mock.minPoolCount,
			}
			got := r.setMaxPoolCountIfNotSet()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got %+v", got)
			}
		})
	}
}

func TestReconcilerSetRAIDTypeIfNotSet(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		isErr              bool
	}{
		"nil raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{},
				},
			},
			isErr: false,
		},
		"empty raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{
						RAIDType: "",
					},
				},
			},
			isErr: false,
		},
		"mirror raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeMirror,
					},
				},
			},
			isErr: false,
		},
		"stripe raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeStripe,
					},
				},
			},
			isErr: false,
		},
		"raidz raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeRAIDZ,
					},
				},
			},
			isErr: false,
		},
		"raidz2 raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeRAIDZ2,
					},
				},
			},
			isErr: false,
		},
		"default raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeDefault,
					},
				},
			},
			isErr: false,
		},
		"invalid raid type": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDType("junk"),
					},
				},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
			}
			got := r.setRAIDTypeIfNotSet()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got %+v", got)
			}
		})
	}
}

func TestReconcilerSetMinDiskCountIfNotSet(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		RAIDType           types.PoolRAIDType
		isErr              bool
	}{
		"min disk count = 0 && PoolRAIDTypeDefault": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCount: resource.MustParse("0"),
					},
				},
			},
			RAIDType: types.PoolRAIDTypeDefault,
			isErr:    false,
		},
		"min disk count = nil && PoolRAIDTypeDefault": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeDefault,
			isErr:              false,
		},
		"min disk count = nil && PoolRAIDTypeMirror": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeMirror,
			isErr:              false,
		},
		"min disk count = nil && PoolRAIDTypeRAIDZ": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeRAIDZ,
			isErr:              false,
		},
		"min disk count = nil && PoolRAIDTypeRAIDZ2": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeRAIDZ2,
			isErr:              false,
		},
		"min disk count = nil && PoolRAIDTypeStripe": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 0 && PoolRAIDTypeStripe": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 1 && PoolRAIDTypeStripe": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 2 && PoolRAIDTypeStripe": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 0 && PoolRAIDTypeMirror": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 1 && PoolRAIDTypeMirror": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 2 && PoolRAIDTypeMirror": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 0 && PoolRAIDTypeRAIDZ": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 1 && PoolRAIDTypeRAIDZ": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
		"min disk count = 2 && PoolRAIDTypeRAIDZ": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			RAIDType:           types.PoolRAIDTypeStripe,
			isErr:              false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
			}
			got := r.setMinDiskCountIfNotSet()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got %+v", got)
			}
		})
	}
}

func TestReconcilerSetMinDiskCapacityIfNotSet(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		isErr              bool
	}{
		"min disk capacity = nil": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{},
			},
			isErr: false,
		},
		"min disk capacity = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCapacity: resource.MustParse("0"),
					},
				},
			},
			isErr: false,
		},
		"min disk capacity = -1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCapacity: resource.MustParse("-1"),
					},
				},
			},
			isErr: true,
		},
		"min disk capacity = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCapacity: resource.MustParse("1"),
					},
				},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
			}
			got := r.setMinDiskCapacityIfNotSet()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got %+v", got)
			}
		})
	}
}

func TestReconcilerValidateRAIDType(t *testing.T) {
	var tests = map[string]struct {
		RAIDType types.PoolRAIDType
		isErr    bool
	}{
		"Invalid RAID type": {
			RAIDType: types.PoolRAIDType("junk"),
			isErr:    true,
		},
		"RAID type = PoolRAIDTypeDefault": {
			RAIDType: types.PoolRAIDTypeDefault,
			isErr:    false,
		},
		"RAID type = PoolRAIDTypeMirror": {
			RAIDType: types.PoolRAIDTypeMirror,
			isErr:    false,
		},
		"RAID type = PoolRAIDTypeRAIDZ": {
			RAIDType: types.PoolRAIDTypeRAIDZ,
			isErr:    false,
		},
		"RAID type = PoolRAIDTypeRAIDZ2": {
			RAIDType: types.PoolRAIDTypeRAIDZ2,
			isErr:    false,
		},
		"RAID type = PoolRAIDTypeStripe": {
			RAIDType: types.PoolRAIDTypeStripe,
			isErr:    false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				poolRAIDType: mock.RAIDType,
			}
			got := r.validateRAIDType()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got [%+v]", got)
			}
		})
	}
}

func TestReconcilerValidateMinDiskCount(t *testing.T) {
	var tests = map[string]struct {
		MinDiskCount int64
		RAIDType     types.PoolRAIDType
		isErr        bool
	}{
		"Disk Count = nil": {
			isErr: true,
		},
		"Disk Count = 0": {
			MinDiskCount: 0,
			isErr:        true,
		},
		"Disk Count = nil && RAID type = default": {
			RAIDType: types.PoolRAIDTypeDefault,
			isErr:    true,
		},
		"Disk Count = 0 && RAID type = default": {
			MinDiskCount: 0,
			RAIDType:     types.PoolRAIDTypeDefault,
			isErr:        true,
		},
		"Disk Count = nil && RAID type = stripe": {
			RAIDType: types.PoolRAIDTypeStripe,
			isErr:    true,
		},
		"Disk Count = 0 && RAID type = stripe": {
			MinDiskCount: 0,
			RAIDType:     types.PoolRAIDTypeStripe,
			isErr:        true,
		},
		"Disk Count = 1 && RAID type = default": {
			MinDiskCount: 1,
			RAIDType:     types.PoolRAIDTypeDefault,
			isErr:        true,
		},
		"Disk Count = 1 && RAID type = stripe": {
			MinDiskCount: 1,
			RAIDType:     types.PoolRAIDTypeStripe,
			isErr:        false,
		},
		"Disk Count = 2 && RAID type = default": {
			MinDiskCount: 2,
			RAIDType:     types.PoolRAIDTypeDefault,
			isErr:        false,
		},
		"Disk Count = 2 && RAID type = stripe": {
			MinDiskCount: 2,
			RAIDType:     types.PoolRAIDTypeStripe,
			isErr:        false,
		},
		"Disk Count = 1 && RAID type = raidz": {
			MinDiskCount: 1,
			RAIDType:     types.PoolRAIDTypeRAIDZ,
			isErr:        true,
		},
		"Disk Count = 2 && RAID type = raidz": {
			MinDiskCount: 2,
			RAIDType:     types.PoolRAIDTypeRAIDZ,
			isErr:        true,
		},
		"Disk Count = 3 && RAID type = raidz": {
			MinDiskCount: 3,
			RAIDType:     types.PoolRAIDTypeRAIDZ,
			isErr:        false,
		},
		"Disk Count = 1 && RAID type = raidz2": {
			MinDiskCount: 1,
			RAIDType:     types.PoolRAIDTypeRAIDZ2,
			isErr:        true,
		},
		"Disk Count = 2 && RAID type = raidz2": {
			MinDiskCount: 2,
			RAIDType:     types.PoolRAIDTypeRAIDZ2,
			isErr:        true,
		},
		"Disk Count = 3 && RAID type = raidz2": {
			MinDiskCount: 3,
			RAIDType:     types.PoolRAIDTypeRAIDZ2,
			isErr:        true,
		},
		"Disk Count = 4 && RAID type = raidz2": {
			MinDiskCount: 4,
			RAIDType:     types.PoolRAIDTypeRAIDZ2,
			isErr:        true,
		},
		"Disk Count = 5 && RAID type = raidz2": {
			MinDiskCount: 5,
			RAIDType:     types.PoolRAIDTypeRAIDZ2,
			isErr:        true,
		},
		"Disk Count = 6 && RAID type = raidz2": {
			MinDiskCount: 6,
			RAIDType:     types.PoolRAIDTypeRAIDZ2,
			isErr:        false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				minDiskCount: mock.MinDiskCount,
				poolRAIDType: mock.RAIDType,
			}
			got := r.validateMinDiskCount()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got [%+v]", got)
			}
		})
	}
}

func TestReconcilerValidateExternalDiskConfig(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		isErr              bool
	}{
		"ExternalDiskConfig = nil": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			isErr:              true,
		},
		"ExternalDiskConfig is empty": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "",
							StorageClassName: "",
						},
					},
				},
			},
			isErr: true,
		},
		"CSIAttacherName = empty && StorageClassName = non-empty": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "",
							StorageClassName: "default",
						},
					},
				},
			},
			isErr: true,
		},
		"CSIAttacherName = non-empty && StorageClassName = empty": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "some-csi-driver",
							StorageClassName: "",
						},
					},
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
			}
			got := r.validateExternalDiskConfig()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got [%+v]", got)
			}
		})
	}
}

func TestReconcilerSyncClusterConfig(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig    *types.CStorClusterConfig
		NodePlanner           *NodePlanner
		expectMinPoolCount    int64
		expectMaxPoolCount    int64
		expectPoolRAIDType    types.PoolRAIDType
		expectMinDiskCount    int64
		expectMinDiskCapacity resource.Quantity
		isErr                 bool
	}{
		"ExternalDiskConfig = nil": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			isErr:              true,
		},
		"ExternalDiskConfig = empty": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "",
							StorageClassName: "",
						},
					},
				},
			},
			isErr: true,
		},
		"ExternalDiskConfig is set && nodes = 1 && elgible nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			expectMinPoolCount:    1,
			expectMaxPoolCount:    3,
			expectPoolRAIDType:    types.PoolRAIDTypeMirror,
			expectMinDiskCount:    2,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && nodes = 2 && elgible nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 2
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			expectMinPoolCount:    1,
			expectMaxPoolCount:    3,
			expectPoolRAIDType:    types.PoolRAIDTypeMirror,
			expectMinDiskCount:    2,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && nodes = 2 && elgible nodes = 2": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 2
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 2, nil
				},
			},
			expectMinPoolCount:    2,
			expectMaxPoolCount:    4,
			expectPoolRAIDType:    types.PoolRAIDTypeMirror,
			expectMinDiskCount:    2,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && raid type = mirror && eligible nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeMirror,
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			expectMinPoolCount:    1,
			expectMaxPoolCount:    3,
			expectPoolRAIDType:    types.PoolRAIDTypeMirror,
			expectMinDiskCount:    2,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && raid type = stripe && eligible nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeStripe,
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			expectMinPoolCount:    1,
			expectMaxPoolCount:    3,
			expectPoolRAIDType:    types.PoolRAIDTypeStripe,
			expectMinDiskCount:    1,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && raid type = stripe && eligible nodes = 1 && disk = 3": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCount: resource.MustParse("3"),
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeStripe,
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			expectMinPoolCount:    1,
			expectMaxPoolCount:    3,
			expectPoolRAIDType:    types.PoolRAIDTypeStripe,
			expectMinDiskCount:    3,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && raid type = raidz && eligible nodes = 1 && disk = 3": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCount: resource.MustParse("3"),
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeRAIDZ,
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			expectMinPoolCount:    1,
			expectMaxPoolCount:    3,
			expectPoolRAIDType:    types.PoolRAIDTypeRAIDZ,
			expectMinDiskCount:    3,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && raid type = raidz2 && eligible nodes = 1 && disk = 6": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCount: resource.MustParse("6"),
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeRAIDZ2,
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			expectMinPoolCount:    1,
			expectMaxPoolCount:    3,
			expectPoolRAIDType:    types.PoolRAIDTypeRAIDZ2,
			expectMinDiskCount:    6,
			expectMinDiskCapacity: DefaultMinDiskCapacity,
			isErr:                 false,
		},
		"ExternalDiskConfig is set && raid type = raidz && eligible nodes = 1 && disk = 5": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCount: resource.MustParse("5"),
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeRAIDZ,
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			isErr: true,
		},
		"ExternalDiskConfig is set && raid type = raidz2 && eligible nodes = 1 && disk = 5": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						MinCount: resource.MustParse("5"),
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
					PoolConfig: types.PoolConfig{
						RAIDType: types.PoolRAIDTypeRAIDZ2,
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 1
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 1, nil
				},
			},
			isErr: true,
		},
		"ExternalDiskConfig is set && nodes = 2 && elgible nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 2
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 0, nil
				},
			},
			isErr: true,
		},
		"ExternalDiskConfig is set && nodes = 0 && elgible nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{
							CSIAttacherName:  "abc-driver",
							StorageClassName: "default",
						},
					},
				},
			},
			NodePlanner: &NodePlanner{
				getAllNodeCountFn: func() int64 {
					return 0
				},
				getAllowedNodeCountFn: func() (int64, error) {
					return 0, nil
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
				NodePlanner:   mock.NodePlanner,
			}
			got := r.syncClusterConfig()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got [%+v]", got)
			}
			if mock.isErr {
				return
			}
			if mock.expectMinPoolCount != r.minPoolCount {
				t.Fatalf(
					"Expected min pool count %d got %d",
					mock.expectMinPoolCount, r.minPoolCount,
				)
			}
			if mock.expectMaxPoolCount != r.maxPoolCount {
				t.Fatalf("Expected max pool count %d got %d",
					mock.expectMaxPoolCount, r.maxPoolCount,
				)
			}
			if mock.expectPoolRAIDType != r.poolRAIDType {
				t.Fatalf("Expected pool raid type %s got %s",
					mock.expectPoolRAIDType, r.poolRAIDType,
				)
			}
			if mock.expectMinDiskCount != r.minDiskCount {
				t.Fatalf("Expected min disk count %d got %d",
					mock.expectMinDiskCount, r.minDiskCount,
				)
			}
			if mock.expectMinDiskCapacity.Value() != r.minDiskCapacity {
				t.Fatalf("Expected min disk capacity %d got %d",
					mock.expectMinDiskCapacity.Value(), r.minDiskCapacity,
				)
			}
		})
	}
}

func TestNewReconciler(t *testing.T) {
	var tests = map[string]struct {
		ClusterConfig *unstructured.Unstructured
		ClusterPlan   *unstructured.Unstructured
		isErr         bool
	}{
		"ClusterConfig = nil && ClusterPlan = nil": {
			ClusterConfig: nil,
			ClusterPlan:   nil,
			isErr:         true,
		},
		"ClusterConfig = empty && ClusterPlan = nil": {
			ClusterConfig: &unstructured.Unstructured{},
			ClusterPlan:   nil,
			isErr:         false,
		},
		"ClusterConfig = in-valid && ClusterPlan = nil": {
			ClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "ABC",
				},
			},
			ClusterPlan: nil,
			isErr:       false,
		},
		"ClusterConfig = valid && ClusterPlan = nil": {
			ClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "CStorClusterConfig",
				},
			},
			ClusterPlan: nil,
			isErr:       false,
		},
		"ClusterConfig = valid && ClusterPlan = empty": {
			ClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "CStorClusterConfig",
				},
			},
			ClusterPlan: &unstructured.Unstructured{},
			isErr:       false,
		},
		"ClusterConfig = valid && ClusterPlan = in-valid": {
			ClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "CStorClusterConfig",
				},
			},
			ClusterPlan: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "DEF",
				},
			},
			isErr: false,
		},
		"ClusterConfig = valid && ClusterPlan = valid": {
			ClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "CStorClusterConfig",
				},
			},
			ClusterPlan: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "CStorClusterPlan",
				},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r, err := NewReconciler(mock.ClusterConfig, mock.ClusterPlan, nil)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && mock.ClusterConfig != nil && r.ClusterConfig == nil {
				t.Fatalf("Expected not nil ClusterConfig got nil")
			}
			if !mock.isErr && mock.ClusterPlan != nil && r.ClusterPlan == nil {
				t.Fatalf("Expected not nil ClusterPlan got nil")
			}
			if !mock.isErr && mock.ClusterPlan == nil && r.ClusterPlan != nil {
				t.Fatalf("Expected nil ClusterPlan got [%+v]", r.ClusterPlan)
			}
		})
	}
}

func TestReconcilerTestSyncClusterPlan(t *testing.T) {
	var tests = map[string]struct {
		ClusterPlan *types.CStorClusterPlan
		nodePlanFn  func(NodePlannerConfig) ([]types.CStorClusterPlanNode, error)
		isErr       bool
	}{
		"ClusterPlan = nil && Planned nodes = nil": {
			nodePlanFn: func(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error) {
				return nil, nil
			},
			isErr: true,
		},
		"ClusterPlan = nil && Planned nodes = empty": {
			nodePlanFn: func(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error) {
				return []types.CStorClusterPlanNode{}, nil
			},
			isErr: true,
		},
		"ClusterPlan = nil && Planned nodes count = 1": {
			nodePlanFn: func(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error) {
				return []types.CStorClusterPlanNode{
					types.CStorClusterPlanNode{},
				}, nil
			},
			isErr: false,
		},
		"ClusterPlan = empty && Planned nodes count = 1": {
			ClusterPlan: &types.CStorClusterPlan{},
			nodePlanFn: func(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error) {
				return []types.CStorClusterPlanNode{
					types.CStorClusterPlanNode{},
				}, nil
			},
			isErr: false,
		},
		"ClusterPlan = 2 node && Planned nodes count = 1": {
			ClusterPlan: &types.CStorClusterPlan{
				Spec: types.CStorClusterPlanSpec{
					Nodes: []types.CStorClusterPlanNode{
						types.CStorClusterPlanNode{
							Name: "node1",
							UID:  "101",
						},
						types.CStorClusterPlanNode{
							Name: "node2",
							UID:  "102",
						},
					},
				},
			},
			nodePlanFn: func(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error) {
				return []types.CStorClusterPlanNode{
					types.CStorClusterPlanNode{},
				}, nil
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterPlan: mock.ClusterPlan,
				NodePlanner: &NodePlanner{
					planFn: mock.nodePlanFn,
				},
			}
			got := r.syncClusterPlan()
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got [%+v]", got)
			}
		})
	}
}
func TestReconcilerGetDesiredClusterConfig(t *testing.T) {
	var tests = map[string]struct {
		clusterConfig *types.CStorClusterConfig
		expectConfig  *unstructured.Unstructured
	}{
		"simple cluster config": {
			clusterConfig: &types.CStorClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test",
				},
			},
			expectConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorClusterConfig),
					"apiVersion": string(types.APIVersionDAOMayaDataV1Alpha1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "default",
					},
					"spec": map[string]interface{}{
						"minPoolCount": int64(0),
						"maxPoolCount": int64(0),
						"diskConfig": map[string]interface{}{
							"minCapacity": int64(0),
							"minCount":    int64(0),
						},
						"poolConfig": map[string]interface{}{
							"raidType": "",
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
			r := &Reconciler{
				ClusterConfig: mock.clusterConfig,
			}
			got := r.getDesiredClusterConfig()
			if !reflect.DeepEqual(got, mock.expectConfig) {
				t.Fatalf("Expected no diff got \n%s", cmp.Diff(got, mock.expectConfig))
			}
		})
	}
}

func TestReconcilerGetDesiredClusterPlan(t *testing.T) {
	var tests = map[string]struct {
		ClusterConfig *types.CStorClusterConfig
		desiredNodes  []types.CStorClusterPlanNode
		expectPlan    *unstructured.Unstructured
	}{
		"desired nodes = nil": {
			ClusterConfig: &types.CStorClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test",
					UID:       "test-101",
				},
			},
			expectPlan: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "dao.mayadata.io/v1alpha1",
					"kind":       string(types.KindCStorClusterPlan),
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterConfigUID): "test-101",
						},
					},
					"spec": map[string]interface{}{
						"nodes": []interface{}(nil),
					},
				},
			},
		},
		"desired node count = 1": {
			ClusterConfig: &types.CStorClusterConfig{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test",
					UID:       "test-101",
				},
			},
			desiredNodes: []types.CStorClusterPlanNode{
				types.CStorClusterPlanNode{
					Name: "node-101",
					UID:  "node-101",
				},
			},
			expectPlan: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "dao.mayadata.io/v1alpha1",
					"kind":       string(types.KindCStorClusterPlan),
					"metadata": map[string]interface{}{
						"namespace": "default",
						"name":      "test",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterConfigUID): "test-101",
						},
					},
					"spec": map[string]interface{}{
						"nodes": []interface{}{
							map[string]interface{}{
								"name": "node-101",
								"uid":  "node-101",
							},
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
			r := &Reconciler{
				ClusterConfig: mock.ClusterConfig,
			}
			got := r.getDesiredClusterPlan(mock.desiredNodes)
			if !reflect.DeepEqual(got, mock.expectPlan) {
				t.Fatalf("Expected no diff got \n%s", cmp.Diff(got, mock.expectPlan))
			}
		})
	}
}

func TestValidateDiskConfig(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		isErr              bool
	}{
		"valid cstor cluster config - external disk config": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{},
						LocalDiskConfig:    nil,
					},
				},
			},
			isErr: false,
		},
		"valid cstor cluster config - local disk config": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: nil,
						LocalDiskConfig:    &types.LocalDiskConfig{},
					},
				},
			},
			isErr: false,
		},
		"invalid cstor cluster config - both local & external disk config": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{},
						LocalDiskConfig:    &types.LocalDiskConfig{},
					},
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
			}
			err := r.validateDiskConfig()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}

func TestIsExternalDiskConfig(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		expectExternal     bool
	}{
		"both local & external as non nil": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{},
						LocalDiskConfig:    &types.LocalDiskConfig{},
					},
				},
			},
			expectExternal: true,
		},
		"local = non nil & external = nil": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: nil,
						LocalDiskConfig:    &types.LocalDiskConfig{},
					},
				},
			},
			expectExternal: false,
		},
		"local = nil & external = non nil": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalDiskConfig: &types.ExternalDiskConfig{},
						LocalDiskConfig:    nil,
					},
				},
			},
			expectExternal: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := &Reconciler{
				ClusterConfig: mock.CStorClusterConfig,
			}
			got := r.isExternalDiskConfig()
			if got != mock.expectExternal {
				t.Fatalf("Expected external %t got %t", mock.expectExternal, got)
			}
		})
	}
}

// TestMetacMergeCStorClusterPlan is a unit test that takes in real world
// inputs & verifies if that works or not
func TestMetacMergeCStorClusterPlan(t *testing.T) {
	var tests = map[string]struct {
		observed    string
		lastApplied string
		desired     string
		want        string
	}{
		"cstorclusterplan whose observed == lastapplied == desired": {
			observed: `{
				"apiVersion": "dao.mayadata.io/v1alpha1",
				"kind": "CStorClusterPlan",
				"metadata": {
				  "annotations": {
					"d28cba04-4bc0-11ea-a70d-42010a800115/gctl-last-applied": "{\"apiVersion\":\"dao.mayadata.io/v1alpha1\",\"kind\":\"CStorClusterPlan\",\"metadata\":{\"annotations\":{\"dao.mayadata.io/cstorclusterconfig-uid\":\"d28cba04-4bc0-11ea-a70d-42010a800115\"},\"name\":\"my-cstor-cluster\",\"namespace\":\"openebs\"},\"spec\":{\"nodes\":[{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-wt3x\",\"uid\":\"bb3318c7-1b11-11ea-a90b-42010a800016\"},{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-blz8\",\"uid\":\"bb3c94a2-1b11-11ea-a90b-42010a800016\"},{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-fvsb\",\"uid\":\"ba217ce0-1b11-11ea-a90b-42010a800016\"}]}}",
					"dao.mayadata.io/cstorclusterconfig-uid": "d28cba04-4bc0-11ea-a70d-42010a800115",
					"metac.openebs.io/created-due-to-watch": "d28cba04-4bc0-11ea-a70d-42010a800115"
				  },
				  "creationTimestamp": "2020-02-10T04:50:12Z",
				  "generation": 1,
				  "name": "my-cstor-cluster",
				  "namespace": "openebs",
				  "resourceVersion": "26400255",
				  "selfLink": "/apis/dao.mayadata.io/v1alpha1/namespaces/openebs/cstorclusterplans/my-cstor-cluster",
				  "uid": "d28f9d03-4bc0-11ea-a70d-42010a800115"
				},
				"spec": {
				  "nodes": [
					{
					  "name": "gke-amitd-dao-d-default-pool-fcc50975-wt3x",
					  "uid": "bb3318c7-1b11-11ea-a90b-42010a800016"
					},
					{
					  "name": "gke-amitd-dao-d-default-pool-fcc50975-blz8",
					  "uid": "bb3c94a2-1b11-11ea-a90b-42010a800016"
					},
					{
					  "name": "gke-amitd-dao-d-default-pool-fcc50975-fvsb",
					  "uid": "ba217ce0-1b11-11ea-a90b-42010a800016"
					}
				  ]
				}
			  }`,
			lastApplied: `{
				"apiVersion":"dao.mayadata.io/v1alpha1",
				"kind":"CStorClusterPlan",
				"metadata":{
					"annotations":{
						"dao.mayadata.io/cstorclusterconfig-uid":"d28cba04-4bc0-11ea-a70d-42010a800115"
					},
					"name":"my-cstor-cluster",
					"namespace":"openebs"
				},
				"spec":{
					"nodes":[{
						"name":"gke-amitd-dao-d-default-pool-fcc50975-wt3x",
						"uid":"bb3318c7-1b11-11ea-a90b-42010a800016"
					},
					{
						"name":"gke-amitd-dao-d-default-pool-fcc50975-blz8",
						"uid":"bb3c94a2-1b11-11ea-a90b-42010a800016"
					},
					{
						"name":"gke-amitd-dao-d-default-pool-fcc50975-fvsb",
						"uid":"ba217ce0-1b11-11ea-a90b-42010a800016"
					}]
				}
			}`,
			desired: `{
				"apiVersion":"dao.mayadata.io/v1alpha1",
				"kind":"CStorClusterPlan",
				"metadata":{
					"annotations":{
						"dao.mayadata.io/cstorclusterconfig-uid":"d28cba04-4bc0-11ea-a70d-42010a800115"
					},
					"name":"my-cstor-cluster",
					"namespace":"openebs"
				},
				"spec":{
					"nodes":[{
						"name":"gke-amitd-dao-d-default-pool-fcc50975-wt3x",
						"uid":"bb3318c7-1b11-11ea-a90b-42010a800016"
					},
					{
						"name":"gke-amitd-dao-d-default-pool-fcc50975-blz8",
						"uid":"bb3c94a2-1b11-11ea-a90b-42010a800016"
					},
					{
						"name":"gke-amitd-dao-d-default-pool-fcc50975-fvsb",
						"uid":"ba217ce0-1b11-11ea-a90b-42010a800016"
					}]
				}
			}`,
			want: `{
				"apiVersion": "dao.mayadata.io/v1alpha1",
				"kind": "CStorClusterPlan",
				"metadata": {
				  "annotations": {
					"d28cba04-4bc0-11ea-a70d-42010a800115/gctl-last-applied": "{\"apiVersion\":\"dao.mayadata.io/v1alpha1\",\"kind\":\"CStorClusterPlan\",\"metadata\":{\"annotations\":{\"dao.mayadata.io/cstorclusterconfig-uid\":\"d28cba04-4bc0-11ea-a70d-42010a800115\"},\"name\":\"my-cstor-cluster\",\"namespace\":\"openebs\"},\"spec\":{\"nodes\":[{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-wt3x\",\"uid\":\"bb3318c7-1b11-11ea-a90b-42010a800016\"},{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-blz8\",\"uid\":\"bb3c94a2-1b11-11ea-a90b-42010a800016\"},{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-fvsb\",\"uid\":\"ba217ce0-1b11-11ea-a90b-42010a800016\"}]}}",
					"dao.mayadata.io/cstorclusterconfig-uid": "d28cba04-4bc0-11ea-a70d-42010a800115",
					"metac.openebs.io/created-due-to-watch": "d28cba04-4bc0-11ea-a70d-42010a800115"
				  },
				  "creationTimestamp": "2020-02-10T04:50:12Z",
				  "generation": 1,
				  "name": "my-cstor-cluster",
				  "namespace": "openebs",
				  "resourceVersion": "26400255",
				  "selfLink": "/apis/dao.mayadata.io/v1alpha1/namespaces/openebs/cstorclusterplans/my-cstor-cluster",
				  "uid": "d28f9d03-4bc0-11ea-a70d-42010a800115"
				},
				"spec": {
				  "nodes": [
					{
					  "name": "gke-amitd-dao-d-default-pool-fcc50975-wt3x",
					  "uid": "bb3318c7-1b11-11ea-a90b-42010a800016"
					},
					{
					  "name": "gke-amitd-dao-d-default-pool-fcc50975-blz8",
					  "uid": "bb3c94a2-1b11-11ea-a90b-42010a800016"
					},
					{
					  "name": "gke-amitd-dao-d-default-pool-fcc50975-fvsb",
					  "uid": "ba217ce0-1b11-11ea-a90b-42010a800016"
					}
				  ]
				}
			  }`,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			observed := make(map[string]interface{})
			if err := json.Unmarshal([]byte(mock.observed), &observed); err != nil {
				t.Fatalf("Can't unmarshal observed: %v", err)
			}
			lastApplied := make(map[string]interface{})
			if err := json.Unmarshal([]byte(mock.lastApplied), &lastApplied); err != nil {
				t.Fatalf("Can't unmarshal tc.lastApplied: %v", err)
			}
			desired := make(map[string]interface{})
			if err := json.Unmarshal([]byte(mock.desired), &desired); err != nil {
				t.Fatalf("Can't unmarshal desired: %v", err)
			}
			want := make(map[string]interface{})
			if err := json.Unmarshal([]byte(mock.want), &want); err != nil {
				t.Fatalf("Can't unmarshal want: %v", err)
			}
			got, err := apply.Merge(observed, lastApplied, desired)
			if err != nil {
				t.Fatalf("Merge error: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Got: %v\nWant: %v\nDiff: %s",
					got, want, cmp.Diff(got, want),
				)
			}
		})
	}
}

// TestMetacApplyCStorClusterPlan is a unit test that takes in real world
// inputs & verifies if that works or not
func TestMetacApplyCStorClusterPlan(t *testing.T) {
	var tests = map[string]struct {
		observed       *unstructured.Unstructured
		lastAppliedKey string
		desired        *unstructured.Unstructured
		isDiff         bool
	}{
		"cstorclusterplan whose observed == lastapplied == desired": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "dao.mayadata.io/v1alpha1",
					"kind":       "CStorClusterPlan",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"d28cba04-4bc0-11ea-a70d-42010a800115/gctl-last-applied": "{\"apiVersion\":\"dao.mayadata.io/v1alpha1\",\"kind\":\"CStorClusterPlan\",\"metadata\":{\"annotations\":{\"dao.mayadata.io/cstorclusterconfig-uid\":\"d28cba04-4bc0-11ea-a70d-42010a800115\"},\"name\":\"my-cstor-cluster\",\"namespace\":\"openebs\"},\"spec\":{\"nodes\":[{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-wt3x\",\"uid\":\"bb3318c7-1b11-11ea-a90b-42010a800016\"},{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-blz8\",\"uid\":\"bb3c94a2-1b11-11ea-a90b-42010a800016\"},{\"name\":\"gke-amitd-dao-d-default-pool-fcc50975-fvsb\",\"uid\":\"ba217ce0-1b11-11ea-a90b-42010a800016\"}]}}",
							"dao.mayadata.io/cstorclusterconfig-uid":                 "d28cba04-4bc0-11ea-a70d-42010a800115",
							"metac.openebs.io/created-due-to-watch":                  "d28cba04-4bc0-11ea-a70d-42010a800115",
						},
						"creationTimestamp": "2020-02-10T04:50:12Z",
						"generation":        "1",
						"name":              "my-cstor-cluster",
						"namespace":         "openebs",
						"resourceVersion":   "26400255",
						"selfLink":          "/apis/dao.mayadata.io/v1alpha1/namespaces/openebs/cstorclusterplans/my-cstor-cluster",
						"uid":               "d28f9d03-4bc0-11ea-a70d-42010a800115",
					},
					"spec": map[string]interface{}{
						"nodes": []interface{}{
							map[string]interface{}{
								"name": "gke-amitd-dao-d-default-pool-fcc50975-wt3x",
								"uid":  "bb3318c7-1b11-11ea-a90b-42010a800016",
							},
							map[string]interface{}{
								"name": "gke-amitd-dao-d-default-pool-fcc50975-blz8",
								"uid":  "bb3c94a2-1b11-11ea-a90b-42010a800016",
							},
							map[string]interface{}{
								"name": "gke-amitd-dao-d-default-pool-fcc50975-fvsb",
								"uid":  "ba217ce0-1b11-11ea-a90b-42010a800016",
							},
						},
					},
				},
			},
			lastAppliedKey: "d28cba04-4bc0-11ea-a70d-42010a800115/gctl-last-applied",
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "dao.mayadata.io/v1alpha1",
					"kind":       "CStorClusterPlan",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"dao.mayadata.io/cstorclusterconfig-uid": "d28cba04-4bc0-11ea-a70d-42010a800115",
						},
						"name":      "my-cstor-cluster",
						"namespace": "openebs",
					},
					"spec": map[string]interface{}{
						"nodes": []interface{}{
							map[string]interface{}{
								"name": "gke-amitd-dao-d-default-pool-fcc50975-wt3x",
								"uid":  "bb3318c7-1b11-11ea-a90b-42010a800016",
							},
							map[string]interface{}{
								"name": "gke-amitd-dao-d-default-pool-fcc50975-blz8",
								"uid":  "bb3c94a2-1b11-11ea-a90b-42010a800016",
							},
							map[string]interface{}{
								"name": "gke-amitd-dao-d-default-pool-fcc50975-fvsb",
								"uid":  "ba217ce0-1b11-11ea-a90b-42010a800016",
							},
						},
					},
				},
			},
			isDiff: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			// check if unstructured instance are proper
			_, err := mock.desired.MarshalJSON()
			if err != nil {
				t.Fatalf("Can't marshal desired: %+v", err)
			}
			_, err = mock.observed.MarshalJSON()
			if err != nil {
				t.Fatalf("Can't marshal observed: %+v", err)
			}

			// actual test starts
			a := common.NewApplyFromAnnKey(mock.lastAppliedKey)
			merged, err := a.Merge(mock.observed, mock.desired)
			if err != nil {
				t.Fatalf("Can't merge: %+v", err)
			}
			isDiff, err := a.HasMergeDiff()
			if err != nil {
				t.Fatalf("Failed to check diff: %+v", err)
			}
			if isDiff {
				t.Fatalf("Diff: %s", cmp.Diff(mock.observed, merged))
			}
		})
	}
}
