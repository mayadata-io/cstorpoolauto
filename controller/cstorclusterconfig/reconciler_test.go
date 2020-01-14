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
	"cstorpoolauto/types"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

func TestReconcilerValidateDiskExternalProvisioner(t *testing.T) {
	var tests = map[string]struct {
		CStorClusterConfig *types.CStorClusterConfig
		isErr              bool
	}{
		"ExternalProvisioner = nil": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			isErr:              true,
		},
		"ExternalProvisioner is empty": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalProvisioner: types.ExternalProvisioner{
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
						ExternalProvisioner: types.ExternalProvisioner{
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
						ExternalProvisioner: types.ExternalProvisioner{
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
			got := r.validateDiskExternalProvisioner()
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
		CStorClusterConfig *types.CStorClusterConfig
		NodePlanner        *NodePlanner
		isErr              bool
	}{
		"external provisioner = nil": {
			CStorClusterConfig: &types.CStorClusterConfig{},
			isErr:              true,
		},
		"external provisioner = empty": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalProvisioner: types.ExternalProvisioner{
							CSIAttacherName:  "",
							StorageClassName: "",
						},
					},
				},
			},
			isErr: true,
		},
		"external provisioner is set && nodes = 1 && elgible nodes = 1": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalProvisioner: types.ExternalProvisioner{
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
			isErr: false,
		},
		"external provisioner is set && nodes = 2 && elgible nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalProvisioner: types.ExternalProvisioner{
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
		"external provisioner is set && nodes = 0 && elgible nodes = 0": {
			CStorClusterConfig: &types.CStorClusterConfig{
				Spec: types.CStorClusterConfigSpec{
					DiskConfig: types.DiskConfig{
						ExternalProvisioner: types.ExternalProvisioner{
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
				},
			},
			expectPlan: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "dao.mayadata.io/v1alpha1",
					"metadata": map[string]interface{}{
						"namespace": "default",
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
			if got.GetAPIVersion() != mock.expectPlan.GetAPIVersion() {
				t.Fatalf(
					"Expected APIVersion %s got %s",
					got.GetAPIVersion(), mock.expectPlan.GetAPIVersion(),
				)
			}
			if got.GetNamespace() != mock.expectPlan.GetNamespace() {
				t.Fatalf(
					"Expected Namespace %s got %s",
					got.GetNamespace(), mock.expectPlan.GetNamespace(),
				)
			}
		})
	}
}
