/*
Copyright 2020 The MayaData Authors.

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

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
)

func TestNewHelper(t *testing.T) {
	var tests = map[string]struct {
		cstorClusterConfig *unstructured.Unstructured
		isErr              bool
	}{
		"nil cstor cluster config": {
			cstorClusterConfig: nil,
			isErr:              true,
		},
		"nil cstor cluster config object": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"invalid cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Junk",
				},
			},
			isErr: true,
		},
		"cstor cluster config with valid kind": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
				},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.cstorClusterConfig)
			if mock.isErr && h.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && h.err != nil {
				t.Fatalf("Expected no error got [%+v]", h.err)
			}
		})
	}
}

func TestHelperIsLocalDisk(t *testing.T) {
	var tests = map[string]struct {
		cstorClusterConfig *unstructured.Unstructured
		isLocal            bool
		isErr              bool
	}{
		"nil cstor cluster config": {
			cstorClusterConfig: nil,
			isErr:              true,
		},
		"nil cstor cluster config object": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"invalid cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Junk",
				},
			},
			isErr: true,
		},
		"cstor cluster config with valid kind": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
				},
			},
			isErr: false,
		},
		"cstor cluster config && valid kind && remote disk": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"remote": map[string]interface{}{},
						},
					},
				},
			},
			isLocal: false,
			isErr:   false,
		},
		"cstor cluster config && valid kind && nil local disk": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": nil,
						},
					},
				},
			},
			isLocal: false,
			isErr:   false,
		},
		"cstor cluster config && valid kind && valid local disk select terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": []interface{}{
										map[string]interface{}{},
									},
								},
							},
						},
					},
				},
			},
			isLocal: true,
			isErr:   false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.cstorClusterConfig)
			got, err := h.IsLocalBlockDiskConfig()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if got != mock.isLocal {
				t.Fatalf("Expected local %t got %t", mock.isLocal, got)
			}
		})
	}
}

func TestHelperIsDiskCountMatchRAIDType(t *testing.T) {
	var tests = map[string]struct {
		cstorClusterConfig *unstructured.Unstructured
		diskCount          int64
		isMatch            bool
		isErr              bool
	}{
		"2 disks mirror cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			diskCount: 2,
			isMatch:   true,
			isErr:     false,
		},
		"3 disks mirror cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			diskCount: 3,
			isMatch:   false,
			isErr:     false,
		},
		"4 disks mirror cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			diskCount: 4,
			isMatch:   true,
			isErr:     false,
		},
		"3 disks raidz cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			diskCount: 3,
			isMatch:   true,
			isErr:     false,
		},
		"4 disks raidz cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			diskCount: 4,
			isMatch:   false,
			isErr:     false,
		},
		"1 disks raidz cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			diskCount: 1,
			isMatch:   false,
			isErr:     false,
		},
		"6 disks raidz2 cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ2),
						},
					},
				},
			},
			diskCount: 6,
			isMatch:   true,
			isErr:     false,
		},
		"7 disks raidz2 cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ2),
						},
					},
				},
			},
			diskCount: 7,
			isMatch:   false,
			isErr:     false,
		},
		"2 disks stripe cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeStripe),
						},
					},
				},
			},
			diskCount: 2,
			isMatch:   true,
			isErr:     false,
		},
		"1 disks stripe cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeStripe),
						},
					},
				},
			},
			diskCount: 1,
			isMatch:   true,
			isErr:     false,
		},
		"0 disks stripe cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeStripe),
						},
					},
				},
			},
			diskCount: 0,
			isMatch:   false,
			isErr:     true,
		},
		"-1 disks stripe cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeStripe),
						},
					},
				},
			},
			diskCount: -1,
			isMatch:   false,
			isErr:     true,
		},
		"0 disks mirror cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			diskCount: 0,
			isMatch:   false,
			isErr:     true,
		},
		"-1 disks mirror cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			diskCount: -1,
			isMatch:   false,
			isErr:     true,
		},
		"0 disks raidz cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			diskCount: 0,
			isMatch:   false,
			isErr:     true,
		},
		"-1 disks raidz cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			diskCount: -1,
			isMatch:   false,
			isErr:     true,
		},
		"0 disks raidz2 cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ2),
						},
					},
				},
			},
			diskCount: 0,
			isMatch:   false,
			isErr:     true,
		},
		"-1 disks raidz2 cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ2),
						},
					},
				},
			},
			diskCount: -1,
			isMatch:   false,
			isErr:     true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.cstorClusterConfig)
			match, err := h.IsDiskCountMatchRAIDType(mock.diskCount)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Exepcted no error got [%+v]", err)
			}
			if mock.isMatch != match {
				t.Fatalf("Expected match %t got %t", mock.isMatch, match)
			}
		})
	}
}

func TestHelperGetRAIDType(t *testing.T) {
	var tests = map[string]struct {
		cstorClusterConfig *unstructured.Unstructured
		expectRAIDType     types.PoolRAIDType
		isErr              bool
	}{
		"nil cstor cluster config": {
			isErr: true,
		},
		"empty cstor cluster config object": {
			cstorClusterConfig: &unstructured.Unstructured{},
			isErr:              true,
		},
		"invalid cstor cluster config kind": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Junk",
				},
			},
			isErr: true,
		},
		"missing raid type": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": nil,
					},
				},
			},
			isErr: true,
		},
		"invalid raid type": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": "junk",
						},
					},
				},
			},
			isErr: true,
		},
		"mirror cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeMirror,
			isErr:          false,
		},
		"stripe cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeStripe),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeStripe,
			isErr:          false,
		},
		"raidz cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeRAIDZ,
			isErr:          false,
		},
		"raidz2 cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ2),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeRAIDZ2,
			isErr:          false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.cstorClusterConfig)
			got, err := h.GetRAIDType()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if got != mock.expectRAIDType {
				t.Fatalf("Expected raid type %s got %s", mock.expectRAIDType, got)
			}
		})
	}
}

func TestHelperGetRAIDTypeOrCached(t *testing.T) {
	var tests = map[string]struct {
		cstorClusterConfig *unstructured.Unstructured
		givenRAIDType      types.PoolRAIDType
		expectRAIDType     types.PoolRAIDType
		isErr              bool
	}{
		"no cache && raidz2": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ2),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeRAIDZ2,
			isErr:          false,
		},
		"raidz2 cache": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ2),
						},
					},
				},
			},
			givenRAIDType:  types.PoolRAIDTypeRAIDZ2,
			expectRAIDType: types.PoolRAIDTypeRAIDZ2,
			isErr:          false,
		},
		"no cache && raidz": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeRAIDZ,
			isErr:          false,
		},
		"raidz cache": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeRAIDZ),
						},
					},
				},
			},
			givenRAIDType:  types.PoolRAIDTypeRAIDZ,
			expectRAIDType: types.PoolRAIDTypeRAIDZ,
			isErr:          false,
		},
		"no cache && mirror": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeMirror,
			isErr:          false,
		},
		"mirror cache": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeMirror),
						},
					},
				},
			},
			givenRAIDType:  types.PoolRAIDTypeMirror,
			expectRAIDType: types.PoolRAIDTypeMirror,
			isErr:          false,
		},
		"no cache && stripe": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeStripe),
						},
					},
				},
			},
			expectRAIDType: types.PoolRAIDTypeStripe,
			isErr:          false,
		},
		"stripe cache": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"poolConfig": map[string]interface{}{
							"raidType": string(types.PoolRAIDTypeStripe),
						},
					},
				},
			},
			givenRAIDType:  types.PoolRAIDTypeStripe,
			expectRAIDType: types.PoolRAIDTypeStripe,
			isErr:          false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.cstorClusterConfig)
			h.raidType = mock.givenRAIDType
			got, err := h.GetRAIDTypeOrCached()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if got != mock.expectRAIDType {
				t.Fatalf("Expected raid type %s got %s", mock.expectRAIDType, got)
			}
		})
	}
}

func TestHelperGetLocalDiskSelector(t *testing.T) {
	var tests = map[string]struct {
		cstorClusterConfig *unstructured.Unstructured
		expectSelector     metac.ResourceSelector
		isErr              bool
	}{
		"nil cstor cluster config": {
			cstorClusterConfig: nil,
			expectSelector:     nilselector,
			isErr:              true,
		},
		"nil cstor cluster config object": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: nil,
			},
			expectSelector: nilselector,
			isErr:          true,
		},
		"invalid cstor cluster config": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Junk",
				},
			},
			expectSelector: nilselector,
			isErr:          true,
		},
		"cstor cluster config with valid kind": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
				},
			},
			expectSelector: nilselector,
			isErr:          true,
		},
		"cstor cluster config && valid kind && remote disk": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"remote": map[string]interface{}{},
						},
					},
				},
			},
			expectSelector: nilselector,
			isErr:          true,
		},
		"cstor cluster config && valid kind && nil local disk": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": nil,
						},
					},
				},
			},
			expectSelector: nilselector,
			isErr:          true,
		},
		"cstor cluster config && valid kind && nil selector terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": nil,
								},
							},
						},
					},
				},
			},
			expectSelector: metac.ResourceSelector{
				SelectorTerms: nil,
			},
			isErr: false,
		},
		"cstor cluster config && valid kind && empty selector terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": []interface{}{},
								},
							},
						},
					},
				},
			},
			expectSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{},
			},
			isErr: false,
		},
		"cstor cluster config && valid kind && 1 empty item in selector terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": []interface{}{
										map[string]interface{}{},
									},
								},
							},
						},
					},
				},
			},
			expectSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{{}},
			},
			isErr: false,
		},
		"cstor cluster config && valid kind && 1 empty matchslice in selector terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": []interface{}{
										map[string]interface{}{
											"matchSlice": map[string]interface{}{},
										},
									},
								},
							},
						},
					},
				},
			},
			expectSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchSlice: map[string][]string{},
					},
				},
			},
			isErr: false,
		},
		"cstor cluster config && valid kind && 1 nil matchslice in selector terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": []interface{}{
										map[string]interface{}{
											"matchSlice": nil,
										},
									},
								},
							},
						},
					},
				},
			},
			expectSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchSlice: nil,
					},
				},
			},
			isErr: false,
		},
		"cstor cluster config && valid kind && 1 matchslice in selector terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": []interface{}{
										map[string]interface{}{
											"matchSlice": map[string][]interface{}{
												"pool.items": []interface{}{"p0"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchSlice: map[string][]string{
							"pool.items": []string{"p0"},
						},
					},
				},
			},
			isErr: false,
		},
		"cstor cluster config && valid kind && 1 matchsliceexp in selector terms": {
			cstorClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorClusterConfig),
					"spec": map[string]interface{}{
						"diskConfig": map[string]interface{}{
							"local": map[string]interface{}{
								"blockDeviceSelector": map[string]interface{}{
									"selectorTerms": []interface{}{
										map[string]interface{}{
											"matchSliceExpressions": []interface{}{
												map[string]interface{}{
													"key":      "pool.items",
													"operator": "Equals",
													"values":   []interface{}{"p0"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectSelector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchSliceExpressions: []metac.SliceSelectorRequirement{
							metac.SliceSelectorRequirement{
								Key:      "pool.items",
								Operator: metac.SliceSelectorOpEquals,
								Values:   []string{"p0"},
							},
						},
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
			h := NewHelper(mock.cstorClusterConfig)
			got, err := h.GetLocalBlockDeviceSelector()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !reflect.DeepEqual(got, mock.expectSelector) {
				t.Fatalf("Expected no diff got\n%s",
					cmp.Diff(got, mock.expectSelector),
				)
			}
		})
	}
}
