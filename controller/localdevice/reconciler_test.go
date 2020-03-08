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

package localdevice

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
	"openebs.io/metac/controller/common"
	"openebs.io/metac/controller/generic"
)

func TestSyncerValidateArgs(t *testing.T) {
	var tests = map[string]struct {
		syncer *syncer
		isErr  bool
	}{
		"nil request && nil response": {
			syncer: &syncer{
				request:  nil,
				response: nil,
			},
			isErr: true,
		},
		"nil request && not nil response": {
			syncer: &syncer{
				request:  nil,
				response: &generic.SyncHookResponse{},
			},
			isErr: true,
		},
		"nil watch && not nil response": {
			syncer: &syncer{
				request:  &generic.SyncHookRequest{},
				response: &generic.SyncHookResponse{},
			},
			isErr: true,
		},
		"nil watch object && not nil response": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{},
				},
				response: &generic.SyncHookResponse{},
			},
			isErr: true,
		},
		"not nil request && not nil response": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			mock.syncer.validateArgs()
			if mock.isErr && mock.syncer.fatal == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && mock.syncer.fatal != nil {
				t.Fatalf("Expected no error got [%+v]", mock.syncer.fatal)
			}
		})
	}
}

func TestSyncerSkipIfNotLocalDisk(t *testing.T) {
	var tests = map[string]struct {
		syncer *syncer
		isSkip bool
		isErr  bool
	}{
		"empty watch": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isErr: true,
		},
		"invalid watch kind": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": "Junk",
						},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isErr: true,
		},
		"valid watch kind && empty specs": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
						},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
			isErr:  false,
		},
		"valid watch kind && empty diskconfig": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{},
							},
						},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
			isErr:  false,
		},
		"valid watch kind && nil diskconfig.local": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": nil,
								},
							},
						},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
			isErr:  false,
		},
		"valid watch kind && nil diskconfig.local.blockDeviceSelector": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": map[string]interface{}{
										"blockDeviceSelector": nil,
									},
								},
							},
						},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
			isErr:  false,
		},
		"valid watch kind && nil diskconfig.local.blockDeviceSelector.selectorTerms": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": map[string]interface{}{
										"blockDeviceSelector": map[string]interface{}{
											"selectorTerms": []interface{}(nil),
										},
									},
								},
							},
						},
					},
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
			isErr:  false,
		},
		"valid watch kind && 0 diskconfig.local.blockDeviceSelector.selectorTerms": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
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
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
			isErr:  false,
		},
		"valid watch kind && 1 empty diskconfig.local.blockDeviceSelector.selectorTerms": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
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
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: false,
			isErr:  false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			mock.syncer.skipIfNotLocalDisk()
			if mock.isErr && mock.syncer.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && mock.syncer.err != nil {
				t.Fatalf("Expected no error got [%+v]", mock.syncer.err)
			}
			if mock.isSkip != mock.syncer.response.SkipReconcile {
				t.Fatalf(
					"Expected skip %t got %t",
					mock.isSkip, mock.syncer.response.SkipReconcile,
				)
			}
		})
	}
}

func TestSyncerSkipIfEmptyAttachments(t *testing.T) {
	// it is ok to share this watch across all the tests
	// since this is not manipulated in the test
	var watch = &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind": string(types.KindCStorClusterConfig),
		},
	}
	var tests = map[string]struct {
		syncer *syncer
		isSkip bool
		isErr  bool
	}{
		"nil attachments": {
			syncer: &syncer{
				request:  &generic.SyncHookRequest{},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
		},
		"empty attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Attachments: common.AnyUnstructRegistry{},
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
		},
		"empty map as attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Attachments: common.AnyUnstructRegistry(
						map[string]map[string]*unstructured.Unstructured{},
					),
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
		},
		"nil objs as attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Attachments: common.AnyUnstructRegistry(
						map[string]map[string]*unstructured.Unstructured{
							"gvk1": map[string]*unstructured.Unstructured{},
							"gvk2": map[string]*unstructured.Unstructured{
								"nsname1": nil,
							},
						},
					),
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: true,
		},
		"1 non nil obj as attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Attachments: common.AnyUnstructRegistry(
						map[string]map[string]*unstructured.Unstructured{
							"gvk1": map[string]*unstructured.Unstructured{},
							"gvk2": map[string]*unstructured.Unstructured{
								"nsname1": nil,
							},
							"gvk3": map[string]*unstructured.Unstructured{
								"nsname1": &unstructured.Unstructured{},
							},
						},
					),
				},
				response: &generic.SyncHookResponse{},
			},
			isSkip: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			// set the common watch to avoid panic during logging
			mock.syncer.request.Watch = watch
			// function under test
			mock.syncer.skipIfEmptyAttachments()
			if mock.isErr && mock.syncer.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && mock.syncer.err != nil {
				t.Fatalf("Expected no error got [%+v]", mock.syncer.err)
			}
			if mock.isSkip != mock.syncer.response.SkipReconcile {
				t.Fatalf(
					"Expected skip %t got %t",
					mock.isSkip, mock.syncer.response.SkipReconcile,
				)
			}
		})
	}
}

func TestSyncerRegisterAttachments(t *testing.T) {
	var tests = map[string]struct {
		syncer                 *syncer
		expectBlockDeviceCount int
		expectCStorPoolCluster bool
		expectAttachmentsCount int
		isErr                  bool
	}{
		"0 attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Attachments: common.AnyUnstructRegistry{},
				},
				response: &generic.SyncHookResponse{},
			},
		},
		"1 blockdevice attachment": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Attachments: common.AnyUnstructRegistry(
						map[string]map[string]*unstructured.Unstructured{
							"gvk1": map[string]*unstructured.Unstructured{
								"nsname1": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindBlockDevice),
									},
								},
							},
						},
					),
				},
				response: &generic.SyncHookResponse{},
			},
			expectBlockDeviceCount: 1,
			expectAttachmentsCount: 1,
		},
		"2 blockdevice attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Attachments: common.AnyUnstructRegistry(
						map[string]map[string]*unstructured.Unstructured{
							"gvk1": map[string]*unstructured.Unstructured{
								"nsname1": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindBlockDevice),
										"metadata": map[string]interface{}{
											"name":      "name1",
											"namespace": "ns",
										},
									},
								},
								"nsname2": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindBlockDevice),
										"metadata": map[string]interface{}{
											"name":      "name2",
											"namespace": "ns",
										},
									},
								},
							},
						},
					),
				},
				response: &generic.SyncHookResponse{},
			},
			expectBlockDeviceCount: 2,
			expectAttachmentsCount: 2,
		},
		"2 blockdevice & 1 related CSPC attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"metadata": map[string]interface{}{
								"uid": "123",
							},
						},
					},
					Attachments: common.AnyUnstructRegistry(
						map[string]map[string]*unstructured.Unstructured{
							"gvk1": map[string]*unstructured.Unstructured{
								"nsname1": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindBlockDevice),
									},
								},
								"nsname2": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindBlockDevice),
									},
								},
							},
							"gvk2": map[string]*unstructured.Unstructured{
								"nsname1": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindCStorPoolCluster),
										"metadata": map[string]interface{}{
											"annotations": map[string]interface{}{
												types.AnnKeyCStorClusterConfigUID: "123",
											},
										},
									},
								},
							},
						},
					),
				},
				response: &generic.SyncHookResponse{},
			},
			expectCStorPoolCluster: true,
			expectBlockDeviceCount: 2,
			expectAttachmentsCount: 2,
		},
		"2 blockdevice & 1 un-related CSPC attachments": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"metadata": map[string]interface{}{
								"uid": "1234",
							},
						},
					},
					Attachments: common.AnyUnstructRegistry(
						map[string]map[string]*unstructured.Unstructured{
							"gvk1": map[string]*unstructured.Unstructured{
								"nsname1": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindBlockDevice),
									},
								},
								"nsname2": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindBlockDevice),
									},
								},
							},
							"gvk2": map[string]*unstructured.Unstructured{
								"nsname1": &unstructured.Unstructured{
									Object: map[string]interface{}{
										"kind": string(types.KindCStorPoolCluster),
										"metadata": map[string]interface{}{
											"annotations": map[string]interface{}{
												types.AnnKeyCStorClusterConfigUID: "123",
											},
										},
									},
								},
							},
						},
					),
				},
				response: &generic.SyncHookResponse{},
			},
			expectBlockDeviceCount: 2,
			expectAttachmentsCount: 3,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			mock.syncer.registerAttachments()
			if mock.isErr && mock.syncer.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && mock.syncer.err != nil {
				t.Fatalf("Expected no error got [%+v]", mock.syncer.err)
			}
			if mock.expectBlockDeviceCount != len(mock.syncer.blockDevices) {
				t.Fatalf("Expect block device count %d got %d",
					mock.expectBlockDeviceCount, len(mock.syncer.blockDevices),
				)
			}
			if mock.expectCStorPoolCluster && mock.syncer.cstorPoolCluster == nil {
				t.Fatalf("Expect cspc got none")
			}
			if !mock.expectCStorPoolCluster && mock.syncer.cstorPoolCluster != nil {
				t.Fatalf("Expect no cspc got [%+v]", mock.syncer.cstorPoolCluster)
			}
			if mock.expectAttachmentsCount != len(mock.syncer.response.Attachments) {
				t.Fatalf("Expect attachments count %d got %d",
					mock.expectAttachmentsCount,
					len(mock.syncer.response.Attachments),
				)
			}
		})
	}
}

// TODO
func TestSyncerReconcile(t *testing.T) {
	var tests = map[string]struct {
		syncer                *syncer
		expectAttachmentCount int
		isSkipReconcile       bool
		isErr                 bool
	}{
		"empty syncer": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{},
			},
			isErr: true,
		},
		"non empty watch": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{},
					},
				},
			},
			isErr: true,
		},
		"non empty watch && non empty block devices": {
			syncer: &syncer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{},
					},
				},
				blockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{},
				},
			},
			isErr: true,
		},
		"striped CStorClusterConfig && 1 block device in 1 node && 0 CStorPoolCluster": {
			syncer: &syncer{
				response: &generic.SyncHookResponse{},
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
							},
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": map[string]interface{}{
										"blockDeviceSelector": map[string]interface{}{},
									},
								},
								"poolConfig": map[string]interface{}{
									"raidType": string(types.PoolRAIDTypeStripe),
								},
							},
						},
					},
				},
				blockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
				},
			},
			expectAttachmentCount: 1,
			isSkipReconcile:       false,
			isErr:                 false,
		},
		"mirror CStorClusterConfig && 1 block device in 1 node && 0 CStorPoolCluster": {
			syncer: &syncer{
				response: &generic.SyncHookResponse{},
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
							},
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": map[string]interface{}{
										"blockDeviceSelector": map[string]interface{}{},
									},
								},
								"poolConfig": map[string]interface{}{
									"raidType": string(types.PoolRAIDTypeMirror),
								},
							},
						},
					},
				},
				blockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
				},
			},
			isErr: true,
		},
		"mirror CStorClusterConfig && 2 block devices in 1 node && 0 CStorPoolCluster": {
			syncer: &syncer{
				response: &generic.SyncHookResponse{},
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
							},
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": map[string]interface{}{
										"blockDeviceSelector": map[string]interface{}{},
									},
								},
								"poolConfig": map[string]interface{}{
									"raidType": string(types.PoolRAIDTypeMirror),
								},
							},
						},
					},
				},
				blockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd2",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
				},
			},
			expectAttachmentCount: 1,
			isSkipReconcile:       false,
			isErr:                 false,
		},
		"mirror CStorClusterConfig && 2 block devices each in 2 nodes && 0 CStorPoolCluster": {
			syncer: &syncer{
				response: &generic.SyncHookResponse{},
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
							},
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": map[string]interface{}{
										"blockDeviceSelector": map[string]interface{}{},
									},
								},
								"poolConfig": map[string]interface{}{
									"raidType": string(types.PoolRAIDTypeMirror),
								},
							},
						},
					},
				},
				blockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd11",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd12",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd21",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd22",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
							},
						},
					},
				},
			},
			expectAttachmentCount: 1,
			isSkipReconcile:       false,
			isErr:                 false,
		},
		"raidz CStorClusterConfig && 3 block devices each in 2 nodes && 0 CStorPoolCluster": {
			syncer: &syncer{
				response: &generic.SyncHookResponse{},
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindCStorClusterConfig),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
							},
							"spec": map[string]interface{}{
								"diskConfig": map[string]interface{}{
									"local": map[string]interface{}{
										"blockDeviceSelector": map[string]interface{}{},
									},
								},
								"poolConfig": map[string]interface{}{
									"raidType": string(types.PoolRAIDTypeRAIDZ),
								},
							},
						},
					},
				},
				blockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd11",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd12",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd13",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd21",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd22",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd23",
								"namespace": "storage",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
							},
						},
					},
				},
			},
			expectAttachmentCount: 1,
			isSkipReconcile:       false,
			isErr:                 false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			s := mock.syncer
			s.reconcile()
			if mock.isErr && s.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && s.err != nil {
				t.Fatalf("Expected no error got [%+v]", s.err)
			}
			if mock.isErr {
				return
			}
			if mock.isSkipReconcile != s.response.SkipReconcile {
				t.Fatalf(
					"Expected skip %t got %t",
					mock.isSkipReconcile, s.response.SkipReconcile,
				)
			}
			if mock.expectAttachmentCount != len(s.response.Attachments) {
				t.Fatalf("Expected attachment count %d got %d",
					mock.expectAttachmentCount, len(s.response.Attachments),
				)
			}
		})
	}
}

func TestReconcilerIsObservedBlockDeviceCountMatchRAIDType(t *testing.T) {
	var tests = map[string]struct {
		reconciler *Reconciler
		isErr      bool
	}{
		"no block devices": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig:         nil,
				hostNameToSelectedBlockDeviceNames: map[string][]string{},
			},
			isErr: false,
		},
		"nil cstor cluster config": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: nil,
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1"},
				},
			},
			isErr: true,
		},
		"mirror cstor cluster config && 1 block device": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": string(types.PoolRAIDTypeMirror),
							},
						},
					},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1"},
				},
			},
			isErr: true,
		},
		"mirror cstor cluster config && 2 block devices": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": string(types.PoolRAIDTypeMirror),
							},
						},
					},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1", "bd2"},
				},
			},
			isErr: false,
		},
		"mirror cstor cluster config && 2, 3 block devices": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": string(types.PoolRAIDTypeMirror),
							},
						},
					},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1", "bd2"},
					"node-002": []string{"bd21", "bd22", "bd23"},
				},
			},
			isErr: true,
		},
		"stripe cstor cluster config && 1 block devices": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": string(types.PoolRAIDTypeStripe),
							},
						},
					},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1"},
				},
			},
			isErr: false,
		},
		"stripe cstor cluster config && 2, 1 block devices": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": string(types.PoolRAIDTypeStripe),
							},
						},
					},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1", "bd2"},
					"node-002": []string{"bd21"},
				},
			},
			isErr: false,
		},
		"raidz cstor cluster config && 3, 3 block devices": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": string(types.PoolRAIDTypeRAIDZ),
							},
						},
					},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1", "bd2", "bd3"},
					"node-002": []string{"bd21", "bd22", "bd23"},
				},
			},
			isErr: false,
		},
		"raidz cstor cluster config && 3, 2 block devices": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": string(types.PoolRAIDTypeRAIDZ),
							},
						},
					},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd1", "bd2", "bd3"},
					"node-002": []string{"bd21", "bd22"},
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := mock.reconciler
			r.init()
			r.isSelectedBlockDeviceCountMatchRAIDType()
			if mock.isErr && r.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && r.err != nil {
				t.Fatalf("Expected no error got [%+v]", r.err)
			}
		})
	}
}

func TestReconcilerMapHostNameToSelectedBlockDevices(t *testing.T) {
	var tests = map[string]struct {
		reconciler                   *Reconciler
		expectHostCount              int
		expectHostToBlockDeviceCount map[string]int
		isErr                        bool
	}{
		"nil observed devices": {
			reconciler: &Reconciler{
				selectedBlockDevices: nil,
			},
			isErr: false,
		},
		"0 observed devices": {
			reconciler: &Reconciler{
				selectedBlockDevices: []*unstructured.Unstructured{},
			},
			isErr: false,
		},
		"1 invalid kind observed device": {
			reconciler: &Reconciler{
				selectedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": "Junk",
						},
					},
				},
			},
			isErr: true,
		},
		"1 valid kind observed device && empty hostname": {
			reconciler: &Reconciler{
				selectedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
						},
					},
				},
			},
			isErr: true,
		},
		"1 valid kind observed device && valid hostname": {
			reconciler: &Reconciler{
				selectedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
				},
			},
			expectHostCount: 1,
			expectHostToBlockDeviceCount: map[string]int{
				"node-001": 1,
			},
			isErr: false,
		},
		"2 valid kind observed devices && valid, invalid hostname": {
			reconciler: &Reconciler{
				selectedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "invalid",
								"namespace": "invalid",
								"labels":    map[string]interface{}{},
							},
						},
					},
				},
			},
			isErr: true,
		},
		"2 valid kind observed devices && valid single hostname": {
			reconciler: &Reconciler{
				selectedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "test2",
								"namespace": "test",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
				},
			},
			expectHostCount: 1,
			expectHostToBlockDeviceCount: map[string]int{
				"node-001": 2,
			},
			isErr: false,
		},
		"2 valid kind observed devices && valid hostnames": {
			reconciler: &Reconciler{
				selectedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "test",
								"namespace": "test",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "test2",
								"namespace": "test",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
							},
						},
					},
				},
			},
			expectHostCount: 2,
			expectHostToBlockDeviceCount: map[string]int{
				"node-001": 1,
				"node-002": 1,
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := mock.reconciler
			r.mapHostNameToSelectedBlockDevices()
			if mock.isErr && r.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && r.err != nil {
				t.Fatalf("Expected no error got [%+v]", r.err)
			}
			if mock.expectHostCount != len(r.hostNameToSelectedBlockDeviceNames) {
				t.Fatalf("Expected host to BD count %d got %d",
					mock.expectHostCount, len(r.hostNameToSelectedBlockDeviceNames),
				)
			}
			if mock.isErr {
				return
			}
			for hostname, blockdevices := range r.hostNameToSelectedBlockDeviceNames {
				if mock.expectHostToBlockDeviceCount[hostname] != len(blockdevices) {
					t.Fatalf("Expected blockdevice count %d got %d: host %q",
						mock.expectHostToBlockDeviceCount[hostname],
						len(blockdevices),
						hostname,
					)
				}
			}
		})
	}
}

func TestReconcilerWalkObservedCStorPoolCluster(t *testing.T) {
	var tests = map[string]struct {
		reconciler              *Reconciler
		expectHostNames         []string
		expectHostToDeviceNames map[string][]string
		isErr                   bool
	}{
		"nil cstor pool cluster": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: nil,
			},
		},
		"empty unstruct cstor pool cluster": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: &unstructured.Unstructured{},
			},
			isErr: true,
		},
		"invalid kind cstor pool cluster": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Junk",
					},
				},
			},
			isErr: true,
		},
		"valid kind cstor pool cluster && no pools": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorPoolCluster),
					},
				},
			},
			isErr: true,
		},
		"valid kind cstor pool cluster && nil pools": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorPoolCluster),
						"spec": map[string]interface{}{},
					},
				},
			},
			isErr: true,
		},
		"valid kind cstor pool cluster && empty pools": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorPoolCluster),
						"spec": map[string]interface{}{
							"pools": []interface{}{},
						},
					},
				},
			},
			isErr: true,
		},
		"valid kind cstor pool cluster && 1 pool": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorPoolCluster),
						"spec": map[string]interface{}{
							"pools": []interface{}{
								map[string]interface{}{
									"poolConfig": map[string]interface{}{
										"defaultRaidGroupType": "stripe",
										"overProvisioning":     false,
										"compression":          "off",
									},
									"nodeSelector": map[string]interface{}{
										"kubernetes.io/hostname": "node-201",
									},
									"raidGroups": []interface{}{
										map[string]interface{}{
											"blockDevices": []interface{}{
												map[string]interface{}{
													"blockDeviceName": "bd-7",
												},
											},
											"type":         "stripe",
											"isWriteCache": false,
											"isSpare":      false,
											"isReadCache":  false,
										},
									},
								},
							},
						},
					},
				},
			},
			expectHostNames: []string{"node-201"},
			expectHostToDeviceNames: map[string][]string{
				"node-201": []string{"bd-7"},
			},
			isErr: false,
		},
		"valid kind cstor pool cluster && 2 pools": {
			reconciler: &Reconciler{
				ObservedCStorPoolCluster: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorPoolCluster),
						"spec": map[string]interface{}{
							"pools": []interface{}{
								map[string]interface{}{
									"poolConfig": map[string]interface{}{
										"defaultRaidGroupType": "stripe",
										"overProvisioning":     false,
										"compression":          "off",
									},
									"nodeSelector": map[string]interface{}{
										"kubernetes.io/hostname": "node-101",
									},
									"raidGroups": []interface{}{
										map[string]interface{}{
											"blockDevices": []interface{}{
												map[string]interface{}{
													"blockDeviceName": "bd-8",
												},
											},
											"type":         "stripe",
											"isWriteCache": false,
											"isSpare":      false,
											"isReadCache":  false,
										},
									},
								},
								map[string]interface{}{
									"poolConfig": map[string]interface{}{
										"defaultRaidGroupType": "stripe",
										"overProvisioning":     false,
										"compression":          "off",
									},
									"nodeSelector": map[string]interface{}{
										"kubernetes.io/hostname": "node-201",
									},
									"raidGroups": []interface{}{
										map[string]interface{}{
											"blockDevices": []interface{}{
												map[string]interface{}{
													"blockDeviceName": "bd-7",
												},
												map[string]interface{}{
													"blockDeviceName": "bd-77",
												},
											},
											"type":         "stripe",
											"isWriteCache": false,
											"isSpare":      false,
											"isReadCache":  false,
										},
									},
								},
							},
						},
					},
				},
			},
			expectHostNames: []string{"node-101", "node-201"},
			expectHostToDeviceNames: map[string][]string{
				"node-101": []string{"bd-8"},
				"node-201": []string{"bd-7", "bd-77"},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := mock.reconciler
			r.walkObservedCStorPoolCluster()
			if mock.isErr && r.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && r.err != nil {
				t.Fatalf("Expected no error got [%+v]", r.err)
			}
			if mock.isErr {
				return
			}
			if !reflect.DeepEqual(mock.expectHostNames, r.observedHostNamesInCSPC) {
				t.Fatalf(
					"Expected no diff got %s",
					cmp.Diff(mock.expectHostNames, r.observedHostNamesInCSPC),
				)
			}
			if r.hostNameToObservedCSPCDeviceNames == nil {
				r.hostNameToObservedCSPCDeviceNames = map[string][]string{}
			}
			for host, expectdevices := range mock.expectHostToDeviceNames {
				actualdevices := r.hostNameToObservedCSPCDeviceNames[host]
				if !reflect.DeepEqual(expectdevices, actualdevices) {
					t.Fatalf("Expected no diff for host %q got %s",
						host, cmp.Diff(expectdevices, actualdevices),
					)
				}
			}
		})
	}
}

func TestReconcilerBuildDesiredCStorPoolCluster(t *testing.T) {
	var tests = map[string]struct {
		reconciler *Reconciler
		expectCSPC *unstructured.Unstructured
		isErr      bool
	}{
		"mirror cspc": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
							"uid":       "ccc-101",
						},
					},
				},
				observedHostNamesInCSPC: []string{"node-001", "node-002"},
				hostNameToObservedCSPCDeviceNames: map[string][]string{
					"node-001": []string{"bd10"},
					"node-002": []string{"bd20"},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd10", "bd11"},
					"node-002": []string{"bd20", "bd21"},
				},
				raidType: types.PoolRAIDTypeMirror,
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": types.APIVersionOpenEBSV1Alpha1,
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterConfigUID): "ccc-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd10",
											},
											map[string]interface{}{
												"blockDeviceName": "bd11",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd20",
											},
											map[string]interface{}{
												"blockDeviceName": "bd21",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
						},
					},
				},
			},
		},
		"stripe cspc": {
			reconciler: &Reconciler{
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
							"uid":       "ccc-101",
						},
					},
				},
				observedHostNamesInCSPC: []string{"node-001", "node-002"},
				hostNameToObservedCSPCDeviceNames: map[string][]string{
					"node-001": []string{"bd10"},
					"node-002": []string{"bd20"},
				},
				hostNameToSelectedBlockDeviceNames: map[string][]string{
					"node-001": []string{"bd10", "bd11"},
					"node-002": []string{"bd20", "bd21"},
				},
				raidType: types.PoolRAIDTypeStripe,
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": types.APIVersionOpenEBSV1Alpha1,
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterConfigUID): "ccc-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "stripe",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd10",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd11",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "stripe",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd20",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd21",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
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
			r := mock.reconciler
			r.buildDesiredCStorPoolCluster()
			if mock.isErr && r.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && r.err != nil {
				t.Fatalf("Expected no error got [%+v]", r.err)
			}
			if mock.isErr {
				return
			}
			if !reflect.DeepEqual(mock.expectCSPC, r.desiredCStorPoolCluster) {
				t.Fatalf(
					"Expected no diff got\n%s",
					cmp.Diff(mock.expectCSPC, r.desiredCStorPoolCluster),
				)
			}
		})
	}
}

func TestReconcilerReconcile(t *testing.T) {
	var tests = map[string]struct {
		reconciler *Reconciler
		expectCSPC *unstructured.Unstructured
		isErr      bool
	}{
		"nil ObservedBlockDevices": {
			reconciler: &Reconciler{
				ObservedBlockDevices: nil,
			},
			isErr: true,
		},
		"nil ObservedCStorClusterConfig": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{},
					},
				},
				ObservedCStorClusterConfig: nil,
			},
			isErr: true,
		},
		"invalid ObservedCStorClusterConfig": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{},
					},
				},
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Junk",
					},
				},
			},
			isErr: true,
		},
		"expect mirror cspc when observed cspc is nil": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name": "bd1",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name": "bd2",
								"labels": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
							},
						},
					},
				},
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
							"uid":       "ccc-101",
						},
						"spec": map[string]interface{}{
							"poolConfig": map[string]interface{}{
								"raidType": "mirror",
							},
							"diskConfig": map[string]interface{}{
								"local": map[string]interface{}{
									"blockDeviceSelector": map[string]interface{}{
										"selectorTerms": []interface{}(nil),
									},
								},
							},
						},
					},
				},
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": types.APIVersionOpenEBSV1Alpha1,
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterConfigUID): "ccc-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd2",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
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
			r := mock.reconciler
			resp, err := r.Reconcile()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if !reflect.DeepEqual(mock.expectCSPC, resp.CStorPoolCluster) {
				t.Fatalf("Expected no diff got\n%s",
					cmp.Diff(mock.expectCSPC, resp.CStorPoolCluster),
				)
			}
		})
	}
}

func TestReconcilerSelectFromObservedBlockDevices(t *testing.T) {
	var tests = map[string]struct {
		reconciler         *Reconciler
		expectBlockDevices []*unstructured.Unstructured
		isErr              bool
	}{
		"nil observed block devices": {
			reconciler: &Reconciler{},
			isErr:      true,
		},
		"empty observed block devices": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{},
			},
			isErr: true,
		},
		"no blockdevice selector": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "openebs",
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdc",
							},
						},
					},
				},
			},
			isErr: true,
		},
		"empty blockdevice selector terms": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "openebs",
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdc",
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd2",
								"namespace": "openebs",
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdb",
							},
						},
					},
				},
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
						"spec": map[string]interface{}{
							"diskConfig": map[string]interface{}{
								"local": map[string]interface{}{
									"blockDeviceSelector": map[string]interface{}{
										"selectorTerms": []interface{}(nil),
									},
								},
							},
						},
					},
				},
			},
			expectBlockDevices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
						"metadata": map[string]interface{}{
							"name":      "bd1",
							"namespace": "openebs",
						},
						"spec": map[string]interface{}{
							"path": "/dev/sdc",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
						"metadata": map[string]interface{}{
							"name":      "bd2",
							"namespace": "openebs",
						},
						"spec": map[string]interface{}{
							"path": "/dev/sdb",
						},
					},
				},
			},
			isErr: false,
		},
		"passing path based blockdevice selector term": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "openebs",
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdc",
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd2",
								"namespace": "openebs",
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdb",
							},
						},
					},
				},
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
						"spec": map[string]interface{}{
							"diskConfig": map[string]interface{}{
								"local": map[string]interface{}{
									"blockDeviceSelector": map[string]interface{}{
										"selectorTerms": []interface{}{
											map[string]interface{}{
												"matchFields": map[string]interface{}{
													"spec.path": "/dev/sdb",
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
			expectBlockDevices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
						"metadata": map[string]interface{}{
							"name":      "bd2",
							"namespace": "openebs",
						},
						"spec": map[string]interface{}{
							"path": "/dev/sdb",
						},
					},
				},
			},
			isErr: true, // bug in metac path based field selector
		},
		"failing path based blockdevice selector term": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "openebs",
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdc",
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd2",
								"namespace": "openebs",
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdb",
							},
						},
					},
				},
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
						"spec": map[string]interface{}{
							"diskConfig": map[string]interface{}{
								"local": map[string]interface{}{
									"blockDeviceSelector": map[string]interface{}{
										"selectorTerms": []interface{}{
											map[string]interface{}{
												"matchFields": map[string]interface{}{
													"spec.path": "/dev/sdd",
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
			isErr: true,
		},
		"passing label based blockdevice selector term": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "openebs",
								"labels": map[string]interface{}{
									"app": "ndm-1",
								},
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdc",
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd2",
								"namespace": "openebs",
								"labels": map[string]interface{}{
									"app": "ndm-2",
								},
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdb",
							},
						},
					},
				},
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
						"spec": map[string]interface{}{
							"diskConfig": map[string]interface{}{
								"local": map[string]interface{}{
									"blockDeviceSelector": map[string]interface{}{
										"selectorTerms": []interface{}{
											map[string]interface{}{
												"matchFields": map[string]interface{}{
													"metadata.labels.app": "ndm-1",
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
			expectBlockDevices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
						"metadata": map[string]interface{}{
							"name":      "bd1",
							"namespace": "openebs",
							"labels": map[string]interface{}{
								"app": "ndm-1",
							},
						},
						"spec": map[string]interface{}{
							"path": "/dev/sdc",
						},
					},
				},
			},
			isErr: false,
		},
		"failing label based blockdevice selector term": {
			reconciler: &Reconciler{
				ObservedBlockDevices: []*unstructured.Unstructured{
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd1",
								"namespace": "openebs",
								"labels": map[string]interface{}{
									"app": "ndm-1",
								},
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdc",
							},
						},
					},
					&unstructured.Unstructured{
						Object: map[string]interface{}{
							"kind": string(types.KindBlockDevice),
							"metadata": map[string]interface{}{
								"name":      "bd2",
								"namespace": "openebs",
								"labels": map[string]interface{}{
									"app": "ndm-2",
								},
							},
							"spec": map[string]interface{}{
								"path": "/dev/sdb",
							},
						},
					},
				},
				ObservedCStorClusterConfig: &unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindCStorClusterConfig),
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
						"spec": map[string]interface{}{
							"diskConfig": map[string]interface{}{
								"local": map[string]interface{}{
									"blockDeviceSelector": map[string]interface{}{
										"selectorTerms": []interface{}{
											map[string]interface{}{
												"matchFields": map[string]interface{}{
													"metadata.labels.app": "ndm-33",
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
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			r := mock.reconciler
			r.init()
			r.selectFromObservedBlockDevices()
			if mock.isErr && r.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && r.err != nil {
				t.Fatalf("Expected no error got [%+v]", r.err)
			}
			if mock.isErr {
				return
			}
			if len(r.selectedBlockDevices) != len(mock.expectBlockDevices) {
				t.Fatalf("Expected block device count %d got %d",
					len(mock.expectBlockDevices), len(r.selectedBlockDevices),
				)
			}
			l := unstruct.List(r.selectedBlockDevices)
			if !l.ContainsAll(mock.expectBlockDevices) {
				t.Fatalf("Expected no diff got \n%s",
					cmp.Diff(r.selectedBlockDevices, mock.expectBlockDevices),
				)
			}
		})
	}
}
