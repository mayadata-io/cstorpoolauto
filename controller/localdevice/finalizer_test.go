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
	"testing"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	"openebs.io/metac/controller/common"
	"openebs.io/metac/controller/generic"
)

func TestValidateArgs(t *testing.T) {
	var tests = map[string]struct {
		request  *generic.SyncHookRequest
		response *generic.SyncHookResponse
		isErr    bool
	}{
		"nil everything": {
			isErr: true,
		},
		"not nil request & nil watch & nil response": {
			request: &generic.SyncHookRequest{},
			isErr:   true,
		},
		"not nil request & not nil watch & nil response": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			isErr: true,
		},
		"not nil everything": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			response: &generic.SyncHookResponse{},
			isErr:    false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			f := &finalizer{
				request:  mock.request,
				response: mock.response,
			}
			f.validateArgs()
			if mock.isErr && f.fatal == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && f.fatal != nil {
				t.Fatalf("Expected no error got [%+v]", f.fatal)
			}
		})
	}
}

func TestFinalizerSkipIfNotLocalDisk(t *testing.T) {
	var tests = map[string]struct {
		finalizer *finalizer
		isSkip    bool
		isErr     bool
	}{
		"empty watch": {
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			finalizer: &finalizer{
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
			mock.finalizer.skipIfNotLocalDisk()
			if mock.isErr && mock.finalizer.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && mock.finalizer.err != nil {
				t.Fatalf("Expected no error got [%+v]", mock.finalizer.err)
			}
			if mock.isSkip != mock.finalizer.response.SkipReconcile {
				t.Fatalf(
					"Expected skip %t got %t",
					mock.isSkip, mock.finalizer.response.SkipReconcile,
				)
			}
		})
	}
}

func TestMarkFinalizedIfEmptyAttachments(t *testing.T) {
	var tests = map[string]struct {
		request     *generic.SyncHookRequest
		isFinalized bool
	}{
		"nil attachments": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			isFinalized: true,
		},
		"no attachment registry": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
				Attachments: common.AnyUnstructRegistry{},
			},
			isFinalized: true,
		},
		"nil attachment registry": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
				Attachments: common.AnyUnstructRegistry(
					map[string]map[string]*unstructured.Unstructured{},
				),
			},
			isFinalized: true,
		},
		"one nil attachment in registry": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
				Attachments: common.AnyUnstructRegistry(
					map[string]map[string]*unstructured.Unstructured{
						"apivers/kind": map[string]*unstructured.Unstructured{
							"ns/name": nil,
						},
					},
				),
			},
			isFinalized: true,
		},
		"one not nil attachment in registry": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
				Attachments: common.AnyUnstructRegistry(
					map[string]map[string]*unstructured.Unstructured{
						"apivers/kind": map[string]*unstructured.Unstructured{
							"ns/name": &unstructured.Unstructured{
								Object: map[string]interface{}{},
							},
						},
					},
				),
			},
			isFinalized: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			f := &finalizer{
				request:  mock.request,
				response: &generic.SyncHookResponse{},
			}
			f.markFinalizedIfNoAttachments()
			if mock.isFinalized != f.response.Finalized {
				t.Fatalf(
					"Expected finalized %t got %t",
					mock.isFinalized,
					f.response.Finalized,
				)
			}
		})
	}
}

func TestHandleError(t *testing.T) {
	var tests = map[string]struct {
		err             error
		isSkipReconcile bool
	}{
		"nil error": {
			err:             nil,
			isSkipReconcile: false,
		},
		"not nil error": {
			err:             errors.Errorf("Error"),
			isSkipReconcile: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			f := &finalizer{
				request: &generic.SyncHookRequest{
					Watch: &unstructured.Unstructured{
						Object: map[string]interface{}{},
					},
				},
				response: &generic.SyncHookResponse{},
				err:      mock.err,
			}
			f.handleError()
			if mock.isSkipReconcile != f.response.SkipReconcile {
				t.Fatalf(
					"Expected skip reconcile %t got %t",
					mock.isSkipReconcile,
					f.response.SkipReconcile,
				)
			}
		})
	}
}

func TestFinalize(t *testing.T) {
	var tests = map[string]struct {
		request         *generic.SyncHookRequest
		response        *generic.SyncHookResponse
		isErr           bool
		isSkipReconcile bool
		isFinalized     bool
	}{
		"invalid CStorClusterConfig": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			response:        &generic.SyncHookResponse{},
			isErr:           true,
			isSkipReconcile: true,
		},
		"CStorClusterConfig without local device config": {
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
			response:        &generic.SyncHookResponse{},
			isSkipReconcile: true,
		},
		"CStorClusterConfig with local device config & selector terms": {
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
			response:    &generic.SyncHookResponse{},
			isFinalized: true,
		},
		"CStorClusterConfig + local device config & selector terms + attachment": {
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
				Attachments: common.MakeAnyUnstructRegistry(
					[]*unstructured.Unstructured{
						&unstructured.Unstructured{
							Object: map[string]interface{}{
								"kind":       "CStorPoolCluster",
								"apiVersion": "openebs.io/v1alpha1",
								"metadata": map[string]interface{}{
									"name": "test",
								},
							},
						},
					},
				),
			},
			response:    &generic.SyncHookResponse{},
			isFinalized: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			f := &finalizer{
				request:  mock.request,
				response: mock.response,
			}
			f.finalize()
			if mock.isErr && f.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && f.err != nil {
				t.Fatalf("Expected no error got [%+v]", f.err)
			}
			if mock.isSkipReconcile != f.response.SkipReconcile {
				t.Fatalf(
					"Expected skip reconcile %t got %t",
					mock.isSkipReconcile,
					f.response.SkipReconcile,
				)
			}
			if mock.isFinalized != f.response.Finalized {
				t.Fatalf(
					"Expected finalized %t got %t",
					mock.isFinalized,
					f.response.Finalized,
				)
			}
		})
	}
}
