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

package k8s

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestVerifyIsKind(t *testing.T) {
	var tests = map[string]struct {
		obj     *unstructured.Unstructured
		kind    string
		isFound bool
		isErr   bool
	}{
		"matching kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "MyResource",
				},
			},
			kind:    "MyResource",
			isFound: true,
		},
		"invalid obj kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kinds": "MyResource",
				},
			},
			kind:    "MyResource",
			isFound: false,
		},
		"missing given kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "MyResource",
				},
			},
			kind:    "",
			isFound: false,
		},
		"no matching kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "MyResource",
				},
			},
			kind:    "My",
			isFound: false,
		},
		"nil obj": {
			obj:     nil,
			kind:    "My",
			isFound: false,
			isErr:   true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got, err := Verify(mock.obj).IsOfKind(mock.kind).Check()
			if mock.isErr && err == nil {
				t.Fatalf("Want error got nil")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got %v", err)
			}
			if mock.isFound != got {
				t.Fatalf("Expected %t got %t", mock.isFound, got)
			}
		})
	}
}

func TestVerifyIsAPIVersion(t *testing.T) {
	var tests = map[string]struct {
		obj        *unstructured.Unstructured
		apiVersion string
		isFound    bool
		isErr      bool
	}{
		"matching api version": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "dao.mayadata.io/v1",
				},
			},
			apiVersion: "dao.mayadata.io/v1",
			isFound:    true,
		},
		"invalid obj api version": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersions": "dao.mayadata.io/v1",
				},
			},
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"missing given api version": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "dao.mayadata.io/v1",
				},
			},
			apiVersion: "",
			isFound:    false,
		},
		"no matching api version": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "dao.mayadata.io/v1",
				},
			},
			apiVersion: "dao.mayadata.io",
			isFound:    false,
		},
		"nil obj": {
			obj:        nil,
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
			isErr:      true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got, err := Verify(mock.obj).IsOfAPIVersion(mock.apiVersion).Check()
			if mock.isErr && err == nil {
				t.Fatalf("Want error got nil")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got %v", err)
			}
			if mock.isFound != got {
				t.Fatalf("Expected %t got %t", mock.isFound, got)
			}
		})
	}
}

func TestVerifyHasLabel(t *testing.T) {
	var tests = map[string]struct {
		obj     *unstructured.Unstructured
		lblKey  string
		lblVal  string
		isFound bool
		isErr   bool
	}{
		"matching label": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
						},
					},
				},
			},
			lblKey:  "name",
			lblVal:  "dao",
			isFound: true,
		},
		"invalid obj labels type - map[string]string": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]string{
							"name": "dao",
						},
					},
				},
			},
			lblKey:  "name",
			lblVal:  "dao",
			isFound: false,
			isErr:   true,
		},
		"missing given label": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
						},
					},
				},
			},
			lblKey:  "",
			isFound: false,
		},
		"no matching label": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
						},
					},
				},
			},
			lblKey:  "name",
			lblVal:  "junk",
			isFound: false,
		},
		"nil obj": {
			obj:     nil,
			lblKey:  "name",
			isFound: false,
			isErr:   true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got, err :=
				Verify(mock.obj).HasLabel(mock.lblKey, mock.lblVal).Check()
			if mock.isErr && err == nil {
				t.Fatalf("Want error got nil")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got %v", err)
			}
			if mock.isFound != got {
				t.Fatalf("Expected %t got %t", mock.isFound, got)
			}
		})
	}
}

func TestVerifyHasAnn(t *testing.T) {
	var tests = map[string]struct {
		obj     *unstructured.Unstructured
		annKey  string
		annVal  string
		isFound bool
		isErr   bool
	}{
		"matching annotations": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"name": "dao",
						},
					},
				},
			},
			annKey:  "name",
			annVal:  "dao",
			isFound: true,
		},
		"invalid obj annotations type - map[string]string": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]string{
							"name": "dao",
						},
					},
				},
			},
			annKey:  "name",
			annVal:  "dao",
			isFound: false,
			isErr:   true,
		},
		"missing given annotations": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"name": "dao",
						},
					},
				},
			},
			annKey:  "",
			isFound: false,
		},
		"no matching annotations": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"name": "dao",
						},
					},
				},
			},
			annKey:  "name",
			annVal:  "junk",
			isFound: false,
		},
		"nil obj": {
			obj:     nil,
			annKey:  "name",
			isFound: false,
			isErr:   true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got, err :=
				Verify(mock.obj).HasAnn(mock.annKey, mock.annVal).Check()
			if mock.isErr && err == nil {
				t.Fatalf("Want error got nil")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got %v", err)
			}
			if mock.isFound != got {
				t.Fatalf("Expected %t got %t", mock.isFound, got)
			}
		})
	}
}

func TestVerifyHasAnns(t *testing.T) {
	var tests = map[string]struct {
		obj     *unstructured.Unstructured
		anns    map[string]string
		isFound bool
		isErr   bool
	}{
		"matching annotations": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			isFound: true,
		},
		"missing given annotations": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
						},
					},
				},
			},
			anns:    map[string]string{},
			isFound: false,
		},
		"no matching annotations": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"app": "cstor",
						},
					},
				},
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			isFound: false,
		},
		"nil obj": {
			obj:     nil,
			anns:    map[string]string{},
			isFound: false,
			isErr:   true,
		},
		"nil given": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
						},
					},
				},
			},
			anns:    nil,
			isFound: false,
			isErr:   false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got, err :=
				Verify(mock.obj).HasAnns(mock.anns).Check()
			if mock.isErr && err == nil {
				t.Fatalf("Want error got nil")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got %v", err)
			}
			if mock.isFound != got {
				t.Fatalf("Expected %t got %t", mock.isFound, got)
			}
		})
	}
}

func TestVerifyHasLabels(t *testing.T) {
	var tests = map[string]struct {
		obj     *unstructured.Unstructured
		lbls    map[string]string
		isFound bool
		isErr   bool
	}{
		"matching labels": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			isFound: true,
		},
		"missing given labels": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
						},
					},
				},
			},
			lbls:    map[string]string{},
			isFound: false,
		},
		"no matching labels": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "cstor",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			isFound: false,
		},
		"nil obj": {
			obj:     nil,
			lbls:    map[string]string{},
			isFound: false,
			isErr:   true,
		},
		"nil given": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
						},
					},
				},
			},
			lbls:    nil,
			isFound: false,
			isErr:   false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got, err :=
				Verify(mock.obj).HasLabels(mock.lbls).Check()
			if mock.isErr && err == nil {
				t.Fatalf("Want error got nil")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got %v", err)
			}
			if mock.isFound != got {
				t.Fatalf("Expected %t got %t", mock.isFound, got)
			}
		})
	}
}

func TestVerifyEverything(t *testing.T) {
	var tests = map[string]struct {
		obj        *unstructured.Unstructured
		kind       string
		apiVersion string
		annKey     string
		annVal     string
		lblKey     string
		lblVal     string
		lbls       map[string]string
		anns       map[string]string
		isFound    bool
		isErr      bool
	}{
		"matching all": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    true,
		},
		"missing given labels": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"missing given anns": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns:       map[string]string{},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"no matching labels": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "junk",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"no matching anns": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "junk",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"nil obj": {
			obj: nil,
			lbls: map[string]string{
				"name": "dao",
				"app":  "junk",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
			isErr:      true,
		},
		"nil given labels": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: nil,
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"nil given anns": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns:       nil,
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"missing kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"missing label": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"missing ann": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "dao.mayadata.io/v1",
			isFound:    false,
		},
		"missing api version": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "MyResource",
					"apiVersion": "dao.mayadata.io/v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
						"annotations": map[string]interface{}{
							"name": "dao",
							"app":  "cstor",
							"zone": "east-2",
						},
					},
				},
			},
			lbls: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			anns: map[string]string{
				"name": "dao",
				"app":  "cstor",
			},
			lblKey:     "app",
			lblVal:     "cstor",
			annKey:     "name",
			annVal:     "dao",
			kind:       "MyResource",
			apiVersion: "",
			isFound:    false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got, err :=
				Verify(mock.obj).
					IsOfKind(mock.kind).
					IsOfAPIVersion(mock.apiVersion).
					HasLabels(mock.lbls).
					HasAnns(mock.anns).
					HasLabel(mock.lblKey, mock.lblVal).
					HasAnn(mock.annKey, mock.annVal).
					Check()
			if mock.isErr && err == nil {
				t.Fatalf("Want error got nil")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got %v", err)
			}
			if mock.isFound != got {
				t.Fatalf("Expected %t got %t", mock.isFound, got)
			}
		})
	}
}
