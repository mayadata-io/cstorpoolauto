/*
Copyright 2020 The MayaData Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package unstruct

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestSelectUnstructAPIVersionANDLabels(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		obj        *unstructured.Unstructured
		apiVersion string
		labels     map[string]string
		isFound    bool
		isErr      bool
	}{
		"apiversion && labels match": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v2",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "cstor",
						},
					},
				},
			},
			apiVersion: "v2",
			labels: map[string]string{
				"app": "cstor",
			},
			isFound: true,
			isErr:   false,
		},
		"apiversion && labels mismatch": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v2",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "cstor",
						},
					},
				},
			},
			apiVersion: "v22",
			labels: map[string]string{
				"app": "cstors",
			},
			isFound: false,
			isErr:   false,
		},
		"apiversion match && labels mismatch": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v2",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "cstor",
						},
					},
				},
			},
			apiVersion: "v2",
			labels: map[string]string{
				"app": "cstors",
			},
			isFound: false,
			isErr:   false,
		},
		"apiversion mismatch && labels match": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v22",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "cstor",
						},
					},
				},
			},
			apiVersion: "v2",
			labels: map[string]string{
				"app": "cstor",
			},
			isFound: false,
			isErr:   false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			conditionName := "apiversion-&-labels"
			// initialize & run
			ul := AsListing(mock.obj)
			eval, err := ul.WithCondition(
				conditionName,
				NewLazyCondition().
					IsAPIVersion(mock.apiVersion).
					HasLabels(mock.labels),
			).EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				got, found, err := eval.ListObjsForCondition(conditionName)
				if mock.isErr && err == nil {
					t.Fatalf("Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.isFound && got == nil {
					t.Fatalf("Expected non nil obj got nil")
				}
				if mock.isFound != found {
					t.Fatalf("Expected found = %t got = %t", mock.isFound, found)
				}
			}
		})
	}
}

func TestSelectUnstructAPIVersionORKind(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		obj        *unstructured.Unstructured
		kind       string
		apiVersion string
		isFound    bool
		isErr      bool
	}{
		"kind match && apiVersion mismatch": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
				},
			},
			kind:       "Pod",
			apiVersion: "v2",
			isFound:    true,
			isErr:      false,
		},
		"kind mismatch && apiVersion match": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v3",
				},
			},
			kind:       "Service",
			apiVersion: "v3",
			isFound:    true,
			isErr:      false,
		},
		"nil as obj": {
			obj:     nil,
			isFound: false,
			isErr:   true,
		},
		"kind & apiVersion mismatch": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1beta1",
				},
			},
			kind:       "Service",
			apiVersion: "v1",
			isFound:    false,
			isErr:      false,
		},
		"kind & apiVersion match": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Service",
					"apiVersion": "v1",
				},
			},
			kind:       "Service",
			apiVersion: "v1",
			isFound:    true,
			isErr:      false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			conditionName := "kind-or-apiversion"
			// initialize & run
			ul := AsListing(mock.obj)
			eval, err := ul.WithCondition(
				conditionName,
				NewLazyORCondition().
					IsKind(mock.kind).
					IsAPIVersion(mock.apiVersion),
			).EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				got, found, err := eval.ListObjsForCondition(conditionName)
				if mock.isErr && err == nil {
					t.Fatalf("Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.isFound && got == nil {
					t.Fatalf("Expected non nil obj got nil")
				}
				if mock.isFound != found {
					t.Fatalf("Expected found = %t got = %t", mock.isFound, found)
				}
			}
		})
	}
}

func TestSelectUnstructKindCondition(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		obj     *unstructured.Unstructured
		kind    string
		isFound bool
		isErr   bool
	}{
		"single matching kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
				},
			},
			kind:    "Pod",
			isFound: true,
			isErr:   false,
		},
		"no matching kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
				},
			},
			kind:    "Service",
			isFound: false,
			isErr:   false,
		},
		"nil as obj": {
			obj:     nil,
			kind:    "Service",
			isFound: false,
			isErr:   true,
		},
		"missing expected kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
				},
			},
			kind:    "",
			isFound: false,
			isErr:   true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			conditionName := "my-selection-kind"
			// initialize & run
			ul := AsListing(mock.obj)
			eval, err := ul.WithCondition(
				conditionName,
				NewLazyCondition().IsKind(mock.kind),
			).EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				got, found, err := eval.ListObjsForCondition(conditionName)
				if mock.isErr && err == nil {
					t.Fatalf("Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.isFound && got == nil {
					t.Fatalf("Expected non nil obj got nil")
				}
				if mock.isFound != found {
					t.Fatalf("Expected found = %t got = %t", mock.isFound, found)
				}
			}
		})
	}
}

func TestSelectRunIsKind(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		objs      []*unstructured.Unstructured
		kind      string
		passCount int
		isFound   bool
		isErr     bool
	}{
		"single matching kind": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Service",
					},
				},
			},
			kind:      "Pod",
			passCount: 1,
			isFound:   true,
			isErr:     false,
		},
		"two matching kinds": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
			},
			kind:      "Pod",
			passCount: 2,
			isFound:   true,
			isErr:     false,
		},
		"no matching kind": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
			},
			kind:      "Service",
			passCount: 0,
			isFound:   false,
			isErr:     false,
		},
		"nil as first obj": {
			objs: []*unstructured.Unstructured{
				nil,
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
			},
			kind:      "Service",
			passCount: 0,
			isFound:   false,
			isErr:     true,
		},
		"nil as second object": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
				nil,
			},
			kind:      "Service",
			passCount: 0,
			isFound:   false,
			isErr:     true,
		},
		"missing expected kind": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
					},
				},
			},
			kind:      "",
			passCount: 0,
			isFound:   false,
			isErr:     true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			conditionName := "my-selection-kind"
			// initialize & run
			ul := NewListing(mock.objs)
			eval, err :=
				ul.WithCondition(
					conditionName, NewLazyCondition().IsKind(mock.kind),
				).EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				got, found, err := eval.ListObjsForCondition(conditionName)
				if mock.isErr && err == nil {
					t.Fatalf("Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.passCount != len(got) {
					t.Fatalf("Expected pass count %d got %d", mock.passCount, len(got))
				}
				if mock.isFound != found {
					t.Fatalf("Expected eval result %t got %t", mock.isFound, found)
				}
			}
		})
	}
}

func TestSelectRunIsKindAndLbl(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		objs      []*unstructured.Unstructured
		kind      string
		lblKey    string
		lblValue  string
		passCount int
		failCount int
		isFound   bool
		isErr     bool
	}{
		"multiple matches & rejects": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "a",
							"labels": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Service",
						"metadata": map[string]interface{}{
							"name": "b",
							"labels": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "c",
							"labels": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "d",
							"labels": map[string]interface{}{
								"app": "cstors",
							},
						},
					},
				},
			},
			kind:      "Pod",
			lblKey:    "app",
			lblValue:  "cstor",
			passCount: 2,
			failCount: 2,
			isFound:   true,
			isErr:     false,
		},
		"no matches": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pods",
						"metadata": map[string]interface{}{
							"name": "a",
							"labels": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Service",
						"metadata": map[string]interface{}{
							"name": "b",
							"labels": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "c",
							"labels": map[string]interface{}{
								"appa": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "d",
							"labels": map[string]interface{}{
								"app": "cstors",
							},
						},
					},
				},
			},
			kind:      "Pod",
			lblKey:    "app",
			lblValue:  "cstor",
			passCount: 0,
			failCount: 4,
			isFound:   false,
			isErr:     false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			conditionName := "my-selection"
			// initialize & run
			ul := NewListing(mock.objs)
			eval, err := ul.WithCondition(
				conditionName,
				NewLazyCondition().
					IsKind(mock.kind).
					HasLabel(mock.lblKey, mock.lblValue),
			).EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				got, found, err := eval.ListObjsForCondition(conditionName)
				if mock.isErr && err == nil {
					t.Fatalf("Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.passCount != len(got) {
					t.Fatalf("Expected pass count %d got %d", mock.passCount, len(got))
				}
				if !mock.isErr && mock.isFound != found {
					t.Fatalf("Expected found %t got %t", mock.isFound, found)
				}
				if !mock.isErr && mock.failCount != len(eval.ListAllConditionRejects()) {
					failMsgs, _ := eval.ListAllConditionFailures()
					t.Fatalf(
						"Expected rejects %d got %d: Rejects [%#v]",
						mock.failCount,
						len(eval.ListAllConditionRejects()),
						failMsgs,
					)
				}
			}
		})
	}
}

func TestSelectRunIsAPIVersionAndAnn(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		objs       []*unstructured.Unstructured
		apiVersion string
		annKey     string
		annValue   string
		passCount  int
		failCount  int
		isFound    bool
		isErr      bool
	}{
		"multiple matches & rejects": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "a",
							"annotations": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v2",
						"metadata": map[string]interface{}{
							"name": "b",
							"annotations": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "c",
							"annotations": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "d",
							"annotations": map[string]interface{}{
								"app": "cstors",
							},
						},
					},
				},
			},
			apiVersion: "v1",
			annKey:     "app",
			annValue:   "cstor",
			passCount:  2,
			failCount:  2,
			isFound:    true,
			isErr:      false,
		},
		"no matches": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v3",
						"metadata": map[string]interface{}{
							"name": "a",
							"annotations": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "b",
							"annotations": map[string]interface{}{
								"appa": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"metadata": map[string]interface{}{
							"name": "c",
							"annotations": map[string]interface{}{
								"app": "cstors",
							},
						},
					},
				},
			},
			apiVersion: "v1",
			annKey:     "app",
			annValue:   "cstor",
			passCount:  0,
			failCount:  3,
			isFound:    false,
			isErr:      false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			conditionName := "my-selection"
			// initialize & run
			ul := NewListing(mock.objs)
			eval, err := ul.WithCondition(
				conditionName,
				NewLazyCondition().
					IsAPIVersion(mock.apiVersion).
					HasAnn(mock.annKey, mock.annValue),
			).EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				got, found, err := eval.ListObjsForCondition(conditionName)
				if mock.isErr && err == nil {
					t.Fatalf("Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.passCount != len(got) {
					t.Fatalf("Expected pass count %d got %d", mock.passCount, len(got))
				}
				if !mock.isErr && mock.isFound != found {
					t.Fatalf("Expected found %t got %t", mock.isFound, found)
				}
				if !mock.isErr && mock.failCount != len(eval.ListAllConditionRejects()) {
					failMsgs, _ := eval.ListAllConditionFailures()
					t.Fatalf(
						"Expected rejects %d got %d: Rejects [%#v]",
						mock.failCount,
						len(eval.ListAllConditionRejects()),
						failMsgs,
					)
				}
			}
		})
	}
}

func TestSelectRunCategorizeIsKindAndAPIVersion(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		objs       []*unstructured.Unstructured
		apiVersion string
		kind       string
		passCount  map[string]int
		isFound    bool
		isErr      bool
	}{
		"multiple matches & rejects": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name": "a",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v2",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name": "b",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name": "c",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Service",
						"metadata": map[string]interface{}{
							"name": "d",
						},
					},
				},
			},
			apiVersion: "v1",
			kind:       "Service",
			passCount: map[string]int{
				"apiversion-cond": 3,
				"kind-cond":       1,
			},
			isFound: true,
			isErr:   false,
		},
		"no matches": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name": "a",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Service",
						"metadata": map[string]interface{}{
							"name": "b",
						},
					},
				},
			},
			apiVersion: "alpha",
			kind:       "Deployment",
			passCount: map[string]int{
				"apiversion-cond": 0,
				"kind-cond":       0,
			},
			isFound: false,
			isErr:   false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			conditionOne := "apiversion-cond"
			conditionTwo := "kind-cond"
			// initialize & run
			ul := NewListing(mock.objs)
			ul.WithCondition(conditionOne, NewLazyCondition().IsAPIVersion(mock.apiVersion))
			ul.WithCondition(conditionTwo, NewLazyCondition().IsKind(mock.kind))
			eval, err := ul.EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				// first condition
				got, found, err := eval.ListObjsForCondition(conditionOne)
				if mock.isErr && err == nil {
					t.Fatalf("Cond-1: Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Cond-1: Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.passCount[conditionOne] != len(got) {
					t.Fatalf("Cond-1: Expected pass count %d got %d",
						mock.passCount[conditionOne], len(got),
					)
				}
				if !mock.isErr && mock.isFound != found {
					t.Fatalf("Cond-1: Expected found %t got %t", mock.isFound, found)
				}

				// second condition
				got, found, err = eval.ListObjsForCondition(conditionTwo)
				if mock.isErr && err == nil {
					t.Fatalf("Cond-2: Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Cond-2: Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.passCount[conditionTwo] != len(got) {
					t.Fatalf("Cond-2: Expected pass count %d got %d",
						mock.passCount[conditionTwo], len(got),
					)
				}
				if !mock.isErr && mock.isFound != found {
					t.Fatalf("Cond-2: Expected found %t got %t", mock.isFound, found)
				}
			}
		})
	}
}

func TestSelectRunConditionalOR(t *testing.T) {
	var tests = map[string]struct {
		// NOTE: Provide at-least two elements in objs
		objs []*unstructured.Unstructured

		// api version _AND_ kind is one condition group
		apiVersion string
		kind       string

		// anns _OR_ lbls is one condition group
		anns map[string]string
		lbls map[string]string

		passCount map[string]int
		isFound   bool
		isErr     bool
	}{
		"group 1 = match && group 2 = no match  ": {
			objs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name": "a",
							"annotations": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v2",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name": "b",
							"labels": map[string]interface{}{
								"app": "cstor",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Pod",
						"metadata": map[string]interface{}{
							"name": "c",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "v1",
						"kind":       "Service",
						"metadata": map[string]interface{}{
							"name": "d",
						},
					},
				},
			},
			apiVersion: "v1",
			kind:       "Service",
			anns: map[string]string{
				"app": "cstor",
			},
			lbls: map[string]string{
				"app": "cstor",
			},
			passCount: map[string]int{
				"and-cond": 1,
				"or-cond":  2,
			},
			isFound: true,
			isErr:   false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			andCondition := "and-cond"
			orCondition := "or-cond"
			// initialize & run
			ul := NewListing(mock.objs)
			ul.WithCondition(
				andCondition,
				NewLazyCondition().
					IsAPIVersion(mock.apiVersion).
					IsKind(mock.kind),
			)
			ul.WithCondition(
				orCondition,
				NewLazyORCondition().
					HasAnns(mock.anns).
					HasLabels(mock.lbls),
			)
			eval, err := ul.EvalAllConditions()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%#v]", err)
			}
			// try finding
			if err == nil {
				// first condition
				got, found, err := eval.ListObjsForCondition(andCondition)
				if mock.isErr && err == nil {
					t.Fatalf("Cond-1: Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Cond-1: Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.passCount[andCondition] != len(got) {
					t.Fatalf("Cond-1: Expected pass count %d got %d",
						mock.passCount[andCondition], len(got),
					)
				}
				if !mock.isErr && mock.isFound != found {
					t.Fatalf("Cond-1: Expected found %t got %t", mock.isFound, found)
				}

				// second condition
				got, found, err = eval.ListObjsForCondition(orCondition)
				if mock.isErr && err == nil {
					t.Fatalf("Cond-2: Expected error got none")
				}
				if !mock.isErr && err != nil {
					t.Fatalf("Cond-2: Expected no error got [%#v]", err)
				}
				if !mock.isErr && mock.passCount[orCondition] != len(got) {
					t.Fatalf("Cond-2: Expected pass count %d got %d",
						mock.passCount[orCondition], len(got),
					)
				}
				if !mock.isErr && mock.isFound != found {
					t.Fatalf("Cond-2: Expected found %t got %t", mock.isFound, found)
				}
			}
		})
	}
}
