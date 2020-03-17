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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
)

func TestListSelectionMatchContains(t *testing.T) {
	var tests = map[string]struct {
		selector metac.ResourceSelector
		items    []*unstructured.Unstructured
		expect   *unstructured.Unstructured
		isFound  bool
	}{
		"passing field selector": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"spec.path": "my-path",
						},
					},
				},
			},
			items: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"kind": "Service",
							"name": "my-service",
							"uid":  "svc-001",
						},
						"spec": map[string]interface{}{
							"path": "my-path",
						},
					},
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"kind": "Service",
						"name": "my-service",
						"uid":  "svc-001",
					},
				},
			},
			isFound: true,
		},
		"passing path based field selector": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"spec.path": "/dev/sdb",
						},
					},
				},
			},
			items: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"kind": "Service",
							"name": "my-service",
							"uid":  "svc-001",
						},
						"spec": map[string]interface{}{
							"path": "/dev/sdb",
						},
					},
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"kind": "Service",
						"name": "my-service",
						"uid":  "svc-001",
					},
				},
			},
			isFound: true,
		},
		"finalizer selector": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchSlice: map[string][]string{
							"metadata.finalizers": []string{
								"a-protect",
								"b-protect",
							},
						},
					},
				},
			},
			items: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"kind": "Service",
							"name": "my-service",
							"finalizers": []interface{}{
								"a-protect",
								"b-protect",
							},
						},
					},
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"kind": "Service",
						"name": "my-service",
					},
				},
			},
			isFound: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			s := ListSelector(mock.selector, mock.items...)
			got := s.MatchContains(mock.expect)
			if got != mock.isFound {
				t.Fatalf("Expected %t got %t: matches %d", mock.isFound, got, len(s.matches))
			}
		})
	}
}

func TestListSelectionMergeByDesired(t *testing.T) {
	var tests = map[string]struct {
		observed *unstructured.Unstructured
		desired  *unstructured.Unstructured
		want     map[string]interface{}
		isErr    bool
	}{
		"observed == desired : kind matches => observed == want": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
				},
			},
			want: map[string]interface{}{
				"kind":       "Pod",
				"apiVersion": "v1",
			},
			isErr: false,
		},
		"observed != desired : kind nomatch: Pod to Service": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Service",
				},
			},
			want: map[string]interface{}{
				"kind":       "Service",
				"apiVersion": "v1",
			},
			isErr: false,
		},
		"observed == desired : kind & apiVersion matches => observed == want": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"name": "Hello",
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
				},
			},
			want: map[string]interface{}{
				"kind":       "Pod",
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"name": "Hello",
				},
			},
			isErr: false,
		},
		"observed != desired : finalizer nomatch : desired finalizers": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"finalizers": []interface{}{
							"a-protect",
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"finalizers": []interface{}{
							"a-protect",
							"b-protect",
						},
					},
				},
			},
			want: map[string]interface{}{
				"kind":       "Pod",
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"finalizers": []interface{}{
						"a-protect",
						"b-protect",
					},
				},
			},
			isErr: false,
		},
		"observed != desired : finalizer nomatch : empty finalizers": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"finalizers": []interface{}{
							"a-protect",
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"finalizers": []interface{}{},
					},
				},
			},
			want: map[string]interface{}{
				"kind":       "Pod",
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"finalizers": []interface{}{},
				},
			},
			isErr: false,
		},
		"observed != desired : missing finalizer => observed == want": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"finalizers": []interface{}{
							"a-protect",
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata":   map[string]interface{}{},
				},
			},
			want: map[string]interface{}{
				"kind":       "Pod",
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"finalizers": []interface{}{
						"a-protect",
					},
				},
			},
			isErr: false,
		},
		"observed != desired : missing labels : => observed == want": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "kool",
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata":   map[string]interface{}{},
				},
			},
			want: map[string]interface{}{
				"kind":       "Pod",
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						"app": "kool",
					},
				},
			},
			isErr: false,
		},
		"observed != desired : missing annotations : => observed == want": {
			observed: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata": map[string]interface{}{
						"annotations": map[string]interface{}{
							"app": "kool",
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       "Pod",
					"apiVersion": "v1",
					"metadata":   map[string]interface{}{},
				},
			},
			want: map[string]interface{}{
				"kind":       "Pod",
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"annotations": map[string]interface{}{
						"app": "kool",
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
			merged, err := mergeByDesired(mock.observed, mock.desired)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !reflect.DeepEqual(merged, mock.want) {
				t.Fatalf("merged != want:\n%s", cmp.Diff(merged, mock.want))
			}
		})
	}
}

func TestListSelectionMatchDesired(t *testing.T) {
	var tests = map[string]struct {
		selector       metac.ResourceSelector
		observedObjs   []*unstructured.Unstructured
		desired        *unstructured.Unstructured
		isDesiredMatch bool
	}{
		"labels selector => merged == observed": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchLabels: map[string]string{
							"app": "kool",
						},
					},
				},
			},
			observedObjs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "my-pod",
							"labels": map[string]interface{}{
								"app": "kool",
							},
							"finalizers": []interface{}{
								"a-protect",
								"b-protect",
							},
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
					"metadata": map[string]interface{}{
						"name": "my-pod",
					},
				},
			},
			isDesiredMatch: true,
		},
		"finalizer selector => merged == observed": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchSlice: map[string][]string{
							"metadata.finalizers": []string{
								"a-protect",
								"b-protect",
							},
						},
					},
				},
			},
			observedObjs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "my-pod",
							"labels": map[string]interface{}{
								"app": "kool",
							},
							"finalizers": []interface{}{
								"a-protect",
								"b-protect",
							},
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
					"metadata": map[string]interface{}{
						"name": "my-pod",
					},
				},
			},
			isDesiredMatch: true,
		},
		"labels selector => merged != observed": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchLabels: map[string]string{
							"app": "kool",
						},
					},
				},
			},
			observedObjs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "my-pod",
							"labels": map[string]interface{}{
								"app": "kool",
							},
							"finalizers": []interface{}{
								"a-protect",
								"b-protect",
							},
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
					"metadata": map[string]interface{}{
						"name": "my-pod",
					},
					"labels": map[string]interface{}{
						"app":  "kool",
						"more": "stuff", // extra
					},
				},
			},
			isDesiredMatch: false,
		},
		"kind & name selector : observed has extra labels & anns => merged == observed": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"kind":          "Pod",
							"metadata.name": "my-pod",
						},
					},
				},
			},
			observedObjs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "my-pod",
							"labels": map[string]interface{}{
								"app": "kool",
							},
							"annotations": map[string]interface{}{
								"name": "kool",
							},
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
					"metadata": map[string]interface{}{
						"name": "my-pod",
					},
				},
			},
			isDesiredMatch: true,
		},
		"kind & name selector : observed has extra anns => merged == observed": {
			selector: metac.ResourceSelector{
				SelectorTerms: []*metac.SelectorTerm{
					&metac.SelectorTerm{
						MatchFields: map[string]string{
							"kind":          "Pod",
							"metadata.name": "my-pod",
						},
					},
				},
			},
			observedObjs: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Pod",
						"metadata": map[string]interface{}{
							"name": "my-pod",
							"labels": map[string]interface{}{
								"app": "kool",
							},
							"annotations": map[string]interface{}{
								"name": "kool",
							},
						},
					},
				},
			},
			desired: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Pod",
					"metadata": map[string]interface{}{
						"name": "my-pod",
						"labels": map[string]interface{}{
							"app": "kool",
						},
					},
				},
			},
			isDesiredMatch: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			s := ListSelector(mock.selector, mock.observedObjs...)
			got := s.MatchDesired(mock.desired)
			if got != mock.isDesiredMatch {
				t.Fatalf("Expected %t got %t", mock.isDesiredMatch, got)
			}
		})
	}
}
