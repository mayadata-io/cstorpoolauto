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

func TestListContains(t *testing.T) {
	var tests = map[string]struct {
		list    []*unstructured.Unstructured
		given   *unstructured.Unstructured
		isFound bool
	}{
		"nil list": {
			isFound: false,
		},
		"nil list & some given": {
			given: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Some",
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
				},
			},
			isFound: false,
		},
		"empty list & some given": {
			list: []*unstructured.Unstructured{},
			given: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Some",
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
				},
			},
			isFound: false,
		},
		"list & given are different": {
			list: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			given: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Some",
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
				},
			},
			isFound: false,
		},
		"list contains given": {
			list: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			given: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Some",
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
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
			l := List(mock.list)
			found := l.Contains(mock.given)
			if mock.isFound != found {
				t.Fatalf("Expected found %t got %t",
					mock.isFound, found,
				)
			}
		})
	}
}

func TestListContainsAll(t *testing.T) {
	var tests = map[string]struct {
		list    []*unstructured.Unstructured
		given   []*unstructured.Unstructured
		isFound bool
	}{
		"nil list": {
			isFound: false,
		},
		"nil list & some given": {
			given: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			isFound: false,
		},
		"empty list & some given": {
			list: []*unstructured.Unstructured{},
			given: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			isFound: false,
		},
		"list & given are different": {
			list: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			given: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			isFound: false,
		},
		"list contains given": {
			list: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			given: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			isFound: true,
		},
		"list contains all the given": {
			list: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			given: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			isFound: true,
		},
		"list does not contain all the given": {
			list: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Some",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			given: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "SomeOne",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Something",
						"metadata": map[string]interface{}{
							"name":      "test",
							"namespace": "test",
						},
					},
				},
			},
			isFound: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			l := List(mock.list)
			found := l.ContainsAll(mock.given)
			if mock.isFound != found {
				t.Fatalf("Expected found %t got %t",
					mock.isFound, found,
				)
			}
		})
	}
}
