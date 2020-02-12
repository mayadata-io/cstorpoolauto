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

package blockdevice

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestGetCapacity(t *testing.T) {
	var tests = map[string]struct {
		src   unstructured.Unstructured
		isErr bool
	}{
		"kind mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "test",
				},
			},
			isErr: true,
		},
		"not found error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": Kind,
				},
			},
			isErr: true,
		},
		"empty value error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": Kind,
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"storage": "",
						},
					},
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": Kind,
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"storage": 0,
						},
					},
				},
			},
			isErr: true,
		},
		"invalid capacity error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": Kind,
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"storage": "10000A",
						},
					},
				},
			},
			isErr: true,
		},
		"valid object": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": Kind,
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"storage": "1000G",
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
			_, err := GetCapacity(mock.src)
			if err == nil && mock.isErr {
				t.Fatal("Expected  error got nil")
			}
		})
	}
}

// TODO @shovan add testcase  for all functions in util once review is done
