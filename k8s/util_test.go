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
	"mayadata.io/cstorpoolauto/types"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestUnstructToTyped(t *testing.T) {
	var tests = map[string]struct {
		src    *unstructured.Unstructured
		target interface{}
		isErr  bool
	}{
		"src = nil && target = nil": {
			src:    nil,
			target: nil,
			isErr:  true,
		},
		"src = empty && target = nil": {
			src:    &unstructured.Unstructured{},
			target: nil,
			isErr:  true,
		},
		"src = empty && target = non pointer": {
			src:    &unstructured.Unstructured{},
			target: types.CStorClusterConfig{},
			isErr:  true,
		},
		"src = empty && target = unstruct pointer": {
			src:    &unstructured.Unstructured{},
			target: &unstructured.Unstructured{},
			isErr:  true,
		},
		"src = valid && target = unstruct pointer": {
			src: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "ABC",
				},
			},
			target: &unstructured.Unstructured{},
			isErr:  false,
		},
		"src = valid && target = cstorclusterconfig pointer": {
			src: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "CStorClusterConfig",
				},
			},
			target: &types.CStorClusterConfig{},
			isErr:  false,
		},
		"src = invalid && target = cstorclusterconfig pointer": {
			src: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "CStorClusterConfig",
					"spec": map[string]interface{}{
						"junk": map[string]interface{}{
							"hi": "hello",
						},
					},
				},
			},
			target: &types.CStorClusterConfig{},
			isErr:  false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := UnstructToTyped(mock.src, mock.target)
			if mock.isErr && got == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && got != nil {
				t.Fatalf("Expected no error got [%+v]", got)
			}
		})
	}
}
