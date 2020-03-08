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

package blockdevice

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
)

func TestNewHelper(t *testing.T) {
	var tests = map[string]struct {
		device *unstructured.Unstructured
		isErr  bool
	}{
		"nil device": {
			isErr: true,
		},
		"nil device object": {
			device: &unstructured.Unstructured{},
			isErr:  true,
		},
		"invalid device kind": {
			device: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "Junk",
				},
			},
			isErr: true,
		},
		"valid device kind": {
			device: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.device)
			if mock.isErr && h.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && h.err != nil {
				t.Fatalf("Expected no error got [%+v]", h.err)
			}
		})
	}
}
