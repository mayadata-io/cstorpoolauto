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

package metac

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"openebs.io/metac/controller/generic"
)

func TestValidateGenericControllerArgs(t *testing.T) {
	var tests = map[string]struct {
		request  *generic.SyncHookRequest
		response *generic.SyncHookResponse
		isErr    bool
	}{
		"nil request": {
			isErr: true,
		},
		"nil request watch": {
			request: &generic.SyncHookRequest{},
			isErr:   true,
		},
		"nil request watch object": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{},
			},
			isErr: true,
		},
		"nil response": {
			request: &generic.SyncHookRequest{
				Watch: &unstructured.Unstructured{
					Object: map[string]interface{}{},
				},
			},
			isErr: true,
		},
		"not nil request, watch & response": {
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
			err := ValidateGenericControllerArgs(mock.request, mock.response)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}
