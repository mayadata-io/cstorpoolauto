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

func TestNewListHelper(t *testing.T) {
	var tests = map[string]struct {
		devices []*unstructured.Unstructured
		isErr   bool
	}{
		"nil devices": {
			isErr: false,
		},
		"empty devices": {
			devices: []*unstructured.Unstructured{},
			isErr:   false,
		},
		"1 nil object device": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
			},
			isErr: true,
		},
		"1 invalid kind device": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Junk",
					},
				},
			},
			isErr: true,
		},
		"1 valid kind device": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
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
			h := NewListHelper(mock.devices)
			if mock.isErr && h.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && h.err != nil {
				t.Fatalf("Expected no error got [%+v]", h.err)
			}
		})
	}
}

func TestListHelperGroupDeviceNameByHostName(t *testing.T) {
	var tests = map[string]struct {
		devices                 []*unstructured.Unstructured
		expectHostToDeviceCount map[string]int
		isErr                   bool
	}{
		"nil devices": {
			isErr: false,
		},
		"empty devices": {
			devices: []*unstructured.Unstructured{},
			isErr:   false,
		},
		"1 nil device object": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
			},
			isErr: true,
		},
		"1 invalid device kind": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": "Junk",
					},
				},
			},
			isErr: true,
		},
		"missing specs in device kind": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
					},
				},
			},
			isErr: true,
		},
		"with hostname in device": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"kubernetes.io/hostname": "node-001",
							},
						},
					},
				},
			},
			expectHostToDeviceCount: map[string]int{
				"node-001": 1,
			},
			isErr: false,
		},
		"multiple devices with hostname": {
			devices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"kind": string(types.KindBlockDevice),
						"metadata": map[string]interface{}{
							"name":      "bd1",
							"namespace": "openebs",
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
							"namespace": "openebs",
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
							"name":      "bd3",
							"namespace": "openebs",
							"labels": map[string]interface{}{
								"kubernetes.io/hostname": "node-002",
							},
						},
					},
				},
			},
			expectHostToDeviceCount: map[string]int{
				"node-001": 1,
				"node-002": 2,
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewListHelper(mock.devices)
			got, err := h.GroupDeviceNamesByHostName()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			if len(got) != len(mock.expectHostToDeviceCount) {
				t.Fatalf("Expected host to device count %d got %d",
					len(mock.expectHostToDeviceCount), len(got),
				)
			}
			for host, devices := range got {
				if mock.expectHostToDeviceCount[host] != len(devices) {
					t.Fatalf("Expected device count %d got %d: Host %s",
						mock.expectHostToDeviceCount[host], len(devices), host,
					)
				}
			}
		})
	}
}
