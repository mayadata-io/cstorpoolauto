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

func TestGetCapacityOrError(t *testing.T) {
	var tests = map[string]struct {
		src   unstructured.Unstructured
		isErr bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
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
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"storage": "test",
						},
					},
				},
			},
			isErr: true,
		},
		"valid capacity": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"storage": int64(1000),
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
			_, err := GetCapacityOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}

func TestGetLogicalSectorSizeOrError(t *testing.T) {
	var tests = map[string]struct {
		src   unstructured.Unstructured
		isErr bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
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
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"logicalSectorSize": "test",
						},
					},
				},
			},
			isErr: true,
		},
		"valid logical sector size": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"logicalSectorSize": int64(512),
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
			_, err := GetLogicalSectorSizeOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}

func TestGetPhysicalSectorSizeOrError(t *testing.T) {
	var tests = map[string]struct {
		src   unstructured.Unstructured
		isErr bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
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
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"physicalSectorSize": "test",
						},
					},
				},
			},
			isErr: true,
		},
		"valid physical sector size": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"physicalSectorSize": int64(1000),
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
			_, err := GetPhysicalSectorSizeOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}

func TestGetHostNameOrError(t *testing.T) {
	var tests = map[string]struct {
		src    unstructured.Unstructured
		result string
		isErr  bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
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
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: true,
		},
		"empty value error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"kubernetes.io/hostname": "",
						},
					},
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"kubernetes.io/hostname": 1,
						},
					},
				},
			},
			isErr: true,
		},
		"valid hostname label": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"kubernetes.io/hostname": "my-host-1",
						},
					},
				},
			},
			isErr:  false,
			result: "my-host-1",
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := GetHostNameOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.result {
				t.Fatalf("Expected host name %q got %q", mock.result, result)
			}
		})
	}
}

func TestGetNodeNameOrError(t *testing.T) {
	var tests = map[string]struct {
		src    unstructured.Unstructured
		result string
		isErr  bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
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
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: true,
		},
		"empty value error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"nodeAttributes": map[string]interface{}{
							"nodeName": "",
						},
					},
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"nodeAttributes": map[string]interface{}{
							"nodeName": 1,
						},
					},
				},
			},
			isErr: true,
		},
		"valid node name": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"nodeAttributes": map[string]interface{}{
							"nodeName": "my-host-1",
						},
					},
				},
			},
			isErr:  false,
			result: "my-host-1",
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := GetNodeNameOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.result {
				t.Fatalf("Expected node name %q got %q", mock.result, result)
			}
		})
	}
}

func TestIsActiveOrError(t *testing.T) {
	var tests = map[string]struct {
		src    unstructured.Unstructured
		result bool
		isErr  bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
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
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: true,
		},
		"empty value error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"state": "",
					},
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"state": 1,
					},
				},
			},
			isErr: true,
		},
		"valid status and inactive": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"state": string(types.BlockDeviceInactive),
					},
				},
			},
			isErr:  false,
			result: false,
		},
		"valid status and active": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"state": string(types.BlockDeviceActive),
					},
				},
			},
			isErr:  false,
			result: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := IsActiveOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.result {
				t.Fatalf("Expected status %t got %t", mock.result, result)
			}
		})
	}
}

func TestIsUnclaimedOrError(t *testing.T) {
	var tests = map[string]struct {
		src    unstructured.Unstructured
		result bool
		isErr  bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
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
					"kind": string(types.KindBlockDevice),
				},
			},
			isErr: true,
		},
		"empty value error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"claimState": "",
					},
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"claimState": 1,
					},
				},
			},
			isErr: true,
		},
		"valid claim status and claimed": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"claimState": string(types.BlockDeviceClaimed),
					},
				},
			},
			isErr:  false,
			result: false,
		},
		"valid claim status and unclaimed": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"claimState": string(types.BlockDeviceUnclaimed),
					},
				},
			},
			isErr:  false,
			result: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := IsUnclaimedOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.result {
				t.Fatalf("Expected claim status %t got %t", mock.result, result)
			}
		})
	}
}

func TestHasFileSystemOrError(t *testing.T) {
	var tests = map[string]struct {
		src    unstructured.Unstructured
		result bool
		isErr  bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
		"kind mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "test",
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"filesystem": map[string]interface{}{
							"fsType": 1,
						},
					},
				},
			},
			isErr: true,
		},
		"file system present": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"filesystem": map[string]interface{}{
							"fsType": "ext4",
						},
					},
				},
			},
			result: true,
			isErr:  false,
		},
		"file not system present": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"filesystem": map[string]interface{}{},
					},
				},
			},
			result: false,
			isErr:  false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := HasFileSystemOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.result {
				t.Fatalf("Expected file system check status %t got %t", mock.result, result)
			}
		})
	}
}

func TestGetDeviceTypeOrError(t *testing.T) {
	var tests = map[string]struct {
		src    unstructured.Unstructured
		result string
		isErr  bool
	}{
		"nil object": {
			src: unstructured.Unstructured{
				Object: nil,
			},
			isErr: true,
		},
		"empty object": {
			src:   unstructured.Unstructured{},
			isErr: true,
		},
		"kind mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "test",
				},
			},
			isErr: true,
		},
		"type mismatch error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"details": map[string]interface{}{
							"deviceType": 1,
						},
					},
				},
			},
			isErr: true,
		},
		"empty value error": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"details": map[string]interface{}{
							"deviceType": "",
						},
					},
				},
			},
			isErr: true,
		},
		"device type present": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"details": map[string]interface{}{
							"deviceType": "HDD",
						},
					},
				},
			},
			result: "HDD",
			isErr:  false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := GetDeviceTypeOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.result {
				t.Fatalf("Expected file system check status %v got %v", mock.result, result)
			}
		})
	}
}
