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

func TestGetCapacity(t *testing.T) {
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
		"zero capacity": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"storage": int64(0),
						},
					},
				},
			},
			isErr: false,
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
			_, err := GetCapacity(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}

func TestGetLogicalSectorSize(t *testing.T) {
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
		"zero logical sector size": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"logicalSectorSize": int64(0),
						},
					},
				},
			},
			isErr: false,
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
			_, err := GetLogicalSectorSize(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}

func TestGetPhysicalSectorSize(t *testing.T) {
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
		"zero physical sector size": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"physicalSectorSize": int64(0),
						},
					},
				},
			},
			isErr: false,
		},
		"valid physical sector size": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"capacity": map[string]interface{}{
							"physicalSectorSize": int64(4096),
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
			_, err := GetPhysicalSectorSize(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}

func TestGetHostName(t *testing.T) {
	var tests = map[string]struct {
		src              unstructured.Unstructured
		expectedHostName string
		isErr            bool
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
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"kubernetes.io/hostname": 1,
						},
					},
				},
			},
			isErr: true,
		},
		"empty value": {
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
			isErr:            false,
			expectedHostName: "",
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
			isErr:            false,
			expectedHostName: "my-host-1",
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			hostName, err := GetHostName(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && hostName != mock.expectedHostName {
				t.Fatalf("Expected host name %q got %q", mock.expectedHostName, hostName)
			}
		})
	}
}

func TestGetHostNameOrError(t *testing.T) {
	var tests = map[string]struct {
		src              unstructured.Unstructured
		expectedHostName string
		isErr            bool
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
			isErr:            false,
			expectedHostName: "my-host-1",
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			hostName, err := GetHostNameOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && hostName != mock.expectedHostName {
				t.Fatalf("Expected host name %q got %q", mock.expectedHostName, hostName)
			}
		})
	}
}

func TestGetNodeName(t *testing.T) {
	var tests = map[string]struct {
		src              unstructured.Unstructured
		expectedNodeName string
		isErr            bool
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
						"nodeAttributes": map[string]interface{}{
							"nodeName": 1,
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
						"nodeAttributes": map[string]interface{}{
							"nodeName": "",
						},
					},
				},
			},
			isErr:            false,
			expectedNodeName: "",
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
			isErr:            false,
			expectedNodeName: "my-host-1",
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			nodeName, err := GetNodeName(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && nodeName != mock.expectedNodeName {
				t.Fatalf("Expected node name %q got %q", mock.expectedNodeName, nodeName)
			}
		})
	}
}

func TestGetNodeNameOrError(t *testing.T) {
	var tests = map[string]struct {
		src              unstructured.Unstructured
		expectedNodeName string
		isErr            bool
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
			isErr:            false,
			expectedNodeName: "my-host-1",
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			nodeName, err := GetNodeNameOrError(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && nodeName != mock.expectedNodeName {
				t.Fatalf("Expected node name %q got %q", mock.expectedNodeName, nodeName)
			}
		})
	}
}

func TestIsActive(t *testing.T) {
	var tests = map[string]struct {
		src      unstructured.Unstructured
		isActive bool
		isErr    bool
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
		"empty value": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"state": "",
					},
				},
			},
			isErr:    false,
			isActive: false,
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
			isErr:    false,
			isActive: false,
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
			isErr:    false,
			isActive: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := IsActive(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.isActive {
				t.Fatalf("Expected status %t got %t", mock.isActive, result)
			}
		})
	}
}

func TestIsUnclaimed(t *testing.T) {
	var tests = map[string]struct {
		src         unstructured.Unstructured
		isUnclaimed bool
		isErr       bool
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
					"status": map[string]interface{}{
						"claimState": 1,
					},
				},
			},
			isErr: true,
		},
		"empty value": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"status": map[string]interface{}{
						"claimState": "",
					},
				},
			},
			isErr:       false,
			isUnclaimed: false,
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
			isErr:       false,
			isUnclaimed: false,
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
			isErr:       false,
			isUnclaimed: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := IsUnclaimed(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.isUnclaimed {
				t.Fatalf("Expected claim status %t got %t", mock.isUnclaimed, result)
			}
		})
	}
}

func TestHasFileSystem(t *testing.T) {
	var tests = map[string]struct {
		src           unstructured.Unstructured
		hasFileSystem bool
		isErr         bool
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
		"empty value": {
			src: unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindBlockDevice),
					"spec": map[string]interface{}{
						"filesystem": map[string]interface{}{
							"fsType": "",
						},
					},
				},
			},
			hasFileSystem: true,
			isErr:         false,
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
			hasFileSystem: true,
			isErr:         false,
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
			hasFileSystem: false,
			isErr:         false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := HasFileSystem(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.hasFileSystem {
				t.Fatalf("Expected file system check status %t got %t", mock.hasFileSystem, result)
			}
		})
	}
}

func TestGetDeviceType(t *testing.T) {
	var tests = map[string]struct {
		src                unstructured.Unstructured
		expectedDeviceType string
		isErr              bool
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
			expectedDeviceType: "",
			isErr:              false,
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
			expectedDeviceType: "HDD",
			isErr:              false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			result, err := GetDeviceType(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && result != mock.expectedDeviceType {
				t.Fatalf("Expected file system check status %v got %v", mock.expectedDeviceType, result)
			}
		})
	}
}
