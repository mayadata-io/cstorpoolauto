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

package unstruct

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestUnstructIterator(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "CSPC",
			"apiVersion": "v2beta1",
			"metadata": map[string]interface{}{
				"name":      "my-cstor-pool",
				"namespace": "openebs-dao",
			},
			"spec": map[string]interface{}{
				"pools": []interface{}{
					// pool on node-001
					map[string]interface{}{
						"nodeSelector": map[string]interface{}{
							"kubernetes.io/hostname": "node-001",
						},
						"raidGroups": []interface{}{
							map[string]interface{}{
								"blockDevices": []interface{}{
									map[string]interface{}{
										"blockDeviceName": "bd11",
									},
									map[string]interface{}{
										"blockDeviceName": "bd12",
									},
								},
								"type": "mirror",
							},
							map[string]interface{}{
								"blockDevices": []interface{}{
									map[string]interface{}{
										"blockDeviceName": "bd13",
									},
									map[string]interface{}{
										"blockDeviceName": "bd14",
									},
								},
								"type": "mirror",
							},
						},
					},
					// pool on node-002
					map[string]interface{}{
						"nodeSelector": map[string]interface{}{
							"kubernetes.io/hostname": "node-002",
						},
						"raidGroups": []interface{}{
							map[string]interface{}{
								"blockDevices": []interface{}{
									map[string]interface{}{
										"blockDeviceName": "bd21",
									},
									map[string]interface{}{
										"blockDeviceName": "bd22",
									},
								},
								"type": "mirror",
							},
							map[string]interface{}{
								"blockDevices": []interface{}{
									map[string]interface{}{
										"blockDeviceName": "bd23",
									},
									map[string]interface{}{
										"blockDeviceName": "bd24",
									},
								},
								"type": "mirror",
							},
						},
					},
				},
			},
		},
	}
	tests := map[string]struct {
		obj                *unstructured.Unstructured
		getNodeName        UnstructOpsFn
		getBlockDeviceName UnstructOpsFn
		isErr              bool
	}{
		"no errors": {
			obj: obj,
			getNodeName: func(obj *unstructured.Unstructured) (err error) {
				_, err =
					GetStringOrError(obj, "spec", "nodeSelector", "kubernetes.io/hostname")
				return
			},
			getBlockDeviceName: func(obj *unstructured.Unstructured) (err error) {
				_, err = GetStringOrError(obj, "spec", "blockDeviceName")
				return
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			getBlockDevicesPerRAIDGroup := func(obj *unstructured.Unstructured) error {
				blockDevices, err := GetSliceOrError(obj, "spec", "blockDevices")
				if err != nil {
					return err
				}
				return SliceIterator(blockDevices).ForEach(mock.getBlockDeviceName)
			}
			getBlockDevicesPerPool := func(obj *unstructured.Unstructured) error {
				raidGroups, err := GetSliceOrError(obj, "spec", "raidGroups")
				if err != nil {
					return err
				}
				return SliceIterator(raidGroups).ForEach(getBlockDevicesPerRAIDGroup)
			}
			pools, err := GetSliceOrError(mock.obj, "spec", "pools")
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if mock.isErr {
				return
			}
			err = SliceIterator(pools).ForEach(mock.getNodeName, getBlockDevicesPerPool)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}
