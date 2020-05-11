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

package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	stringcommon "mayadata.io/cstorpoolauto/common/string"
	"mayadata.io/cstorpoolauto/types"
)

func TestNewHelper(t *testing.T) {
	var tests = map[string]struct {
		obj   *unstructured.Unstructured
		isErr bool
	}{
		"cspc kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorPoolCluster),
				},
			},
			isErr: false,
		},
		"invalid kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "InValid",
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.obj)
			if mock.isErr && h.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && h.err != nil {
				t.Fatalf("Expected no error got [%+v]", h.err)
			}
		})
	}
}

func TestHelperGetAllHostNames(t *testing.T) {
	var tests = map[string]struct {
		obj    *unstructured.Unstructured
		expect []string
		isErr  bool
	}{
		"cspc kind - no data": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorPoolCluster),
				},
			},
			isErr: true,
		},
		"invalid kind": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": "InValid",
				},
			},
			isErr: true,
		},
		"cspc kind - 2 pools - full blown specs": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorPoolCluster),
					"spec": map[string]interface{}{
						"pools": []interface{}{
							// pool on node-001
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								// 3 raidGroups per pool
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
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd15",
											},
											map[string]interface{}{
												"blockDeviceName": "bd16",
											},
										},
										"type": "mirror",
									},
								},
							},
							// pool on node-002
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
								// 3 raidGroups per pool
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
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd25",
											},
											map[string]interface{}{
												"blockDeviceName": "bd26",
											},
										},
										"type": "mirror",
									},
								},
							},
						},
					},
				},
			},
			isErr:  false,
			expect: []string{"node-001", "node-002"},
		},
		"cspc kind - 1 pool": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorPoolCluster),
					"spec": map[string]interface{}{
						"pools": []interface{}{
							// pool on node-001
							map[string]interface{}{
								"poolConfig": map[string]interface{}{},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"raidGroups": []interface{}{},
							},
						},
					},
				},
			},
			isErr:  false,
			expect: []string{"node-001"},
		},
		"cspc kind - 0 pool": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorPoolCluster),
					"spec": map[string]interface{}{
						"pools": []interface{}{},
					},
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.obj)
			got, err := h.GetOrderedHostNames()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if stringcommon.NewEquality(got, mock.expect).IsDiff() {
				t.Fatalf("Expected no diff got diff: got [%+v]", got)
			}
		})
	}
}

func TestHelperGetAllHostNamesOrCached(t *testing.T) {
	var tests = map[string]struct {
		cached []string
		expect []string
		isErr  bool
	}{
		"cspc - 1 cached host": {
			cached: []string{"node-001"},
			expect: []string{"node-001"},
			isErr:  false,
		},
		"cspc - 2 cached hosts": {
			cached: []string{"node-001", "node-002"},
			expect: []string{"node-001", "node-002"},
			isErr:  false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := &Helper{
				allHostNames: mock.cached,
			}
			got, err := h.GetOrderedHostNamesOrCached()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if stringcommon.NewEquality(got, mock.expect).IsDiff() {
				t.Fatalf("Expected no diff got diff: got [%+v]", got)
			}
		})
	}
}

func TestHelperGroupBlockDeviceNamesByHostName(t *testing.T) {
	var tests = map[string]struct {
		obj    *unstructured.Unstructured
		expect map[string][]string
		isErr  bool
	}{
		"cspc kind - 2 pools - full blown specs": {
			obj: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind": string(types.KindCStorPoolCluster),
					"spec": map[string]interface{}{
						"pools": []interface{}{
							// pool on node-001
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								// 3 raidGroups per pool
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
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd15",
											},
											map[string]interface{}{
												"blockDeviceName": "bd16",
											},
										},
										"type": "mirror",
									},
								},
							},
							// pool on node-002
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
								// 2 raidGroups per pool
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
							// pool on node-003
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-003",
								},
								// 1 raidGroups per pool
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd31",
											},
											map[string]interface{}{
												"blockDeviceName": "bd32",
											},
										},
										"type": "mirror",
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
			expect: map[string][]string{
				"node-001": []string{"bd11", "bd12", "bd13", "bd14", "bd15", "bd16"},
				"node-002": []string{"bd21", "bd22", "bd23", "bd24"},
				"node-003": []string{"bd31", "bd32"},
			},
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := NewHelper(mock.obj)
			got, err := h.GroupBlockDeviceNamesByHostName()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if len(mock.expect) != len(got) {
				t.Fatalf("Expected count %d got %d", len(mock.expect), len(got))
			}
			for host, expectDevices := range mock.expect {
				gotDevices := got[host]
				if stringcommon.NewEquality(gotDevices, expectDevices).IsDiff() {
					t.Fatalf(
						"Expected no diff:\nhost %q\ngot devices [%+v]\nexpect devices [%+v]",
						host, gotDevices, expectDevices,
					)
				}
			}
		})
	}
}

func TestHelperGroupBlockDeviceNamesByHostNameOrCached(t *testing.T) {
	var tests = map[string]struct {
		cached map[string][]string
		expect map[string][]string
		isErr  bool
	}{
		"cspc - 1 cached host": {
			cached: map[string][]string{
				"node-001": []string{"bd1"},
			},
			expect: map[string][]string{
				"node-001": []string{"bd1"},
			},
			isErr: false,
		},
		"cspc - 2 cached hosts": {
			cached: map[string][]string{
				"node-001": []string{"bd1"},
				"node-002": []string{"bd2"},
			},
			expect: map[string][]string{
				"node-001": []string{"bd1"},
				"node-002": []string{"bd2"},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			h := &Helper{
				hostNameToBlockDeviceNames: mock.cached,
			}
			got, err := h.GroupBlockDeviceNamesByHostNameOrCached()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if len(mock.expect) != len(got) {
				t.Fatalf("Expected count %d got %d", len(mock.expect), len(got))
			}
			for host, expectDevices := range mock.expect {
				gotDevices := got[host]
				if stringcommon.NewEquality(gotDevices, expectDevices).IsDiff() {
					t.Fatalf(
						"Expected no diff:\nhost %q\ngot devices [%+v]\nexpect devices [%+v]",
						host, gotDevices, expectDevices,
					)
				}
			}
		})
	}
}
