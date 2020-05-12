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

package cstorpoolcluster

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
)

func TestNewBuilder(t *testing.T) {
	var tests = map[string]struct {
		opts  []BuildOption
		isErr bool
	}{
		"no build options": {
			opts:  []BuildOption{},
			isErr: false,
		},
		"1 noop build options": {
			opts: []BuildOption{
				func(b *Builder) error {
					return nil
				},
			},
			isErr: false,
		},
		"2 noop build options": {
			opts: []BuildOption{
				func(b *Builder) error {
					return nil
				},
				func(b *Builder) error {
					return nil
				},
			},
			isErr: false,
		},
		"1 build option that returns error": {
			opts: []BuildOption{
				func(b *Builder) error {
					return errors.Errorf("err")
				},
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			b := NewBuilder(mock.opts...)
			if mock.isErr && b.err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && b.err != nil {
				t.Fatalf("Expected no error got [%+v]", b.err)
			}
		})
	}
}

func TestBuilderBuildDesiredState(t *testing.T) {
	var tests = map[string]struct {
		builder *Builder
		expect  *unstructured.Unstructured
		isErr   bool
	}{
		"missing name": {
			builder: &Builder{},
			isErr:   true,
		},
		"missing namespace": {
			builder: &Builder{
				Name: "test",
			},
			isErr: true,
		},
		"missing raid type": {
			builder: &Builder{
				Name:      "test",
				Namespace: "test",
			},
			isErr: true,
		},
		"mirror with 1 host & 1 disks": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeMirror,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1"},
				},
			},
			isErr: true,
		},
		"mirror with 1 host & 3 disks": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeMirror,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3"},
				},
			},
			isErr: true,
		},
		"raidz with 1 host & 1 disks": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1"},
				},
			},
			isErr: true,
		},
		"raidz with 1 host & 4 disks": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3", "bd4"},
				},
			},
			isErr: true,
		},
		"raidz2 with 1 host & 2 disks": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2"},
				},
			},
			isErr: true,
		},
		"raidz2 with 1 host & 5 disks": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3", "bd4", "bd5"},
				},
			},
			isErr: true,
		},
		"raidz2 with 2 hosts & one host with 6 disks & other with 5 disks": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3", "bd4", "bd5", "bd6"},
					"node-002": {"bd21", "bd22", "bd23", "bd24", "bd25"},
				},
			},
			isErr: true,
		},
		"no observed block devices && no observed cspc": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeMirror,
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}(nil),
					},
				},
			},
			isErr: false,
		},
		"1 node && 2 desired block devices && no observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeMirror,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2"},
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "mirror",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd2",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
		},
		"1 node && stripe && 1 new block device && 1 observed cspc device": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeStripe,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2"}, // bd2 is new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1"}, // bd1 is existing
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "stripe",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
										},
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd2",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
		},
		"1 node && stripe && 1 new, 1 remove block device && 2 observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeStripe,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd3"}, // bd3 is new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2"}, // bd1 & bd2 are existing
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "stripe",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
										},
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd3",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
		},
		"2 nodes && stripe && 1 new, 1 remove block device && 2 observed cspc devices": {
			builder: &Builder{
				Name:             "test",
				Namespace:        "test",
				DesiredRAIDType:  types.PoolRAIDTypeStripe,
				OrderedHostNames: []string{"node-002", "node-001"}, // reversed order
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd3"},   // bd3 is new
					"node-002": {"bd21", "bd23"}, // bd23 is new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2"},   // bd1 & bd2 are existing
					"node-002": {"bd21", "bd22"}, // bd21 & bd22 are existing
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "stripe",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd21",
											},
										},
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd23",
											},
										},
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "stripe",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
										},
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd3",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
		},
		"2 nodes && mirror && 1 new, 1 remove block device && 2 observed cspc devices": {
			builder: &Builder{
				Name:             "test",
				Namespace:        "test",
				DesiredRAIDType:  types.PoolRAIDTypeMirror,
				OrderedHostNames: []string{"node-001", "node-002"},
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd3"},   // bd3 is new
					"node-002": {"bd21", "bd23"}, // bd23 is new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2"},   // bd1 & bd2 are existing
					"node-002": {"bd21", "bd22"}, // bd21 & bd22 are existing
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "mirror",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd3",
											},
										},
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "mirror",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd21",
											},
											map[string]interface{}{
												"blockDeviceName": "bd23",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
		},
		"1 nodes && mirror && 2 new, 1 remove block device && 2 observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeMirror,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd3", "bd4"}, // bd3 & bd4 are new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2"}, // bd1 & bd2 are existing
				},
			},
			isErr: true,
		},
		"1 node && mirror && 1 remove block device && 3 observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeMirror,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd3"}, // bd2 is removed
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3"}, // bd1, bd2 & bd3 are existing
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "mirror",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd3",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
		},
		"1 node && raidz && 2 new block devices && 1 observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd3", "bd4"}, // bd3 & bd4 are new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1"}, // bd1 is existing
				},
			},
			isErr: true,
		},
		"1 node && raidz && 3 new block devices && 1 observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3", "bd4"}, // bd2, bd3 & bd4 are new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1"}, // bd1 is existing
				},
			},
			isErr: true,
		},
		"1 node && raidz2 && 3 new block devices && 1 observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ2,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3", "bd4"}, // bd2, bd3 & bd4 are new
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1"}, // bd1 is existing
				},
			},
			isErr: true,
		},
		"1 node && raidz2 && 5 new block devices && 1 observed cspc devices": {
			builder: &Builder{
				Name:            "test",
				Namespace:       "test",
				DesiredRAIDType: types.PoolRAIDTypeRAIDZ2,
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3", "bd4", "bd5", "bd6"},
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1"}, // bd1 is existing
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "raidz2",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd2",
											},
											map[string]interface{}{
												"blockDeviceName": "bd3",
											},
											map[string]interface{}{
												"blockDeviceName": "bd4",
											},
											map[string]interface{}{
												"blockDeviceName": "bd5",
											},
											map[string]interface{}{
												"blockDeviceName": "bd6",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			isErr: false,
		},
		"2 nodes && raidz2 && 5 new block devices && 1 observed cspc devices": {
			builder: &Builder{
				Name:             "test",
				Namespace:        "test",
				DesiredRAIDType:  types.PoolRAIDTypeRAIDZ2,
				OrderedHostNames: []string{"node-002", "node-001"},
				HostNameToDesiredDeviceNames: map[string][]string{
					"node-001": {"bd1", "bd2", "bd3", "bd4", "bd5", "bd6"},
					"node-002": {"bd21", "bd22", "bd23", "bd24", "bd25", "bd26"},
				},
				HostNameToObservedDeviceNames: map[string][]string{
					"node-001": {"bd1"},  // bd1 is existing
					"node-002": {"bd21"}, // bd21 is existing
				},
			},
			expect: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionCStorOpenEBSV1),
					"metadata": map[string]interface{}{
						"name":      "test",
						"namespace": "test",
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "raidz2",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-002",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd21",
											},
											map[string]interface{}{
												"blockDeviceName": "bd22",
											},
											map[string]interface{}{
												"blockDeviceName": "bd23",
											},
											map[string]interface{}{
												"blockDeviceName": "bd24",
											},
											map[string]interface{}{
												"blockDeviceName": "bd25",
											},
											map[string]interface{}{
												"blockDeviceName": "bd26",
											},
										},
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"dataRaidGroupType": "raidz2",
									"thickProvision":    false,
									"compression":       "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-001",
								},
								"dataRaidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd2",
											},
											map[string]interface{}{
												"blockDeviceName": "bd3",
											},
											map[string]interface{}{
												"blockDeviceName": "bd4",
											},
											map[string]interface{}{
												"blockDeviceName": "bd5",
											},
											map[string]interface{}{
												"blockDeviceName": "bd6",
											},
										},
									},
								},
							},
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
			b := mock.builder
			got, err := b.BuildDesiredState()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none: [%+v]", b)
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !reflect.DeepEqual(got, mock.expect) {
				t.Fatalf("Expected no diff got\n%s", cmp.Diff(got, mock.expect))
			}
		})
	}
}
