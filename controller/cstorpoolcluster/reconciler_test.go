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

package cstorpoolcluster

import (
	"reflect"
	"testing"

	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
	stringutil "mayadata.io/cstorpoolauto/util/string"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestPlannerGetDesiredCStorPoolCluster(t *testing.T) {
	var tests = map[string]struct {
		observedCStorClusterPlan        *types.CStorClusterPlan
		observedClusterConfig           *unstructured.Unstructured
		desiredRAIDType                 string
		nodeNameToObservedStorageSetUID map[string]string
		nodeNameToDesiredCSPCDevices    map[string][]string
		expectCSPC                      *unstructured.Unstructured
	}{
		"Mirror : 2x4 : 2 Pools x 4 Disks on each Pool": {
			observedCStorClusterPlan: &types.CStorClusterPlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-2x4",
					Namespace: "test-mirror",
					UID:       "plan-101",
				},
			},
			observedClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"uid": "config-101",
					},
				},
			},
			desiredRAIDType: string(types.PoolRAIDTypeMirror),
			nodeNameToObservedStorageSetUID: map[string]string{
				"node-101": "sset-101",
				"node-201": "sset-201",
			},
			nodeNameToDesiredCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3", "bd-4"},
				"node-201": []string{"bd-5", "bd-6", "bd-7", "bd-8"},
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionOpenEBSV1Alpha1),
					"metadata": map[string]interface{}{
						"name":      "mirror-2x4",
						"namespace": "test-mirror",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterPlanUID):   "plan-101",
							string(types.AnnKeyCStorClusterConfigUID): "config-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-101",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-2",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-3",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-4",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-201",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-5",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-6",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-7",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-8",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
						},
					},
				},
			},
		},
		"Mirror : 2x2 : 2 Pools x 2 Disks on each Pool": {
			observedCStorClusterPlan: &types.CStorClusterPlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "mirror-2x2",
					Namespace: "test-mirror",
					UID:       "plan-101",
				},
			},
			observedClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"uid": "config-101",
					},
				},
			},
			desiredRAIDType: string(types.PoolRAIDTypeMirror),
			nodeNameToObservedStorageSetUID: map[string]string{
				"node-101": "sset-101",
				"node-201": "sset-201",
			},
			nodeNameToDesiredCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionOpenEBSV1Alpha1),
					"metadata": map[string]interface{}{
						"name":      "mirror-2x2",
						"namespace": "test-mirror",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterPlanUID):   "plan-101",
							string(types.AnnKeyCStorClusterConfigUID): "config-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-101",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-2",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "mirror",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-201",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-3",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-4",
											},
										},
										"type":         "mirror",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
						},
					},
				},
			},
		},
		"Stripe: 2x4 : 2 Pools x 4 Disks on each Pool": {
			observedCStorClusterPlan: &types.CStorClusterPlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "stripe-2x4",
					Namespace: "test-stripe",
					UID:       "plan-101",
				},
			},
			observedClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"uid": "config-101",
					},
				},
			},
			desiredRAIDType: string(types.PoolRAIDTypeStripe),
			nodeNameToObservedStorageSetUID: map[string]string{
				"node-101": "sset-101",
				"node-201": "sset-201",
			},
			nodeNameToDesiredCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3", "bd-4"},
				"node-201": []string{"bd-5", "bd-6", "bd-7", "bd-8"},
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionOpenEBSV1Alpha1),
					"metadata": map[string]interface{}{
						"name":      "stripe-2x4",
						"namespace": "test-stripe",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterPlanUID):   "plan-101",
							string(types.AnnKeyCStorClusterConfigUID): "config-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "stripe",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-101",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-1",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-2",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-3",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-4",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "stripe",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-201",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-5",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-6",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-7",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-8",
											},
										},
										"type":         "stripe",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
						},
					},
				},
			},
		},
		"RAIDZ : 2x6 : 2 Pools x 6 Disks on each Pool": {
			observedCStorClusterPlan: &types.CStorClusterPlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "raidz-2x6",
					Namespace: "test-raidz",
					UID:       "plan-101",
				},
			},
			observedClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"uid": "config-101",
					},
				},
			},
			desiredRAIDType: string(types.PoolRAIDTypeRAIDZ),
			nodeNameToObservedStorageSetUID: map[string]string{
				"node-101": "sset-101",
				"node-201": "sset-201",
			},
			nodeNameToDesiredCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3", "bd-4", "bd-5", "bd-6"},
				"node-201": []string{"bd-7", "bd-8", "bd-9", "bd-10", "bd-11", "bd-12"},
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionOpenEBSV1Alpha1),
					"metadata": map[string]interface{}{
						"name":      "raidz-2x6",
						"namespace": "test-raidz",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterPlanUID):   "plan-101",
							string(types.AnnKeyCStorClusterConfigUID): "config-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "raidz",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-101",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-2",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-3",
											},
										},
										"type":         "raidz",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-4",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-5",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-6",
											},
										},
										"type":         "raidz",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "raidz",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-201",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-7",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-8",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-9",
											},
										},
										"type":         "raidz",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-10",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-11",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-12",
											},
										},
										"type":         "raidz",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
						},
					},
				},
			},
		},
		"RAIDZ2 : 2x6 : 2 Pools x 6 Disks on each Pool": {
			observedCStorClusterPlan: &types.CStorClusterPlan{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "raidz2-2x6",
					Namespace: "test-raidz2",
					UID:       "plan-101",
				},
			},
			observedClusterConfig: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"metadata": map[string]interface{}{
						"uid": "config-101",
					},
				},
			},
			desiredRAIDType: string(types.PoolRAIDTypeRAIDZ2),
			nodeNameToObservedStorageSetUID: map[string]string{
				"node-101": "sset-101",
				"node-201": "sset-201",
			},
			nodeNameToDesiredCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3", "bd-4", "bd-5", "bd-6"},
				"node-201": []string{"bd-7", "bd-8", "bd-9", "bd-10", "bd-11", "bd-12"},
			},
			expectCSPC: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"kind":       string(types.KindCStorPoolCluster),
					"apiVersion": string(types.APIVersionOpenEBSV1Alpha1),
					"metadata": map[string]interface{}{
						"name":      "raidz2-2x6",
						"namespace": "test-raidz2",
						"annotations": map[string]interface{}{
							string(types.AnnKeyCStorClusterPlanUID):   "plan-101",
							string(types.AnnKeyCStorClusterConfigUID): "config-101",
						},
					},
					"spec": map[string]interface{}{
						"pools": []interface{}{
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "raidz2",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-101",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-1",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-2",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-3",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-4",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-5",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-6",
											},
										},
										"type":         "raidz2",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
							map[string]interface{}{
								"poolConfig": map[string]interface{}{
									"defaultRaidGroupType": "raidz2",
									"overProvisioning":     false,
									"compression":          "off",
								},
								"nodeSelector": map[string]interface{}{
									"kubernetes.io/hostname": "node-201",
								},
								"raidGroups": []interface{}{
									map[string]interface{}{
										"blockDevices": []interface{}{
											map[string]interface{}{
												"blockDeviceName": "bd-7",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-8",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-9",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-10",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-11",
											},
											map[string]interface{}{
												"blockDeviceName": "bd-12",
											},
										},
										"type":         "raidz2",
										"isWriteCache": false,
										"isSpare":      false,
										"isReadCache":  false,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &Planner{
				ObservedCStorClusterPlan:        mock.observedCStorClusterPlan,
				ObservedClusterConfig:           mock.observedClusterConfig,
				desiredRAIDType:                 mock.desiredRAIDType,
				nodeNameToObservedStorageSetUID: mock.nodeNameToObservedStorageSetUID,
				nodeNameToDesiredCSPCDevices:    mock.nodeNameToDesiredCSPCDevices,
			}
			got := p.getDesiredCStorPoolCluster()
			expect := mock.expectCSPC
			// api version check
			if got.GetAPIVersion() != expect.GetAPIVersion() {
				t.Fatalf(
					"Expected api version %s got %s",
					expect.GetAPIVersion(), got.GetAPIVersion(),
				)
			}
			// kind check
			if got.GetKind() != expect.GetKind() {
				t.Fatalf(
					"Expected kind %s got %s", expect.GetKind(), got.GetKind(),
				)
			}
			// metadata equality check
			gotMeta := unstruct.MustGetNestedMap(got, "metadata")
			expectMeta := unstruct.MustGetNestedMap(expect, "metadata")
			if !reflect.DeepEqual(gotMeta, expectMeta) {
				t.Fatalf("Expected cspc meta: [%+v] got: [%+v]", expectMeta, gotMeta)
			}
			// pools equality check
			gotPools := unstruct.MustGetNestedSlice(got, "spec", "pools")
			expectPools := unstruct.MustGetNestedSlice(expect, "spec", "pools")
			gotPoolCount := len(gotPools)
			expectPoolCount := len(expectPools)
			if gotPoolCount != expectPoolCount {
				t.Fatalf("Expected pool count %d got %d", expectPoolCount, gotPoolCount)
			}
			var successCount int
			for idx := range gotPools {
				for jdx := range expectPools {
					if reflect.DeepEqual(gotPools[idx], expectPools[jdx]) {
						// any got index can match any expect index
						successCount++
						break
					}
				}
			}
			if successCount != gotPoolCount {
				gotJSON, _ := got.MarshalJSON()
				expectJSON, _ := mock.expectCSPC.MarshalJSON()
				t.Fatalf("Expected cspc:\n%sGot cspc:\n%s", expectJSON, gotJSON)
			}
		})
	}
}

func TestPlannerInitNodeToDesiredCSPCDevices(t *testing.T) {
	var tests = map[string]struct {
		storageSetToBlockDevices    map[string][]string
		storageSetUIDToNodeName     map[string]string
		nodeNameToCSPCDevices       map[string][]string
		expectNodeNameToCSPCDevices map[string][]string
		isErr                       bool
	}{
		"nil observed block devices": {
			storageSetToBlockDevices: nil,
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1"},
			},
			expectNodeNameToCSPCDevices: nil,
			isErr:                       false,
		},
		"invalid storageset to node": {
			storageSetToBlockDevices: map[string][]string{
				"sset-invalid": []string{"bd-invalid-1"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1"},
			},
			isErr: true,
		},
		"missing storageset uid": {
			storageSetToBlockDevices: map[string][]string{
				"": []string{"bd-invalid-1"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1"},
			},
			isErr: true,
		},
		"missing node": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-invalid-1"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1"},
			},
			isErr: true,
		},
		"Mirror - No Change - 3x2 Desired Block Devices & 3x2 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-1", "bd-2"},
				"sset-201": []string{"bd-3", "bd-4"},
				"sset-301": []string{"bd-5", "bd-6"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
				"node-301": []string{"bd-5", "bd-6"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
				"node-301": []string{"bd-5", "bd-6"},
			},
			isErr: false,
		},
		"Mirror - Add - 6x2 Desired Block Devices & 3x2 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-1", "bd-2"},
				"sset-201": []string{"bd-3", "bd-4"},
				"sset-301": []string{"bd-5", "bd-6"},
				"sset-401": []string{"bd-7", "bd-8"},
				"sset-501": []string{"bd-9", "bd-10"},
				"sset-601": []string{"bd-11", "bd-12"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
				"sset-401": "node-401",
				"sset-501": "node-501",
				"sset-601": "node-601",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
				"node-301": []string{"bd-5", "bd-6"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
				"node-301": []string{"bd-5", "bd-6"},
				"node-401": []string{"bd-7", "bd-8"},
				"node-501": []string{"bd-9", "bd-10"},
				"node-601": []string{"bd-11", "bd-12"},
			},
			isErr: false,
		},
		"Mirror - Add & Update - 6x2 Desired Block Devices & 3x2 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-1", "bd-22"},
				"sset-201": []string{"bd-33", "bd-4"},
				"sset-301": []string{"bd-55", "bd-66"},
				"sset-401": []string{"bd-7", "bd-8"},
				"sset-501": []string{"bd-9", "bd-10"},
				"sset-601": []string{"bd-11", "bd-12"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
				"sset-401": "node-401",
				"sset-501": "node-501",
				"sset-601": "node-601",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
				"node-301": []string{"bd-5", "bd-6"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-22"},
				"node-201": []string{"bd-33", "bd-4"},
				"node-301": []string{"bd-55", "bd-66"},
				"node-401": []string{"bd-7", "bd-8"},
				"node-501": []string{"bd-9", "bd-10"},
				"node-601": []string{"bd-11", "bd-12"},
			},
			isErr: false,
		},
		"Mirror - Update - 3x2 Desired Block Devices & 3x2 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-1", "bd-22"},
				"sset-201": []string{"bd-33", "bd-4"},
				"sset-301": []string{"bd-55", "bd-66"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
				"node-301": []string{"bd-5", "bd-6"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-22"},
				"node-201": []string{"bd-33", "bd-4"},
				"node-301": []string{"bd-55", "bd-66"},
			},
			isErr: false,
		},
		"Mirror - Update & Delete - 2x2 Desired Block Devices & 3x2 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-1", "bd-22"},
				"sset-201": []string{"bd-33", "bd-4"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2"},
				"node-201": []string{"bd-3", "bd-4"},
				"node-301": []string{"bd-5", "bd-6"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-22"},
				"node-201": []string{"bd-33", "bd-4"},
			},
			isErr: false,
		},
		"RAIDZ - Delete - 2x3 Desired Block Devices & 3x3 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-1", "bd-2", "bd-3"},
				"sset-201": []string{"bd-4", "bd-5", "bd-6"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3"},
				"node-201": []string{"bd-4", "bd-5", "bd-6"},
				"node-301": []string{"bd-7", "bd-8", "bd-9"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3"},
				"node-201": []string{"bd-4", "bd-5", "bd-6"},
			},
			isErr: false,
		},
		"RAIDZ - Update & Delete - 2x3 Desired Block Devices & 3x3 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-11", "bd-2", "bd-33"},
				"sset-201": []string{"bd-4", "bd-55", "bd-6"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3"},
				"node-201": []string{"bd-4", "bd-5", "bd-6"},
				"node-301": []string{"bd-7", "bd-8", "bd-9"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-11", "bd-2", "bd-33"},
				"node-201": []string{"bd-4", "bd-55", "bd-6"},
			},
			isErr: false,
		},
		"RAIDZ - Add - 4x3 Desired Block Devices & 3x3 Observed CSPC Devices": {
			storageSetToBlockDevices: map[string][]string{
				"sset-101": []string{"bd-1", "bd-2", "bd-3"},
				"sset-201": []string{"bd-4", "bd-5", "bd-6"},
				"sset-301": []string{"bd-7", "bd-8", "bd-9"},
				"sset-401": []string{"bd-10", "bd-11", "bd-12"},
			},
			storageSetUIDToNodeName: map[string]string{
				"sset-101": "node-101",
				"sset-201": "node-201",
				"sset-301": "node-301",
				"sset-401": "node-401",
			},
			nodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3"},
				"node-201": []string{"bd-4", "bd-5", "bd-6"},
				"node-301": []string{"bd-7", "bd-8", "bd-9"},
			},
			expectNodeNameToCSPCDevices: map[string][]string{
				"node-101": []string{"bd-1", "bd-2", "bd-3"},
				"node-201": []string{"bd-4", "bd-5", "bd-6"},
				"node-301": []string{"bd-7", "bd-8", "bd-9"},
				"node-401": []string{"bd-10", "bd-11", "bd-12"},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &Planner{
				storageSetToObservedBlockDevices: mock.storageSetToBlockDevices,
				storageSetUIDToObservedNodeName:  mock.storageSetUIDToNodeName,
				nodeNameToObservedCSPCDevices:    mock.nodeNameToCSPCDevices,
			}
			p.initNodeToDesiredCSPCDevices()
			if len(p.nodeNameToDesiredCSPCDevices) != len(mock.expectNodeNameToCSPCDevices) {
				t.Fatalf("Expected node to devices mapping count %d got %d",
					len(mock.expectNodeNameToCSPCDevices), len(p.nodeNameToDesiredCSPCDevices),
				)
			}
			for node, gotDevices := range p.nodeNameToDesiredCSPCDevices {
				expectDevices := mock.expectNodeNameToCSPCDevices[node]
				_, adds, deletes := stringutil.NewEquality(expectDevices, gotDevices).Diff()
				if len(adds) != 0 || len(deletes) != 0 {
					t.Fatalf(
						"Expected devices [%+v] got [%+v] at node %s",
						expectDevices, gotDevices, node,
					)
				}
			}
		})
	}
}

func TestPlannerInitStorageSetToObservedBlockDevices(t *testing.T) {
	var tests = map[string]struct {
		blockDevices      []*unstructured.Unstructured
		expectDeviceNames map[string][]string
		isErr             bool
	}{
		"invalid blockdevice": {
			blockDevices: []*unstructured.Unstructured{
				&unstructured.Unstructured{},
			},
			isErr: true,
		},
		"missing storageset uid in label": {
			blockDevices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name":   "no-lbl",
							"labels": map[string]interface{}{},
						},
					},
				},
			},
			isErr: true,
		},
		"single valid blockdevice": {
			blockDevices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "single-valid",
							"labels": map[string]interface{}{
								string(types.AnnKeyCStorClusterStorageSetUID): "101",
							},
						},
					},
				},
			},
			expectDeviceNames: map[string][]string{
				"101": []string{"single-valid"},
			},
			isErr: false,
		},
		"multi valid blockdevices": {
			blockDevices: []*unstructured.Unstructured{
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "multi-valid-1",
							"labels": map[string]interface{}{
								string(types.AnnKeyCStorClusterStorageSetUID): "101",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "multi-valid-2",
							"labels": map[string]interface{}{
								string(types.AnnKeyCStorClusterStorageSetUID): "101",
							},
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"metadata": map[string]interface{}{
							"name": "single-valid",
							"labels": map[string]interface{}{
								string(types.AnnKeyCStorClusterStorageSetUID): "201",
							},
						},
					},
				},
			},
			expectDeviceNames: map[string][]string{
				"101": []string{"multi-valid-1", "multi-valid-2"},
				"201": []string{"single-valid"},
			},
			isErr: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &Planner{
				ObservedBlockDevices: mock.blockDevices,
			}
			err := p.initStorageSetToObservedBlockDevices()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr {
				for ssetUID, expectDevices := range mock.expectDeviceNames {
					gotDevices := p.storageSetToObservedBlockDevices[ssetUID]
					if len(gotDevices) != len(expectDevices) {
						t.Fatalf(
							"Expected device count %d got %d",
							len(expectDevices), len(gotDevices),
						)
					}
					eq := stringutil.NewEquality(expectDevices, gotDevices)
					_, adds, removals := eq.Diff()
					if len(adds) != 0 || len(removals) != 0 {
						t.Fatalf(
							"Expected [%+v] got [%+v] for StorageSetUID %s",
							expectDevices, gotDevices, ssetUID,
						)
					}
				}
			}
		})
	}
}

func TestPlannerInitNodeToObservedCSPCDevices(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "CStorPoolCluster",
			"apiVersion": "v1",
			"metadata": map[string]interface{}{
				"name":      "wow-cstor-pool",
				"namespace": "mayadata-dao",
			},
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
	}
	var tests = map[string]struct {
		cspc                  *unstructured.Unstructured
		isErr                 bool
		expectedNodeToDevices map[string][]string
	}{
		"use above fake cspc obj": {
			cspc:  obj,
			isErr: false,
			expectedNodeToDevices: map[string][]string{
				"node-001": []string{"bd11", "bd12", "bd13", "bd14", "bd15", "bd16"},
				"node-002": []string{"bd21", "bd22", "bd23", "bd24", "bd25", "bd26"},
			},
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			p := &Planner{
				ObservedCStorPoolCluster: mock.cspc,
			}
			err := p.initNodeToObservedCSPCDevices()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr {
				if len(p.nodeNameToObservedCSPCDevices) != 2 {
					t.Fatalf(
						"Expected pool count 2 got %d: [%+v]",
						len(p.nodeNameToObservedCSPCDevices),
						p.nodeNameToObservedCSPCDevices,
					)
				}
				for nodeName, actualDevices := range p.nodeNameToObservedCSPCDevices {
					expectedDevices := mock.expectedNodeToDevices[nodeName]
					if len(expectedDevices) != len(actualDevices) {
						t.Fatalf(
							"Expected block device count %d got %d: NodeName %q",
							len(expectedDevices),
							len(actualDevices),
							nodeName,
						)
					}
					for idx, expectedDevice := range expectedDevices {
						if actualDevices[idx] != expectedDevice {
							t.Fatalf(
								"Expected block device %q got %q at index %d: NodeName %q",
								expectedDevice, actualDevices[idx], idx, nodeName,
							)
						}
					}
				}
			}
		})
	}
}

func TestPlannerIsReadyByNodeCount(t *testing.T) {
	var tests = map[string]struct {
		planner *Planner
		isReady bool
	}{
		"node count == observed storageset count": {
			planner: &Planner{
				ObservedCStorClusterPlan: &types.CStorClusterPlan{
					Spec: types.CStorClusterPlanSpec{
						Nodes: []types.CStorClusterPlanNode{
							types.CStorClusterPlanNode{},
							types.CStorClusterPlanNode{},
						},
					},
				},
				ObservedStorageSets: []*unstructured.Unstructured{
					&unstructured.Unstructured{},
					&unstructured.Unstructured{},
				},
			},
			isReady: true,
		},
		"node count < observed storageset count": {
			planner: &Planner{
				ObservedCStorClusterPlan: &types.CStorClusterPlan{
					Spec: types.CStorClusterPlanSpec{
						Nodes: []types.CStorClusterPlanNode{
							types.CStorClusterPlanNode{},
						},
					},
				},
				ObservedStorageSets: []*unstructured.Unstructured{
					&unstructured.Unstructured{},
					&unstructured.Unstructured{},
				},
			},
			isReady: false,
		},
		"node count > observed storageset count": {
			planner: &Planner{
				ObservedCStorClusterPlan: &types.CStorClusterPlan{
					Spec: types.CStorClusterPlanSpec{
						Nodes: []types.CStorClusterPlanNode{
							types.CStorClusterPlanNode{},
							types.CStorClusterPlanNode{},
						},
					},
				},
				ObservedStorageSets: []*unstructured.Unstructured{
					&unstructured.Unstructured{},
				},
			},
			isReady: false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := mock.planner.isReadyByNodeCount()
			if got != mock.isReady {
				t.Fatalf("Want %t got %t", mock.isReady, got)
			}
		})
	}
}

func TestPlannerIsReadyByNodeDiskCount(t *testing.T) {
	mockloginfo := &types.CStorClusterPlan{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "test",
		},
	}
	var tests = map[string]struct {
		planner *Planner
		isReady bool
	}{
		"desired disk count == observed disk count": {
			planner: &Planner{
				storageSetToDesiredDiskCount: map[string]resource.Quantity{
					"101": resource.MustParse("1"),
				},
				storageSetToObservedBlockDevices: map[string][]string{
					"101": []string{"bd1"},
				},
			},
			isReady: true,
		},
		"desired disk count > observed disk count": {
			planner: &Planner{
				// TODO (@amitkumardas):
				// 	Use log as a field in Planner
				ObservedCStorClusterPlan: mockloginfo,
				storageSetToDesiredDiskCount: map[string]resource.Quantity{
					"101": resource.MustParse("2"),
				},
				storageSetToObservedBlockDevices: map[string][]string{
					"101": []string{"bd1"},
				},
			},
			isReady: false,
		},
		"desired disk count < observed disk count": {
			planner: &Planner{
				storageSetToDesiredDiskCount: map[string]resource.Quantity{
					"101": resource.MustParse("2"),
				},
				storageSetToObservedBlockDevices: map[string][]string{
					"101": []string{"bd1", "bd2", "bd3"},
				},
			},
			isReady: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := mock.planner.isReadyByNodeDiskCount()
			if got != mock.isReady {
				t.Fatalf("Want %t got %t", mock.isReady, got)
			}
		})
	}
}
