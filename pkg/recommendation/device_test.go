package recommendation

import (
	"fmt"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
)

func TestGetRecommendation(t *testing.T) {
	poolCapacity, _ := resource.ParseQuantity(fmt.Sprintf("53687091200"))
	var tests = map[string]struct {
		request  cStorPoolClusterRecommendationRequest
		response map[string]types.CStorPoolClusterRecommendation
		isErr    bool
	}{
		"empty blockdevice list": {
			request: cStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity:    poolCapacity,
					BlockDeviceList: unstructured.UnstructuredList{},
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 2,
					},
				},
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"invalid DataConfig": {
			request: cStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: poolCapacity,
					BlockDeviceList: unstructured.UnstructuredList{
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"apiVersion": "openebs.io/v1alpha1",
									"kind":       string(types.KindBlockDevice),
									"metadata": map[string]interface{}{
										"name": "bd-1",
										"labels": map[string]interface{}{
											"kubernetes.io/hostname":  "node-1",
											"ndm.io/managed":          "false",
											"ndm.io/blockdevice-type": "blockdevice",
										},
									},
									"spec": map[string]interface{}{
										"capacity": map[string]interface{}{
											"storage":            int64(53687091200),
											"physicalSectorSize": int32(512),
											"logicalSectorSize":  int32(512),
										},
										"details": map[string]interface{}{
											"deviceType": "",
										},
										"nodeAttributes": map[string]interface{}{
											"nodeName": "node-1",
										},
										"filesystem": map[string]interface{}{},
									},
									"status": map[string]interface{}{
										"claimState": string("Unclaimed"),
										"state":      string(types.BlockDeviceActive),
									},
								},
							},
						},
					},
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 1,
					},
				},
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"zero available block devices": {
			request: cStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: poolCapacity,
					BlockDeviceList: unstructured.UnstructuredList{
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"apiVersion": "openebs.io/v1alpha1",
									"kind":       string(types.KindBlockDevice),
									"metadata": map[string]interface{}{
										"name": "bd-1",
										"labels": map[string]interface{}{
											"kubernetes.io/hostname":  "node-1",
											"ndm.io/managed":          "false",
											"ndm.io/blockdevice-type": "blockdevice",
										},
									},
									"spec": map[string]interface{}{
										"capacity": map[string]interface{}{
											"storage":            int64(53687091200),
											"physicalSectorSize": int32(512),
											"logicalSectorSize":  int32(512),
										},
										"details": map[string]interface{}{
											"deviceType": "",
										},
										"nodeAttributes": map[string]interface{}{
											"nodeName": "node-1",
										},
										"filesystem": map[string]interface{}{},
									},
									"status": map[string]interface{}{
										"claimState": string("Unclaimed"),
										"state":      string(types.BlockDeviceInactive),
									},
								},
							},
						},
					},
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 2,
					},
				},
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"missing node name in bd": {
			request: cStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: poolCapacity,
					BlockDeviceList: unstructured.UnstructuredList{
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"apiVersion": "openebs.io/v1alpha1",
									"kind":       string(types.KindBlockDevice),
									"metadata": map[string]interface{}{
										"name": "bd-1",
										"labels": map[string]interface{}{
											"kubernetes.io/hostname":  "node-1",
											"ndm.io/managed":          "false",
											"ndm.io/blockdevice-type": "blockdevice",
										},
									},
									"spec": map[string]interface{}{
										"capacity": map[string]interface{}{
											"storage":            int64(53687091200),
											"physicalSectorSize": int32(512),
											"logicalSectorSize":  int32(512),
										},
										"details": map[string]interface{}{
											"deviceType": "",
										},
										"nodeAttributes": map[string]interface{}{},
										"filesystem":     map[string]interface{}{},
									},
									"status": map[string]interface{}{
										"claimState": string("Unclaimed"),
										"state":      string(types.BlockDeviceActive),
									},
								},
							},
						},
					},
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 2,
					},
				},
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"valid request and response": {
			request: cStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: poolCapacity,
					BlockDeviceList: unstructured.UnstructuredList{
						Items: []unstructured.Unstructured{
							{
								Object: map[string]interface{}{
									"apiVersion": "openebs.io/v1alpha1",
									"kind":       string(types.KindBlockDevice),
									"metadata": map[string]interface{}{
										"name":      "bd-1",
										"namespace": "openebs",
										"labels": map[string]interface{}{
											"kubernetes.io/hostname":  "node-1",
											"ndm.io/managed":          "false",
											"ndm.io/blockdevice-type": "blockdevice",
										},
									},
									"spec": map[string]interface{}{
										"capacity": map[string]interface{}{
											"storage":            int64(53687091200),
											"physicalSectorSize": int32(512),
											"logicalSectorSize":  int32(512),
										},
										"details": map[string]interface{}{
											"deviceType": "HDD",
										},
										"nodeAttributes": map[string]interface{}{
											"nodeName": "node-1",
										},
										"filesystem": map[string]interface{}{},
									},
									"status": map[string]interface{}{
										"claimState": string("Unclaimed"),
										"state":      string(types.BlockDeviceActive),
									},
								},
							},
							{
								Object: map[string]interface{}{
									"apiVersion": "openebs.io/v1alpha1",
									"kind":       string(types.KindBlockDevice),
									"metadata": map[string]interface{}{
										"name":      "bd-2",
										"namespace": "openebs",
										"labels": map[string]interface{}{
											"kubernetes.io/hostname":  "node-1",
											"ndm.io/managed":          "false",
											"ndm.io/blockdevice-type": "blockdevice",
										},
									},
									"spec": map[string]interface{}{
										"capacity": map[string]interface{}{
											"storage":            int64(53687091200),
											"physicalSectorSize": int32(512),
											"logicalSectorSize":  int32(512),
										},
										"details": map[string]interface{}{
											"deviceType": "HDD",
										},
										"nodeAttributes": map[string]interface{}{
											"nodeName": "node-1",
										},
										"filesystem": map[string]interface{}{},
									},
									"status": map[string]interface{}{
										"claimState": string("Unclaimed"),
										"state":      string(types.BlockDeviceActive),
									},
								},
							},
						},
					},
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 2,
					},
				},
			},
			response: map[string]types.CStorPoolClusterRecommendation{
				"HDD": {
					RequestSpec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						BlockDeviceList: unstructured.UnstructuredList{
							Items: []unstructured.Unstructured{
								{
									Object: map[string]interface{}{
										"apiVersion": "openebs.io/v1alpha1",
										"kind":       string(types.KindBlockDevice),
										"metadata": map[string]interface{}{
											"name":      "bd-1",
											"namespace": "openebs",
											"labels": map[string]interface{}{
												"kubernetes.io/hostname":  "node-1",
												"ndm.io/managed":          "false",
												"ndm.io/blockdevice-type": "blockdevice",
											},
										},
										"spec": map[string]interface{}{
											"capacity": map[string]interface{}{
												"storage":            int64(53687091200),
												"physicalSectorSize": int32(512),
												"logicalSectorSize":  int32(512),
											},
											"details": map[string]interface{}{
												"deviceType": "HDD",
											},
											"nodeAttributes": map[string]interface{}{
												"nodeName": "node-1",
											},
											"filesystem": map[string]interface{}{},
										},
										"status": map[string]interface{}{
											"claimState": string("Unclaimed"),
											"state":      string(types.BlockDeviceActive),
										},
									},
								},
								{
									Object: map[string]interface{}{
										"apiVersion": "openebs.io/v1alpha1",
										"kind":       string(types.KindBlockDevice),
										"metadata": map[string]interface{}{
											"name":      "bd-2",
											"namespace": "openebs",
											"labels": map[string]interface{}{
												"kubernetes.io/hostname":  "node-1",
												"ndm.io/managed":          "false",
												"ndm.io/blockdevice-type": "blockdevice",
											},
										},
										"spec": map[string]interface{}{
											"capacity": map[string]interface{}{
												"storage":            int64(53687091200),
												"physicalSectorSize": int32(512),
												"logicalSectorSize":  int32(512),
											},
											"details": map[string]interface{}{
												"deviceType": "HDD",
											},
											"nodeAttributes": map[string]interface{}{
												"nodeName": "node-1",
											},
											"filesystem": map[string]interface{}{},
										},
										"status": map[string]interface{}{
											"claimState": string("Unclaimed"),
											"state":      string(types.BlockDeviceActive),
										},
									},
								},
							},
						},
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
					Spec: types.CStorPoolClusterRecommendationSpec{
						PoolInstances: []types.PoolInstanceConfig{
							{
								Node: types.Reference{
									Name: "node-1",
								},
								Capacity: poolCapacity,
								BlockDevices: types.BlockDeviceTopology{
									DataDevices: []types.Reference{
										{
											Name:       "bd-1",
											Namespace:  "openebs",
											Kind:       "BlockDevice",
											APIVersion: "openebs.io/v1alpha1",
											UID:        "",
										},
										{
											Name:       "bd-2",
											Namespace:  "openebs",
											Kind:       "BlockDevice",
											APIVersion: "openebs.io/v1alpha1",
											UID:        "",
										},
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
		t.Run(name, func(t *testing.T) {
			response := mock.request.GetRecommendation()
			// if name == "invalid DataConfig" {
			// 	t.Fatalf("Response - [%+v] - [%+v]\n", response, mock.request)
			// }
			if !reflect.DeepEqual(response, mock.response) {
				t.Fatalf("Expected [%+v] response got [%+v]", mock.response, response)
			}
		})
	}
}

func TestNewRequestForDevice(t *testing.T) {
	poolCapacity, _ := resource.ParseQuantity(fmt.Sprintf("53687091200"))
	var tests = map[string]struct {
		src   Data
		isErr bool
	}{
		"nil RaidConfig": {
			src: Data{
				RaidConfig: nil,
			},
			isErr: true,
		},
		"nil PoolCapacity": {
			src: Data{
				RaidConfig: &types.RaidGroupConfig{
					Type:             types.PoolRAIDTypeMirror,
					GroupDeviceCount: 2,
				},
				PoolCapacity: nil,
			},
			isErr: true,
		},
		"nil BlockDeviceList": {
			src: Data{
				RaidConfig: &types.RaidGroupConfig{
					Type:             types.PoolRAIDTypeMirror,
					GroupDeviceCount: 2,
				},
				PoolCapacity:    &poolCapacity,
				BlockDeviceList: nil,
			},
			isErr: true,
		},
		"invalid RaidConfig": {
			src: Data{
				RaidConfig: &types.RaidGroupConfig{
					Type:             types.PoolRAIDTypeMirror,
					GroupDeviceCount: 0,
				},
				PoolCapacity: &poolCapacity,
				BlockDeviceList: &unstructured.UnstructuredList{
					Items: []unstructured.Unstructured{
						{
							Object: map[string]interface{}{
								"apiVersion": "openebs.io/v1alpha1",
								"kind":       string(types.KindBlockDevice),
								"metadata": map[string]interface{}{
									"name": "bd-1",
									"labels": map[string]interface{}{
										"kubernetes.io/hostname":  "node-1",
										"ndm.io/managed":          "false",
										"ndm.io/blockdevice-type": "blockdevice",
									},
								},
								"spec": map[string]interface{}{
									"capacity": map[string]interface{}{
										"storage":            int64(53687091200),
										"physicalSectorSize": int32(512),
										"logicalSectorSize":  int32(512),
									},
									"details": map[string]interface{}{
										"deviceType": "",
									},
									"nodeAttributes": map[string]interface{}{
										"nodeName": "node-1",
									},
									"filesystem": map[string]interface{}{},
								},
								"status": map[string]interface{}{
									"claimState": string("Unclaimed"),
									"state":      string(types.BlockDeviceActive),
								},
							},
						},
					},
				},
			},
			isErr: true,
		},
		"valid case": {
			src: Data{
				RaidConfig: &types.RaidGroupConfig{
					Type:             types.PoolRAIDTypeMirror,
					GroupDeviceCount: 2,
				},
				PoolCapacity: &poolCapacity,
				BlockDeviceList: &unstructured.UnstructuredList{
					Items: []unstructured.Unstructured{
						{
							Object: map[string]interface{}{
								"apiVersion": "openebs.io/v1alpha1",
								"kind":       string(types.KindBlockDevice),
								"metadata": map[string]interface{}{
									"name": "bd-1",
									"labels": map[string]interface{}{
										"kubernetes.io/hostname":  "node-1",
										"ndm.io/managed":          "false",
										"ndm.io/blockdevice-type": "blockdevice",
									},
								},
								"spec": map[string]interface{}{
									"capacity": map[string]interface{}{
										"storage":            int64(53687091200),
										"physicalSectorSize": int32(512),
										"logicalSectorSize":  int32(512),
									},
									"details": map[string]interface{}{
										"deviceType": "",
									},
									"nodeAttributes": map[string]interface{}{
										"nodeName": "node-1",
									},
									"filesystem": map[string]interface{}{},
								},
								"status": map[string]interface{}{
									"claimState": string("Unclaimed"),
									"state":      string(types.BlockDeviceActive),
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
		t.Run(name, func(t *testing.T) {
			_, err := NewRequestForDevice(mock.src)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}

}
