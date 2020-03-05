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
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
				},
				Data: Data{
					BlockDeviceList: &unstructured.UnstructuredList{},
				},
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"invalid DataConfig": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 1,
						},
					},
				},
				Data: Data{
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
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"zero available block devices": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
				},
				Data: Data{
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
										"state":      string(types.BlockDeviceInactive),
									},
								},
							},
						},
					},
				},
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"missing node name in bd": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
				},
				Data: Data{
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
				},
			},
			response: make(map[string]types.CStorPoolClusterRecommendation),
		},
		"valid request and response": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
				},
				Data: Data{
					BlockDeviceList: &unstructured.UnstructuredList{
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
				},
			},
			response: map[string]types.CStorPoolClusterRecommendation{
				"HDD": {
					RequestSpec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
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
		"blockdevices count is less than group device count": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeRAIDZ,
							GroupDeviceCount: 3,
						},
					},
				},
				Data: Data{
					BlockDeviceList: &unstructured.UnstructuredList{
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
				},
			},
			response: map[string]types.CStorPoolClusterRecommendation{},
		},
		"poolCapacity is greater than max capacity in node": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
				},
				Data: Data{
					BlockDeviceList: &unstructured.UnstructuredList{
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
											"storage":            int64(5368709120),
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
											"storage":            int64(5368709120),
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
				},
			},
			response: map[string]types.CStorPoolClusterRecommendation{},
		},
		"poolCapacity is less than blockdevice capacity": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
				},
				Data: Data{
					BlockDeviceList: &unstructured.UnstructuredList{
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
											"storage":            int64(107374182400),
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
											"storage":            int64(107374182400),
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
				},
			},
			response: map[string]types.CStorPoolClusterRecommendation{
				"HDD": {
					RequestSpec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
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
		"multiple size suitable blockdevices": {
			request: cStorPoolClusterRecommendationRequest{
				Request: types.CStorPoolClusterRecommendationRequest{
					Spec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
						DataConfig: types.RaidGroupConfig{
							Type:             types.PoolRAIDTypeMirror,
							GroupDeviceCount: 2,
						},
					},
				},
				Data: Data{
					BlockDeviceList: &unstructured.UnstructuredList{
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
							{
								Object: map[string]interface{}{
									"apiVersion": "openebs.io/v1alpha1",
									"kind":       string(types.KindBlockDevice),
									"metadata": map[string]interface{}{
										"name":      "bd-3",
										"namespace": "openebs",
										"labels": map[string]interface{}{
											"kubernetes.io/hostname":  "node-1",
											"ndm.io/managed":          "false",
											"ndm.io/blockdevice-type": "blockdevice",
										},
									},
									"spec": map[string]interface{}{
										"capacity": map[string]interface{}{
											"storage":            int64(107374182400),
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
										"name":      "bd-4",
										"namespace": "openebs",
										"labels": map[string]interface{}{
											"kubernetes.io/hostname":  "node-1",
											"ndm.io/managed":          "false",
											"ndm.io/blockdevice-type": "blockdevice",
										},
									},
									"spec": map[string]interface{}{
										"capacity": map[string]interface{}{
											"storage":            int64(107374182400),
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
				},
			},
			response: map[string]types.CStorPoolClusterRecommendation{
				"HDD": {
					RequestSpec: types.CStorPoolClusterRecommendationRequestSpec{
						PoolCapacity: poolCapacity,
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
			if !reflect.DeepEqual(response, mock.response) {
				t.Fatalf("Expected [%+v] response got [%+v]", mock.response, response)
			}
		})
	}
}

func TestNewRequestForDevice(t *testing.T) {
	// zeroPoolCapacity, _ := resource.ParseQuantity(fmt.Sprintf("0"))
	poolCapacity, _ := resource.ParseQuantity(fmt.Sprintf("53687091200"))
	var tests = map[string]struct {
		request *types.CStorPoolClusterRecommendationRequest
		data    *Data
		isErr   bool
	}{
		"nil request": {
			request: nil,
			data:    &Data{},
			isErr:   true,
		},
		"nil PoolCapacity": {
			request: &types.CStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: resource.Quantity{},
				},
			},
			data:  &Data{},
			isErr: true,
		},
		"nil BlockDeviceList": {
			request: &types.CStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: poolCapacity,
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 2,
					},
				},
			},
			data: &Data{
				BlockDeviceList: nil,
			},
			isErr: true,
		},
		"invalid RaidConfig": {
			request: &types.CStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: poolCapacity,
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 0,
					},
				},
			},
			data: &Data{
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
			request: &types.CStorPoolClusterRecommendationRequest{
				Spec: types.CStorPoolClusterRecommendationRequestSpec{
					PoolCapacity: poolCapacity,
					DataConfig: types.RaidGroupConfig{
						Type:             types.PoolRAIDTypeMirror,
						GroupDeviceCount: 2,
					},
				},
			},
			data: &Data{
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
			_, err := NewRequestForDevice(mock.request, mock.data)
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}

}
