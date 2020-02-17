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

package recommendation

import (
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
)

const (
	// DeviceKindSSD represents device kind of SSD devices.
	DeviceKindSSD string = "SSD"
	// DeviceKindHDD represents device kind of HDD devices.
	DeviceKindHDD string = "HDD"
)

// BlockDeviceInfo contains identity of block device
// and some metadata for a block device.
type BlockDeviceInfo struct {
	NodeName string
	HostName string
	Identity *types.Reference
	Capacity *resource.Quantity
}

// MinMaxCapacityRequest contains Block device list
// and raid group config. This is the request schema
// to get min and max pool capacity.
type MinMaxCapacityRequest struct {
	BlockDeviceList *unstructured.UnstructuredList
	RaidGroupConfig *types.RaidGroupConfig
}

// MinMaxCapacity contains min capacity and max capacity
// both are in resource.Quantity format. This is the o/p
// of a min max request.
type MinMaxCapacity struct {
	MinCapacity resource.Quantity
	MaxCapacity resource.Quantity
}
