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
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
)

// NewMinMaxCapacityRequest returns an empty object
// of MinMaxCapacityRequest.
func NewMinMaxCapacityRequest() *MinMaxCapacityRequest {
	return &MinMaxCapacityRequest{}
}

// WithBlockDeviceList adds block device list
// in MinMaxCapacityRequest object.
func (r *MinMaxCapacityRequest) WithBlockDeviceList(
	blockDeviceList *unstructured.UnstructuredList) *MinMaxCapacityRequest {
	r.BlockDeviceList = blockDeviceList
	return r
}

// WithRaidGroupConfig adds raid group configuration
// in MinMaxCapacityRequest object.
func (r *MinMaxCapacityRequest) WithRaidGroupConfig(
	raidGroupConfig *types.RaidGroupConfig) *MinMaxCapacityRequest {
	r.RaidGroupConfig = raidGroupConfig
	return r
}

// BuildAndVerifyOrError builds a min max request object and verify it
// If there is any error during validation then it returns an error.
func (r *MinMaxCapacityRequest) BuildAndVerifyOrError() (
	*MinMaxCapacityRequest, error) {
	if r.RaidGroupConfig == nil {
		r.RaidGroupConfig = types.GetDefaultRaidGroupConfig()
	}
	err := r.RaidGroupConfig.Validate()
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Get calculate min max and return it as a response.
func (r *MinMaxCapacityRequest) Get() MinMaxCapacity {
	deviceTypeNodeBlockDeviceMap := buildNodeBlockDeviceMap(*r.BlockDeviceList)
	resultMap := make(map[string]*MinMaxCapacity)

	deviceKind := []string{DeviceKindSSD, DeviceKindHDD}

	// Get min max capacity for each type of block devices.
	for _, kind := range deviceKind {

		// Check nodeBlockDeviceListMap present for a device type not
		// if not then skip fetching min max for that kind.
		nodeBlockDeviceListMap, ok := deviceTypeNodeBlockDeviceMap[kind]
		if !ok {
			continue
		}

		// nodeCapacityDeviceCountMap contains block device capacity and count
		// for that capacity for each node (block device is present in that node)
		// After populating nodeBlockDeviceListMap with data this map with looks
		// like this -
		// Node-1
		//     - [100GB, 4]
		//     - [500GB, 6]
		//     - [1TB, 4]
		// Node-2
		//     - [100GB, 1]
		//     - [500GB, 8]
		//     - [1TB, 2]
		// Node-3
		//     - [500GB, 5]
		//     - [1TB, 5]
		// Node-4
		//     - [100GB, 8]
		//     - [500GB, 3]
		nodeCapacityDeviceCountMap := make(map[string]map[int64]int64)

		// Iterating over nodeBlockDeviceListMap map and populating
		// nodeCapacityDeviceCountMap
		for nodeName, blockDeviceList := range nodeBlockDeviceListMap {

			// If capacity count map not found for a node then create it.
			capacityCountMap, capacityCountMapFound := nodeCapacityDeviceCountMap[nodeName]
			if !capacityCountMapFound {
				capacityCountMap = make(map[int64]int64)
			}

			// Iterate over blockDeviceList and populate
			for _, blockDevice := range blockDeviceList {

				// find capacity for each block device nodeBlockDeviceListMap
				capacity, capacityFound := blockDevice.Capacity.AsInt64()
				if !capacityFound {
					// TODO handle this case
					continue
				}

				// In capacity device count map if entry not found for any capacity then
				// make it 0. If found then increase by 1.
				_, ok := capacityCountMap[capacity]
				if !ok {
					capacityCountMap[capacity] = 0
				}
				capacityCountMap[capacity]++
			}

			// update nodeCapacityDeviceCountMap for each node.
			nodeCapacityDeviceCountMap[nodeName] = capacityCountMap
		}

		result := &MinMaxCapacity{
			MinCapacity: resource.Quantity{},
			MaxCapacity: resource.Quantity{},
		}

		// Iterate over nodeCapacityDeviceCountMap and get min and max.
		for _, capacityDeviceCountMap := range nodeCapacityDeviceCountMap {
			for capacity, deviceCount := range capacityDeviceCountMap {

				// If device count is less than group device count
				// then skip those block devices.
				if deviceCount < r.RaidGroupConfig.GroupDeviceCount {
					continue
				}

				// effectiveBlockDeviceCount is round up block device count.
				// ie - one node has 5 block devices of 100GB and pool type
				// is mirror then effective block device count will be 4.
				effectiveBlockDeviceCount := deviceCount / r.RaidGroupConfig.GroupDeviceCount

				// Get min capacity from block device capacity and count.
				// If min capacity in response 0 or greater than new min
				// then update it.
				min, err := resource.ParseQuantity(fmt.Sprintf("%d",
					(capacity * r.RaidGroupConfig.GetDataDeviceCount())))
				if err != nil {
					continue
				}
				if result.MinCapacity.IsZero() || result.MinCapacity.Cmp(min) > 0 {
					result.MinCapacity = min
				}

				// Get max capacity from block device capacity and count.
				// If max capacity in response 0 or less than new max then
				// update it.
				max, err := resource.ParseQuantity(fmt.Sprintf("%d",
					(capacity * (effectiveBlockDeviceCount / r.RaidGroupConfig.GroupDeviceCount) *
						r.RaidGroupConfig.GroupDeviceCount)))
				if err != nil {
					continue
				}
				if result.MaxCapacity.IsZero() || result.MaxCapacity.Cmp(max) < 0 {
					result.MaxCapacity = max
				}
			}
		}

		// put min max result for all the types in a map.
		resultMap[kind] = result
	}

	response := &MinMaxCapacity{
		MinCapacity: resource.Quantity{},
		MaxCapacity: resource.Quantity{},
	}

	// Iterate over min max capacity map for each type of
	// block device and get final min max.
	for _, resultForOneType := range resultMap {
		if response.MinCapacity.IsZero() ||
			response.MinCapacity.Cmp(resultForOneType.MinCapacity) > 0 {
			response.MinCapacity = resultForOneType.MinCapacity
		}
		if response.MaxCapacity.IsZero() ||
			response.MaxCapacity.Cmp(resultForOneType.MaxCapacity) < 0 {
			response.MaxCapacity = resultForOneType.MaxCapacity
		}
	}
	return *response

}
