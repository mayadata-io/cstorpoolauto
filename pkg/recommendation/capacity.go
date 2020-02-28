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

	"github.com/pkg/errors"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	bdutil "mayadata.io/cstorpoolauto/util/blockdevice"
)

// capacityRecommendationRequest contains Block device list
// and raid group config. This is the request schema to get
// min and max pool capacity for a given raid type.
type capacityRecommendationRequest struct {
	BlockDeviceList unstructured.UnstructuredList
	RaidGroupConfig types.RaidGroupConfig
}

// CapacityRecommendation contains min capacity and max
// capacity both are in resource.Quantity format. This is
// the o/p of a min max request.
type CapacityRecommendation struct {
	MinCapacity resource.Quantity
	MaxCapacity resource.Quantity
}

// NewCapacityRequest builds a capacity request object and verify it.
// If there is any error during validation then it returns an error.
func NewCapacityRequest(
	raidConfig *types.RaidGroupConfig, blockDeviceList *unstructured.UnstructuredList) (
	*capacityRecommendationRequest, error) {
	if raidConfig == nil {
		return nil, errors.New(
			"Unable to create capacity recommendation request: Got nil raid config")
	}
	if blockDeviceList == nil {
		return nil, errors.New(
			"Unable to create capacity recommendation request: Got nil block device list")
	}

	err := raidConfig.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "Unable to create capacity recommendation request")
	}
	return &capacityRecommendationRequest{
		BlockDeviceList: *blockDeviceList,
		RaidGroupConfig: *raidConfig,
	}, nil
}

// GetRecommendation calculate min max recommendation and return it as a response.
func (r *capacityRecommendationRequest) GetRecommendation() map[string]CapacityRecommendation {
	resultMap := make(map[string]CapacityRecommendation)

	// If block device list is empty then return empty map
	if len(r.BlockDeviceList.Items) == 0 {
		return resultMap
	}

	// Pick the block devices which are eligible for cStor pool
	// After filtering if filtered block device list is empty
	// then return empty map in response.
	eligibleBlockDeviceList := unstructured.UnstructuredList{}
	eligibleBlockDeviceList.Object = r.BlockDeviceList.Object
	for _, bd := range r.BlockDeviceList.Items {
		isEligible, err := bdutil.IsEligibleForCStorPool(bd)
		if err == nil && isEligible {
			eligibleBlockDeviceList.Items = append(eligibleBlockDeviceList.Items, bd)
		}
	}
	if len(eligibleBlockDeviceList.Items) == 0 {
		return resultMap
	}

	// Make topology map using eligible block devices.
	// If topology is empty then return empty map in response.
	deviceTypeNodeBlockDeviceMap := bdutil.GetTopologyMapGroupByDeviceTypeAndBlockSize(eligibleBlockDeviceList)
	if len(deviceTypeNodeBlockDeviceMap) == 0 {
		return resultMap
	}

	// Get min max capacity for each type of block devices.
	for kind, nodeBlockDeviceListMap := range deviceTypeNodeBlockDeviceMap {

		// nodeCapacityDeviceCountMap contains block device capacity and count
		// for that capacity for each node (block device is present in that node)
		// After populating nodeBlockDeviceListMap with data this map will look
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
		nodeCapacityDeviceCountMap := nodeCapacityCount{}

		// Iterating over nodeBlockDeviceListMap map and populating
		// nodeCapacityDeviceCountMap
		for nodeName, blockDeviceList := range nodeBlockDeviceListMap {

			// capacityCountMap contains block device capacity and total count
			// of that capacity in one node.
			capacityCountMap := nodeCapacityDeviceCountMap.getOrDefault(nodeName)

			for _, blockDevice := range blockDeviceList {
				capacity, found := blockDevice.Capacity.AsInt64()
				if !found {
					// TODO handle this case
					continue
				}
				// get device count for one capacity and increase it by 1
				count := capacityCountMap.getOrDefault(capacity)
				capacityCountMap.update(capacity, count+1)
			}

			// update nodeCapacityDeviceCountMap for each node.
			nodeCapacityDeviceCountMap.update(nodeName, capacityCountMap)
		}

		// put min max result for all the types in a map.
		cr := nodeCapacityDeviceCountMap.getCapacityRecommendation(r.RaidGroupConfig)
		if !cr.MaxCapacity.IsZero() && !cr.MinCapacity.IsZero() {
			resultMap[kind] = cr
		}
	}

	return resultMap
}

// capacityCount is a typed map contains device capacity
// and count for that capacity
type capacityCount map[int64]int64

// getOrDefault returns count of a given capacity. If key not
// found then it adds that key in map and assign with 0
func (cc capacityCount) getOrDefault(key int64) int64 {
	value, found := cc[key]
	if !found {
		value = 0
		cc[key] = value
	}
	return value
}

// update takes key and value as argument and update that map
// with key value pair.
func (cc capacityCount) update(key, value int64) {
	cc[key] = value
}

// getCapacityRecommendation returns takes raidgroup config as an input and
// returns min and max capacity in resource.Quantity format
func (cc capacityCount) getCapacityRecommendation(
	raidConfig types.RaidGroupConfig) CapacityRecommendation {
	result := CapacityRecommendation{
		MinCapacity: resource.Quantity{},
		MaxCapacity: resource.Quantity{},
	}

	// Ideally caller will check raid group config is valid or not
	// If invalid raid config is passed then it will not return error.
	// It only logs the error and return empty struct
	if err := raidConfig.Validate(); err != nil {
		// TODO log the error.
		return result
	}

	for capacity, count := range cc {
		// If device count is less than group device count
		// then skip those block devices.
		if count < raidConfig.GroupDeviceCount {
			continue
		}

		// Get newMin capacity from block device capacity and count.
		// If min capacity in response 0 or greater than newMin then
		// update the min to newMin.
		newMin, err := resource.ParseQuantity(fmt.Sprintf("%d",
			capacity*raidConfig.GetDataDeviceCount()))
		if err != nil {
			// TODO log the error
			continue
		}
		if result.MaxCapacity.IsZero() || result.MinCapacity.Cmp(newMin) > 0 {
			result.MinCapacity = newMin
		}

		// noOfRaidGroup raid group count that can be formed.
		// ie - one node has 5 block devices of 100GB and pool type
		// is mirror then noOfRaidGroup will be 2.
		noOfRaidGroup := count / raidConfig.GroupDeviceCount

		// Get newMax capacity from block device capacity and count.
		// If max capacity in response 0 or less than newMax then
		// update the max to new max.
		newMax, err := resource.ParseQuantity(fmt.Sprintf("%d",
			capacity*noOfRaidGroup*raidConfig.GetDataDeviceCount()))
		if err != nil {
			// TODO log the error
			continue
		}
		if result.MaxCapacity.IsZero() || result.MaxCapacity.Cmp(newMax) < 0 {
			result.MaxCapacity = newMax
		}
	}

	return result
}

// nodeCapacityCount is a typed map contains node name and capacitycount
type nodeCapacityCount map[string]capacityCount

// getOrDefault returns capacityCount of a given node. If key not
// found then it adds that key in map and assign with default value
func (ncc nodeCapacityCount) getOrDefault(key string) capacityCount {
	value, found := ncc[key]
	if !found {
		value = capacityCount{}
		ncc[key] = value
	}
	return value
}

// update takes key and value as argument and update that map
// with key value pair.
func (ncc nodeCapacityCount) update(key string, value capacityCount) {
	ncc[key] = value
}

// getCapacityRecommendation returns takes raidgroup config as
// an input and returns CapacityRecommendation
func (ncc nodeCapacityCount) getCapacityRecommendation(
	raidConfig types.RaidGroupConfig) CapacityRecommendation {
	result := CapacityRecommendation{
		MinCapacity: resource.Quantity{},
		MaxCapacity: resource.Quantity{},
	}

	// Ideally caller will check raid group config is valid or not
	// If invalid raid config passed then it will not return error.
	// It only logs the error and return empty struct
	if err := raidConfig.Validate(); err != nil {
		// TODO log the error.
		return result
	}

	// check min max for all the nodes and calculate final CapacityRecommendation
	for _, cc := range ncc {
		cr := cc.getCapacityRecommendation(raidConfig)
		if cr.MaxCapacity.IsZero() || cr.MinCapacity.IsZero() {
			continue
		}

		// If min capacity in response 0 or greater than newMin then
		// update the min to newMin.
		if result.MinCapacity.IsZero() || result.MinCapacity.Cmp(cr.MinCapacity) > 0 {
			result.MinCapacity = cr.MinCapacity
		}

		// If max capacity in response 0 or less than newMax then
		// update the max to new max.
		if result.MaxCapacity.IsZero() || result.MaxCapacity.Cmp(cr.MaxCapacity) < 0 {
			result.MaxCapacity = cr.MaxCapacity
		}
	}

	return result
}
