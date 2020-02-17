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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	bdutil "mayadata.io/cstorpoolauto/util/blockdevice"
)

/*
This function builds a map using existing block devices. Block devices are
grouped by device kind ie - HDD or SSD and in one group all the block devices
are arranged by node name.

1. initial stage it will be a list of block devices.
[]blockdevice

2.
at last it will be converted into below map -
- HDD:
	- node-1 []blockdevice
	- node-2 []blockdevice
	- node-3 []blockdevice
- SSD:
	- node-1 []blockdevice
	- node-2 []blockdevice
	- node-3 []blockdevice

3. In some case all the may not have block device or all the
types of block device. For that this may look like this -
- HDD:
	- node-1 []blockdevice
	- node-3 []blockdevice
	- node-4 []blockdevice
	- node-7 []blockdevice
- SSD:
	- node-3 []blockdevice
	- node-5 []blockdevice
	- node-7 []blockdevice
*/
func buildNodeBlockDeviceMap(
	bdlist unstructured.UnstructuredList) map[string]map[string][]BlockDeviceInfo {
	deviceTypeNodeBlockDeviceMap := make(map[string]map[string][]BlockDeviceInfo)

	for _, bd := range bdlist.Items {

		// If block device is not in active state or we got any error while
		// fetching block device status then we can not use that block device
		// to create a pool.
		if ok, err := bdutil.IsActiveOrError(bd); err != nil || !ok {
			continue
		}

		// If claim status block device is 'Claimed' or we got any error while
		// fetching claim status of block device then we can not use that block
		// device to create a pool.
		if ok, err := bdutil.IsUnclaimedOrError(bd); err != nil || !ok {
			continue
		}

		// If file system present in a block device or we got any error while
		// fetching file system details of block device then we can not use that
		// block device to create a pool.
		if ok, err := bdutil.HasFileSystemOrError(bd); err != nil || ok {
			continue
		}

		// Block device should be associated with a node if mode name is missing or
		// we got any error during fatching node name then we can not use that
		// block device to create a pool.
		nodeName, err := bdutil.GetNodeNameOrError(bd)
		if err != nil {
			continue
		}

		// Host name property is a must for Block device. If host name is missing or
		// we got any error during fatching host name then we can not use that
		// block device to create a pool.
		hostName, err := bdutil.GetHostNameOrError(bd)
		if err != nil {
			continue
		}

		// Capacity of a block device is a must to create a recommendation. If capacity
		// is missing or we got any error during fatching capacity then we can not use
		// that block device to create a recommendation.
		capacity, err := bdutil.GetCapacityOrError(bd)
		if err != nil {
			continue
		}

		// Device type of a block device is a must to create a recommendation. If
		// device type is missing or we got any error during fatching device type
		// then we can not use it to create a recommendation.
		deviceType, err := bdutil.GetDeviceTypeOrError(bd)
		if err != nil {
			continue
		}

		// nodeBlockDeviceMap contains block devices grouped by node
		// If for any device type (HDD OR SSD) this map is not present
		// then create it.
		nodeBlockDeviceMap, ok := deviceTypeNodeBlockDeviceMap[deviceType]
		if !ok {
			nodeBlockDeviceMap = make(map[string][]BlockDeviceInfo)
		}

		// bdinfo contains some metadata of a block device with it's identity.
		bdinfo := BlockDeviceInfo{
			NodeName: nodeName,
			HostName: hostName,
			Capacity: &capacity,
			Identity: &types.Reference{
				Name:       bd.GetName(),
				Namespace:  bd.GetNamespace(),
				Kind:       bd.GetKind(),
				APIVersion: bd.GetAPIVersion(),
				UID:        bd.GetUID(),
			},
		}

		// blockDeviceList contains block devices associated with a node.
		// If for any node this list is not present then create it.
		blockDeviceList, ok := nodeBlockDeviceMap[hostName]
		if !ok {
			blockDeviceList = make([]BlockDeviceInfo, 0)
		}
		blockDeviceList = append(blockDeviceList, bdinfo)

		// update block device list for a node
		nodeBlockDeviceMap[hostName] = blockDeviceList

		// update node block device map for a given block device type
		deviceTypeNodeBlockDeviceMap[deviceType] = nodeBlockDeviceMap
	}

	return deviceTypeNodeBlockDeviceMap
}
