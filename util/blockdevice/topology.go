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

package blockdevice

import (
	"fmt"

	"github.com/golang/glog"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
)

const (
	// DriveTypeSSD represents SSD drive devices.
	DriveTypeSSD string = "SSD"
	// DriveTypeHDD represents HDD drive devices.
	DriveTypeHDD string = "HDD"
	// DriveTypeUnKnown represents unknown drive type.
	DriveTypeUnKnown string = "Unknown"
	// DeviceTypeUnKnown represents unknown device type.
	DeviceTypeUnKnown string = "Unknown"
)

// MetaInfo contains identity of block device and some
// meta information for a block device.
type MetaInfo struct {
	HostName string
	Identity *types.Reference
	Capacity *resource.Quantity
}

/*
GetTopologyMapGroupByDeviceTypeAndBlockSize function builds a map using
list of block devices. Block devices are grouped by device kind and blocksize
ie - disk-HDD, disk-SSD-16384, disk-HDD-4096, disk-HDD-512 or disk-SSD,
disk-SSD-4096, disk-SSD-16384 and in one group all the block devices are arranged
by node name.

Top level key is contains DeviceType-DriveType-PhysicalSectorSize

1. initial stage it will be a list of block devices.
[]blockdevice

2. At last it will be converted into below map -

- disk-HDD:
	- node-1 []blockdevice
	- node-3 []blockdevice
- disk-HDD-4096:
	- node-1 []blockdevice
	- node-2 []blockdevice
- disk-HDD-16384:
	- node-2 []blockdevice
	- node-3 []blockdevice
- disk-SSD:
	- node-1 []blockdevice
	- node-2 []blockdevice
- disk-SSD-4096:
	- node-1 []blockdevice
	- node-2 []blockdevice
- disk-SSD-16384:
	- node-1 []blockdevice
	- node-3 []blockdevice

*/
func GetTopologyMapGroupByDeviceTypeAndBlockSize(
	bdList unstructured.UnstructuredList) map[string]map[string][]MetaInfo {
	deviceTypeNodeBlockDeviceMap := make(map[string]map[string][]MetaInfo)

	for _, bd := range bdList.Items {

		// Host device should be associated with a node if host name is missing or
		// we got any error during fetching host name then we can not use that to
		// create topology map.
		hostName, err := GetHostNameOrError(bd)
		if err != nil {
			glog.Warningf("Block device %s will not consider for recommendation : host name not present",
				bd.GetName())
			continue
		}

		// If capacity is missing or we got any error during fetching capacity then
		// we can not use that block device to create topology map.
		capacity, err := GetCapacity(bd)
		if err != nil {
			glog.Warningf("Block device %s will not consider for recommendation : capacity not present",
				bd.GetName())
			continue
		}

		// Device type represents block device type ie - (sparse, disk, partition, lvm, raid)
		// If device type is empty or we got any error during fetching this then
		// add that block device in unknown group.
		deviceType, err := GetDeviceType(bd)
		if err != nil {
			glog.Warningf("Block device %s will not consider for recommendation : device type not present",
				bd.GetName())
			continue
		}
		// If deviceType is empty then add it to unknown device type group
		if deviceType == "" {
			deviceType = DeviceTypeUnKnown
		}

		// Drive type represents block device drive type ie - (HDD, SSD)
		// If drive type is empty or we got any error during fetching this then
		// add that block device in unknown group.
		driveType, err := GetDriveType(bd)
		if err != nil {
			glog.Warningf("Block device %s will not consider for recommendation : drive type not present",
				bd.GetName())
			continue
		}
		// If deviceType is empty then add it to unknown device type group
		if driveType == "" {
			driveType = DriveTypeUnKnown
		}

		deviceType = deviceType + "-" + driveType

		// metaInfo contains some metadata of a block device with it's identity.
		metaInfo := MetaInfo{
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

		// deviceTypeList contains top level keys in this topology
		// ie - disk-HDD, disk-SSD-16384, disk-HDD-4096, disk-HDD-512
		// or disk-SSD, disk-SSD-4096, disk-SSD-16384
		deviceTypeList := make([]string, 0)
		deviceTypeList = append(deviceTypeList, deviceType)

		// get the physical block size if error is nil and it is not zero
		// then add a top level key for it.
		physicalBlockSize, err := GetPhysicalSectorSize(bd)
		if err == nil && !physicalBlockSize.IsZero() {
			deviceTypeList = append(deviceTypeList, fmt.Sprintf("%s-%s", deviceType, physicalBlockSize.String()))
		}

		// For all the top level key update the map.
		for _, bdType := range deviceTypeList {

			// nodeBlockDeviceMap contains block devices grouped by node.
			// If for any device type (disk-HDD, disk-SSD-16384, disk-HDD-4096,
			// disk-HDD-512 or disk-SSD, disk-SSD-4096, disk-SSD-16384) this
			// map is not present then create it.
			nodeBlockDeviceMap, ok := deviceTypeNodeBlockDeviceMap[bdType]
			if !ok {
				nodeBlockDeviceMap = make(map[string][]MetaInfo)
			}

			// blockDeviceList contains block devices associated with a node.
			// If for any node this list is not present then create it.
			blockDeviceList, ok := nodeBlockDeviceMap[hostName]
			if !ok {
				blockDeviceList = make([]MetaInfo, 0)
			}
			blockDeviceList = append(blockDeviceList, metaInfo)

			// update block device list for a node
			nodeBlockDeviceMap[hostName] = blockDeviceList

			// update node block device map for a given block device type
			deviceTypeNodeBlockDeviceMap[bdType] = nodeBlockDeviceMap

		}
	}

	return deviceTypeNodeBlockDeviceMap
}
