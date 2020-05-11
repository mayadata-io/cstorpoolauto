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
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
)

// Helper exposes utility methods w.r.t BlockDevice unstructured
// instance
type Helper struct {
	CStorPoolCluster *unstructured.Unstructured

	allHostNames               []string
	hostNameToBlockDeviceNames map[string][]string
	err                        error
}

// NewHelper returns a new instance of CStorClusterConfig based helper
func NewHelper(obj *unstructured.Unstructured) *Helper {
	var err error
	if obj == nil {
		err = errors.Errorf("Can't init cspc helper: Nil obj")
	} else if obj.GetKind() != string(types.KindCStorPoolCluster) {
		err = errors.Errorf(
			"Can't init cspc helper: Want %q got %q",
			string(types.KindCStorPoolCluster), obj.GetKind(),
		)
	}
	if err != nil {
		return &Helper{
			err: err,
		}
	}
	return &Helper{
		CStorPoolCluster: obj,
	}
}

// GetOrderedHostNames returns all hostname(s) associated with
// this CStorPoolCluster unstructured instance in the same order
// they are found at observed CSPC specs
func (h *Helper) GetOrderedHostNames() ([]string, error) {
	if h.err != nil {
		return nil, h.err
	}
	var allHostNames []string
	// local function to get the host name
	getNodeName := func(obj *unstructured.Unstructured) error {
		hostname, err :=
			unstruct.GetStringOrError(
				obj, "spec", "nodeSelector", "kubernetes.io/hostname",
			)
		if err != nil {
			return err
		}
		allHostNames = append(allHostNames, hostname)
		return nil
	}
	// logic starts here
	pools, err := unstruct.GetSliceOrError(h.CStorPoolCluster, "spec", "pools")
	if err != nil {
		return nil, err
	}
	// Below will iterate through the pools & run following
	// getNodeName callback against each iterated pool
	err = unstruct.SliceIterator(pools).ForEach(getNodeName)
	if err != nil {
		return nil, err
	}
	h.allHostNames = allHostNames
	return h.allHostNames, nil
}

// GetOrderedHostNamesOrCached returns all hostname(s) associated
// with this CStorPoolCluster unstructured instance
func (h *Helper) GetOrderedHostNamesOrCached() ([]string, error) {
	if h.err != nil {
		return nil, h.err
	}
	if len(h.allHostNames) == 0 {
		// don't use the cache
		return h.GetOrderedHostNames()
	}
	// return the cached copy
	return h.allHostNames, nil
}

// GroupBlockDeviceNamesByHostName maps hostname to corresponding block
// device names & returns this mapping
func (h *Helper) GroupBlockDeviceNamesByHostName() (map[string][]string, error) {
	if h.err != nil {
		return nil, h.err
	}
	// currentNodeName holds the current node name of the
	// CStorPoolCluster nodes that are under iteration
	var currentNodeName string
	h.hostNameToBlockDeviceNames = map[string][]string{}

	// local function to get the host name
	getNodeName := func(obj *unstructured.Unstructured) (err error) {
		currentNodeName, err =
			unstruct.GetStringOrError(obj, "spec", "nodeSelector", "kubernetes.io/hostname")
		return
	}
	// local function to get the blockdevice name
	getBlockDeviceName := func(obj *unstructured.Unstructured) error {
		deviceName, err := unstruct.GetStringOrError(obj, "spec", "blockDeviceName")
		if err != nil {
			return err
		}
		// map the current iterated nodename with current iterated
		// block device name
		h.hostNameToBlockDeviceNames[currentNodeName] =
			append(h.hostNameToBlockDeviceNames[currentNodeName], deviceName)
		return nil
	}
	// local function to get blockdevices per raid group
	getBlockDevicesPerRAIDGroup := func(obj *unstructured.Unstructured) error {
		blockDevices, err := unstruct.GetSliceOrError(obj, "spec", "blockDevices")
		if err != nil {
			return err
		}
		// iterate through each blockdevice
		return unstruct.SliceIterator(blockDevices).ForEach(getBlockDeviceName)
	}
	// local function to get blockdevices per pool
	getBlockDevicesPerPool := func(obj *unstructured.Unstructured) error {
		raidGroups, err := unstruct.GetSliceOrError(obj, "spec", "raidGroups")
		if err != nil {
			return err
		}
		// iterate through each raidgroup
		return unstruct.SliceIterator(raidGroups).ForEach(getBlockDevicesPerRAIDGroup)
	}
	// logic starts here
	pools, err := unstruct.GetSliceOrError(h.CStorPoolCluster, "spec", "pools")
	if err != nil {
		return nil, err
	}
	// Below iteration will iterate pools & run following
	// callbacks against each iterated pool:
	//
	//	1/ getNodeName &
	// 	2/ getBlockDevicesPerPool
	//
	// NOTE:
	// 	One of the local functions maps the node name
	// against corresponding blockdevice names.
	err = unstruct.SliceIterator(pools).ForEach(
		getNodeName,
		getBlockDevicesPerPool,
	)
	if err != nil {
		return nil, err
	}
	return h.hostNameToBlockDeviceNames, nil
}

// GroupBlockDeviceNamesByHostNameOrCached maps hostname to
// corresponding block device names & returns this mapping
func (h *Helper) GroupBlockDeviceNamesByHostNameOrCached() (map[string][]string, error) {
	if h.err != nil {
		return nil, h.err
	}
	if len(h.hostNameToBlockDeviceNames) == 0 {
		// dont use the cache
		return h.GroupBlockDeviceNamesByHostName()
	}
	for name, devices := range h.hostNameToBlockDeviceNames {
		if name == "" || len(devices) == 0 {
			// dont use the cache
			return h.GroupBlockDeviceNamesByHostName()
		}
	}
	// return the cached copy
	return h.hostNameToBlockDeviceNames, nil
}
