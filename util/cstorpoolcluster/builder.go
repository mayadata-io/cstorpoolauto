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
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	stringutil "mayadata.io/cstorpoolauto/util/string"
)

// Builder helps building an unstructured instance of
// kind CStorPoolCluster
type Builder struct {
	// Name of the desired CStorPoolCluster
	Name string

	// Namespace of the desired CStorPoolCluster
	Namespace string

	// OrderedHostNames represent an ordered list of host names
	// that will be used to build a CStorPoolCluster instance
	OrderedHostNames []string

	// mapping of host name to block device names that are currently
	// **observed** _(in existing CStorPoolCluster)_
	HostNameToObservedDeviceNames map[string][]string

	// mapping of host name to block device names that are **desired**
	// _(present as available BlockDevices)_ & should participate in
	// CStorPoolCluster formation
	HostNameToDesiredDeviceNames map[string][]string

	// annotations that should be used during building the desired
	// CStorPoolCluster
	DesiredAnnotations map[string]string

	// labels that should be used during building the desired
	// CStorPoolCluster
	DesiredLabels map[string]string

	// RAID type that should be used to form the desired
	// CStorPoolCluster
	DesiredRAIDType types.PoolRAIDType

	// ordered and eligible hosts that will participate in formation
	// of CStorPoolCluster
	desiredOrderedHostNames []string

	// maps the host name to devices by taking into consideration
	// desired host names, observed devices & desired devices
	hostNameToFinalDeviceNames map[string][]string

	err error
}

// BuildOption is a functional approach to set various fields of
// Builder instance
type BuildOption func(*Builder) error

// NewBuilder returns a new instance of Builder
func NewBuilder(options ...BuildOption) *Builder {
	b := &Builder{
		HostNameToObservedDeviceNames: map[string][]string{},
		HostNameToDesiredDeviceNames:  map[string][]string{},
	}
	for _, opt := range options {
		err := opt(b)
		if err != nil {
			b.err = err
			break
		}
	}
	if b.err != nil {
		return b
	}
	return b
}

func (b *Builder) setDefaultsIfNotSet() {
	if b.HostNameToObservedDeviceNames == nil {
		b.HostNameToObservedDeviceNames = map[string][]string{}
	}
	if b.HostNameToDesiredDeviceNames == nil {
		b.HostNameToDesiredDeviceNames = map[string][]string{}
	}
	b.setOrderedHostNamesFromDesiredIfNotSet()
	b.setDesiredOrderedHostNamesIfNotSet()
	b.mapHostNameToFinalDeviceNamesIfNotSet()
}

func (b *Builder) setOrderedHostNamesFromDesiredIfNotSet() {
	if len(b.OrderedHostNames) != 0 {
		// nothing to do since it is already set
		return
	}
	// build the desired hostnames that follows
	// HostNameToDesiredDeviceNames
	//
	// NOTE:
	//	Order is not guaranteed to be same everytime
	// since this slice is populated from a map
	for hostname := range b.HostNameToDesiredDeviceNames {
		b.OrderedHostNames = append(b.OrderedHostNames, hostname)
	}
}

func (b *Builder) setDesiredOrderedHostNamesIfNotSet() {
	if len(b.desiredOrderedHostNames) != 0 {
		// nothing to do since it is already set
		return
	}
	for _, hostName := range b.OrderedHostNames {
		if len(b.HostNameToDesiredDeviceNames[hostName]) == 0 {
			// ignore this hostname if it is no more desired
			// i.e. ignore this host if there there are no
			// corresponding devices
			continue
		}
		b.desiredOrderedHostNames =
			append(b.desiredOrderedHostNames, hostName)
	}
}

func (b *Builder) mapHostNameToFinalDeviceNamesIfNotSet() {
	if len(b.hostNameToFinalDeviceNames) != 0 {
		return
	}
	b.hostNameToFinalDeviceNames = map[string][]string{}
	for hostName, desiredDeviceNames := range b.HostNameToDesiredDeviceNames {
		if len(desiredDeviceNames) == 0 {
			// no need to build the final device names against this host
			// if there are no corresponding desired devices
			continue
		}
		observedDeviceNames := b.HostNameToObservedDeviceNames[hostName]
		// merge desired against the observed device names by keeping the order
		// of observed device names
		//
		// NOTE:
		//	This is very important logic that can reduce the disruptions to a pool.
		// This is handled by placing the blockdevice name(s) at their old position(s).
		b.hostNameToFinalDeviceNames[hostName] =
			stringutil.NewEquality(observedDeviceNames, desiredDeviceNames).Merge()
	}
}

func (b *Builder) validate() {
	// simple validations
	if b.Name == "" {
		b.err = errors.Errorf("Can't build desired CStorPoolCluster: Missing name")
		return
	}
	if b.Namespace == "" {
		b.err = errors.Errorf("Can't build desired CStorPoolCluster: Missing namespace")
		return
	}
	if b.DesiredRAIDType == "" {
		b.err = errors.Errorf("Can't build desired CStorPoolCluster: Missing raid type")
		return
	}
	b.validateDiskCount()
}

func (b *Builder) validateDiskCount() {
	var errMsgs []string
	diskCountByRAIDType :=
		int(types.RAIDTypeToDefaultMinDiskCount[b.DesiredRAIDType])
	for hostName, devices := range b.hostNameToFinalDeviceNames {
		if len(devices)%diskCountByRAIDType != 0 {
			errMsgs = append(errMsgs, fmt.Sprintf(
				"Invalid disk count %d w.r.t RAID %q on host %q",
				len(devices), b.DesiredRAIDType, hostName,
			))
		}
	}
	if len(errMsgs) != 0 {
		b.err = errors.Errorf("Validation failed: [%s]", strings.Join(errMsgs, ", "))
	}
}

// buildDesiredRAIDGroupsByHostName builds that fragment of the
// CStorPoolCluster spec that deals with raid groups.
// The resulting fragment is based on the given node name.
func (b *Builder) buildDesiredRAIDGroupsByHostName(nodeName string) []interface{} {
	// local function to build blockdevice sections per raid group
	buildBlockDevices := func(deviceNames []string) []interface{} {
		var blockDevices []interface{}
		for _, deviceName := range deviceNames {
			blockDevices = append(
				blockDevices, map[string]interface{}{
					"blockDeviceName": deviceName,
				},
			)
		}
		return blockDevices
	}
	// local function to build a raid group
	buildSingleRAIDGroup := func(deviceNames []string) interface{} {
		return map[string]interface{}{
			"type":         string(b.DesiredRAIDType),
			"isWriteCache": false,
			"isSpare":      false,
			"isReadCache":  false,
			"blockDevices": buildBlockDevices(deviceNames),
		}
	}
	// local function to build all raid groups within a node/pool
	buildAllRAIDGroupsPerHost := func(deviceNames []string) []interface{} {
		var raidGroupList []interface{}
		var raidGroup []string

		diskCountByRAIDType :=
			int(types.RAIDTypeToDefaultMinDiskCount[b.DesiredRAIDType])
		for idx, deviceName := range deviceNames {
			raidGroup = append(raidGroup, deviceName)
			// Following logic takes care of distributing disks based
			// on RAID type.
			//
			// NOTE:
			//	Disks get distributed as follows:
			// 	- Mirror has 2 disks per raid group
			//	- RAIDZ has 3 disks per raid group
			//  - RAIDZ2 has 6 disks per raid group
			//  - Stripe has 1 disk per raid group
			if (idx+1)%diskCountByRAIDType == 0 {
				raidGroupList = append(raidGroupList, buildSingleRAIDGroup(raidGroup))
				// reset the raidGroup to make way to build next raidGroup for this pool
				raidGroup = nil
			}
		}
		return raidGroupList
	}
	return buildAllRAIDGroupsPerHost(b.hostNameToFinalDeviceNames[nodeName])
}

// buildDesiredPoolByHostName builds that fragment of CStorPoolCluster
// that deals with specifying a single pool instance. The resulting
// fragment is based on the given node name.
func (b *Builder) buildDesiredPoolByHostName(hostName string) interface{} {
	return map[string]interface{}{
		"nodeSelector": map[string]interface{}{
			"kubernetes.io/hostname": hostName,
		},
		"raidGroups": b.buildDesiredRAIDGroupsByHostName(hostName),
		"poolConfig": map[string]interface{}{
			"defaultRaidGroupType": string(b.DesiredRAIDType),
			"overProvisioning":     false,
			"compression":          "off",
		},
	}
}

// buildDesiredPools builds that fragment of CStorPoolCluster
// that deals with specifying all the desired pool instances.
func (b *Builder) buildDesiredPools() []interface{} {
	var pools []interface{}
	for _, hostName := range b.desiredOrderedHostNames {
		pool := b.buildDesiredPoolByHostName(hostName)
		pools = append(pools, pool)
	}
	return pools
}

// BuildDesiredState builds the desired CStorPoolCluster based on
// observed & desired block device names
func (b *Builder) BuildDesiredState() (*unstructured.Unstructured, error) {
	// set the defaults before proceeding to create the desired state
	b.setDefaultsIfNotSet()
	// run some validation checks
	b.validate()
	// check for any build or validation errors
	if b.err != nil {
		return nil, b.err
	}
	// start constructing the desired state
	cspc := &unstructured.Unstructured{}
	cspc.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      b.Name,
			"namespace": b.Namespace,
		},
		"spec": map[string]interface{}{
			"pools": b.buildDesiredPools(),
		},
	})
	if len(b.DesiredAnnotations) != 0 {
		cspc.SetAnnotations(b.DesiredAnnotations)
	}
	if len(b.DesiredLabels) != 0 {
		cspc.SetLabels(b.DesiredLabels)
	}
	// below is the right way to set APIVersion & Kind
	cspc.SetAPIVersion(string(types.APIVersionOpenEBSV1Alpha1))
	cspc.SetKind(string(types.KindCStorPoolCluster))
	return cspc, nil
}
