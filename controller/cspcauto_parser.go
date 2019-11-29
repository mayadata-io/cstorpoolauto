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

package lib

import (
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	// BlockDeviceReservationKey is an annotation key that
	// identifies if the block device was marked as reserved
	// by any resource instance.
	//
	// TODO: (@kmova, @akhilerm)
	// It is expected for NDM to understand this reservation.
	BlockDeviceReservationKey string = "ndm.openebs.io/reservation"
)

// CSPCAutoParser parses a CSPCAuto instance and exposes
// various building blocks (reads methods) that deal with managing
// cstor pool cluster.
type CSPCAutoParser struct {
	// this cstor pool auto instance will be parsed
	target CSPCAuto

	// label key used to identify the nodes
	nodeLabelKey string

	// label key used to identify the resource that has
	// reserved a block device
	blockDeviceReservationKey string

	// registry of node names that are referred to in CSPCAuto
	nodes map[string]bool

	// TODO (@amitkumardas):
	// Merge both these maps to a single map
	//
	// i.e. nodeToDiskConfig map[string]CSPIDiskConfig

	// number of disks per node that participate in
	// provisioning a cstor pool instance
	nodeToDiskCount map[string]*int

	// node with corresponding disk capacity
	//
	// NOTE:
	// 	It is assumed that each disk will have same
	// capacity
	nodeToDiskCapacity map[string]resource.Quantity
}

// NewCSPCAutoParser parses the provided CSPCAuto instance
// and returns a new instance of CSPCAutoParser.
//
// NOTE:
//	One must make use of this constructor before invoking
// any parser methods.
func NewCSPCAutoParser(target CSPCAuto) (*CSPCAutoParser, error) {
	nodeToDiskCount := make(map[string]*int)
	nodeToDiskCapacity := make(map[string]resource.Quantity)
	nodes := make(map[string]bool)
	uniqueNodeLabelKey := ""
	targetName := target.GetName()

	for _, cspi := range target.Spec.CSPIList.Items {
		if cspi.DiskCount == nil || *cspi.DiskCount == 0 {
			return nil,
				errors.Errorf("Invalid CSPCAuto %s: Nil disk count", targetName)
		}
		if len(cspi.NodeLabel) == 0 {
			return nil,
				errors.Errorf(
					"Invalid CSPCAuto %s: Invalid CSPI: Missing node label", targetName,
				)
		}
		if len(cspi.NodeLabel) != 1 {
			return nil,
				errors.Errorf(
					"Invalid CSPI in CSPCAuto %s: Only one node label is supported; found %d: NodeLabel %v",
					targetName, len(cspi.NodeLabel), cspi.NodeLabel,
				)
		}
		if cspi.DiskCapacity.IsZero() {
			return nil,
				errors.Errorf(
					"Invalid CSPCAuto %s: CSPI disk capacity can't be zero: Node label %v",
					targetName, cspi.NodeLabel,
				)
		}

		for nodeLblKey, nodeLblVal := range cspi.NodeLabel {
			if uniqueNodeLabelKey != "" && uniqueNodeLabelKey != nodeLblKey {
				return nil,
					errors.Errorf(
						"Invalid cspi list in CSPCAuto %s: Only one node label key is supported: Found %s & %s",
						targetName, uniqueNodeLabelKey, nodeLblKey,
					)
			}
			// check if this node is already used by some other
			// cstor pool instance in this list
			if nodes[nodeLblVal] {
				return nil,
					errors.Errorf(
						"Invalid cspi list in CSPCAuto %s: Duplicate node label %s found",
						targetName, nodeLblVal,
					)
			}
			// finally set the values
			// TODO (@amitkumardas): Log these at debug level V(4)
			nodes[nodeLblVal] = true
			nodeToDiskCount[nodeLblVal] = cspi.DiskCount
			nodeToDiskCapacity[nodeLblVal] = cspi.DiskCapacity
		}
	}

	return &CSPCAutoParser{
		target:                    target,
		nodeLabelKey:              uniqueNodeLabelKey,
		nodes:                     nodes,
		nodeToDiskCount:           nodeToDiskCount,
		nodeToDiskCapacity:        nodeToDiskCapacity,
		blockDeviceReservationKey: BlockDeviceReservationKey,
	}, nil
}

// NodeLabelKey returns the node label key evaluated
// by this parser
func (p *CSPCAutoParser) NodeLabelKey() string {
	return p.nodeLabelKey
}

// NodeToDiskCount returns the disk counts grouped by
// node name
func (p *CSPCAutoParser) NodeToDiskCount() map[string]*int {
	return p.nodeToDiskCount
}

// NodeToDiskCapacity returns the disk capacities group by
// node name
func (p *CSPCAutoParser) NodeToDiskCapacity() map[string]resource.Quantity {
	return p.nodeToDiskCapacity
}

// Nodes returns the nodes evaluated by this parser
func (p *CSPCAutoParser) Nodes() map[string]bool {
	return p.nodes
}

// IsNode returns true if the provided nodeName exists
func (p *CSPCAutoParser) IsNode(nodeName string) bool {
	return p.nodes[nodeName]
}

// GroupStoragesByNode maps each of the provided storage
// instances corresponding by their node names
//
// NOTE:
//	The unstruct instances that are provided to this method
// are assumed to belong to this CSPCAuto instance. Some
// higher level logic would have filtered the same.
func (p *CSPCAutoParser) groupStoragesByNode(
	all []*unstructured.Unstructured,
) (map[string][]*unstructured.Unstructured, error) {
	nodeToStorages := make(map[string][]*unstructured.Unstructured)

	for _, storage := range all {
		storageName := storage.GetName()
		nodeName, found, err :=
			unstructured.NestedString(storage.UnstructuredContent(), "spec", "nodeName")
		if err != nil {
			return nil,
				errors.Wrapf(err, "Error grouping storage %s by node", storageName)
		}
		if !found || nodeName == "" {
			return nil,
				errors.Errorf(
					"Failed to group storage %s by node: Node name not found", storageName,
				)
		}
		if !p.nodes[nodeName] {
			return nil,
				errors.Errorf(
					"Failed to group storage %s by node: Node %s is not declared in CSPCAuto %s",
					storageName, nodeName, p.target.GetName(),
				)
		}
		// finally set this storage
		nodeToStorages[nodeName] = append(nodeToStorages[nodeName], storage)
	}

	return nodeToStorages, nil
}

// FilterEligibleBlockDevicesByNode evaluates each of the
// provided block devices and if eligible, maps them to
// corresponding node name
func (p *CSPCAutoParser) filterEligibleBlockDevicesByNode(
	all []*unstructured.Unstructured,
) (map[string][]*unstructured.Unstructured, error) {
	nodeToEligibleBlockDevices := make(map[string][]*unstructured.Unstructured)

	// Now filter the eligible block devices from the
	// provided list of devices
	for _, bd := range all {
		bdName := bd.GetName()

		nodeName, found, err :=
			unstructured.NestedString(
				bd.UnstructuredContent(), "spec", "nodeAttributes", "nodeName",
			)
		if err != nil {
			return nil,
				errors.Wrapf(err, "Error fetching node name for block device %s", bdName)
		}
		if !found || nodeName == "" {
			return nil, errors.Errorf("Invalid block device %s: Node name not found", bdName)
		}
		// check if this is a desired node?
		if !p.nodes[nodeName] {
			// this node is not desired; continue with the next device
			continue
		}
		// check if this device is reserved by someone else?
		reservedBy, _, err := unstructured.NestedString(
			bd.UnstructuredContent(),
			"metadata", "annotations", p.blockDeviceReservationKey,
		)
		if err != nil {
			// TODO (@amitkumardas):
			// Check if this should be just logged & continued?
			return nil,
				errors.Wrapf(
					err,
					"Invalid block device %s: Failed to get reservation annotation %s",
					bd.GetName(), p.blockDeviceReservationKey,
				)
		}
		if reservedBy != "" && reservedBy != p.target.GetName() {
			// this device is already reserved by some other resource
			// TODO (@amitkumardas):
			// Log this @ debug level
			continue
		}
		cap, found, err :=
			unstructured.NestedInt64(bd.UnstructuredContent(), "spec", "capacity", "storage")
		if err != nil {
			// TODO (@amitkumardas):
			// Check if this should be just logged & continued?
			return nil,
				errors.Wrapf(
					err, "Invalid block device %s: Failed to get capacity", bd.GetName(),
				)
		}
		if !found {
			// TODO (@amitkumardas):
			// Check if this should be just logged & continued?
			return nil,
				errors.Errorf("Invalid block device %s: Capacity not found", bd.GetName())
		}
		desiredCap := p.nodeToDiskCapacity[nodeName]
		if desiredCap.CmpInt64(cap) == 1 {
			// capacity is not sufficient; continue with next device
			// TODO (@amitkumardas):
			// log this at debug level
			continue
		}
		// finally add this blockdevice to the eligible list
		nodeToEligibleBlockDevices[nodeName] = append(nodeToEligibleBlockDevices[nodeName], bd)
	}

	return nodeToEligibleBlockDevices, nil
}

// GetDesiredDisks provides the desired disk details
// grouped by node name
func (p *CSPCAutoParser) getDesiredDisks() map[string]CSPIDiskConfig {
	nodeToCSPIDiskInfo := make(map[string]CSPIDiskConfig)
	for nodeName := range p.nodes {
		cap := p.nodeToDiskCapacity[nodeName]
		count := p.nodeToDiskCount[nodeName]
		nodeToCSPIDiskInfo[nodeName] = CSPIDiskConfig{
			DiskCapacity: cap,
			DiskCount:    count,
		}
	}
	return nodeToCSPIDiskInfo
}

// FindMissingDevices determines the missing disks
// based on the provided storages, devices and from this
// parser's own fields.
func (p *CSPCAutoParser) FindMissingDevices(
	storages []*unstructured.Unstructured,
	devices []*unstructured.Unstructured,
) (map[string]CSPIDiskConfig, error) {

	observedStorages, err := p.groupStoragesByNode(storages)
	if err != nil {
		return nil, err
	}
	observedDevices, err := p.filterEligibleBlockDevicesByNode(devices)
	if err != nil {
		return nil, err
	}
	desiredDisks := p.getDesiredDisks()

	missingDevices := make(map[string]CSPIDiskConfig)
	for nodeName, disk := range desiredDisks {
		observedStorCount := len(observedStorages[nodeName])
		observedDiskCount := len(observedDevices[nodeName])
		if *disk.DiskCount == observedStorCount+observedDiskCount {
			// TODO (@amitkumardas):
			// Log this at debug level
			// nothing is missing
			continue
		}
		if *disk.DiskCount < observedStorCount+observedDiskCount {
			// TODO (@amitkumardas):
			// log this; this might be a defect in cstor pool auto
			// since we are getting extra disks which might be a
			// result of cstor pool auto operator provisioning more
			// storages than its required
			//
			// we have more disks; hence nothing is missing
			continue
		}
		availableDiskCount := observedStorCount + observedDiskCount
		missingDiskCount := *disk.DiskCount - availableDiskCount
		missingDevices[nodeName] = CSPIDiskConfig{
			DiskCapacity: disk.DiskCapacity,
			DiskCount:    IntPtr(missingDiskCount),
		}
	}
	return missingDevices, nil
}
