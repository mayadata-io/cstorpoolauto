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
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// SyncCSPCAutoKeeper reconciles cspcauto request by applying
// corresponding CSPCAutoKeeper resource
//
// TODO (@amitkumardas):
// Unit Tests
func SyncCSPCAutoKeeper(req *GenericHookRequest) (*GenericHookResponse, error) {

	errMsgTitle := "Failed to sync cspc auto keeper"

	wraw, err := req.Watch.MarshalJSON()
	if err != nil {
		// TODO (@amitkumardas):
		// Publish this error as TTL event against GenericController
		// This should avoid getting into logs
		return nil, err
	}

	var watch CSPCAuto
	jd := json.NewDecoder(bytes.NewReader(wraw))
	err = jd.Decode(&watch)
	if err != nil {
		return nil, errors.Wrapf(err, errMsgTitle)
	}

	// TODO (@amitkumardas):
	// Clean up old status if any

	if len(watch.Spec.CSPIList.Items) == 0 {
		// TODO (@amitkumardas):
		// update this against the watch's status
		return nil, errors.Errorf("%s: Nil CSPI items", errMsgTitle)
	}

	if req.Attachments.IsEmpty() {
		// TODO (@amitkumardas):
		// this is not an error
		// update this against the watch's status
		return nil,
			errors.Errorf("%s: No available block devices", errMsgTitle)
	}

	blockDevices := req.Attachments.FilterResourceMapByGroupKind(
		// TODO (@amitkumardas):
		// Set correct apiVersion of NDM
		"openebs.io/v1alpha1", "BlockDevice",
	)
	if blockDevices == nil {
		// TODO (@amitkumardas):
		// this is not an error
		// update this against the watch's status
		return nil,
			errors.Errorf("%s: No block devices in available state", errMsgTitle)
	}

	cspiKConfigs, err :=
		buildCSPIKeeperConfigList(blockDevices, watch.Spec.CSPIList.Items)
	if err != nil {
		// TODO
		// Log this error & report this against the watch's status
		return nil, errors.Wrapf(err, "%s: CSPCAuto %s", errMsgTitle, watch.Name)
	}

	cspcAK := &unstructured.Unstructured{}
	cspcAK.SetKind("CSPCAutoKeeper")
	cspcAK.SetAPIVersion("dao.mayadata.io/v1alpha1")
	cspcAK.SetName(watch.GetName())
	cspcAK.SetNamespace(watch.GetNamespace())
	unstructured.SetNestedSlice(
		cspcAK.Object,
		cspiKConfigs,
		"spec", "cspiList", "items",
	)

	// initialize the hook response
	resp := &GenericHookResponse{}
	resp.Attachments = append(resp.Attachments, cspcAK)

	return resp, nil
}

type cspiKeeperConfigs []interface{}

// buildCSPIKeeperConfigList builds a list of cstor pool
// keeper configs. These keeper configs have eligible
// block devices to be used to create cstor pool instances via
// cstor pool auto resource.
//
// TODO (@amitkumardas):
// 1/ Make use of CSPCAutoParser
// 2/ Unit Tests
func buildCSPIKeeperConfigList(
	availableBlockDevices map[string]*unstructured.Unstructured,
	desiredCSPIConfigs []CSPIConfig,
) (cspiKeeperConfigs, error) {
	var cspiKConfigs cspiKeeperConfigs
	var uniqueNodeLabelKey string
	errMsgTitle := "Failed to build cspi keeper configs"

	nodeToDesiredDiskCount := make(map[string]*int)
	nodeToDesiredDiskCapacity := make(map[string]resource.Quantity)
	nodeToKeeperDiskCount := make(map[string]int)
	nodeToEligibleBlockDevices := make(map[string][]string)

	// evaluate desired cstor pool instances
	for _, cspi := range desiredCSPIConfigs {
		if cspi.DiskCount == nil || *cspi.DiskCount == 0 {
			// log at V(3 or 4)
			continue
		}
		if cspi.DiskCapacity.IsZero() {
			return nil,
				errors.Errorf(
					"%s: CSPI disk capacity can't be zero: Node label %v",
					errMsgTitle, cspi.NodeLabel,
				)
		}
		if len(cspi.NodeLabel) == 0 {
			return nil,
				errors.Errorf(
					"%s: Invalid CSPI: Missing node label", errMsgTitle,
				)
		}
		if len(cspi.NodeLabel) != 1 {
			return nil,
				errors.Errorf(
					"%s: Invalid CSPI: Only one node label required; found %d: %v",
					errMsgTitle, len(cspi.NodeLabel), cspi.NodeLabel,
				)
		}
		for nodeLblKey, nodeLblVal := range cspi.NodeLabel {
			if nodeLblKey == "" {
				return nil,
					errors.Errorf(
						"%s: Invalid CSPI node label %v",
						errMsgTitle, cspi.NodeLabel,
					)
			}
			if uniqueNodeLabelKey != "" && uniqueNodeLabelKey != nodeLblKey {
				return nil,
					errors.Errorf(
						"%s: Different CSPI node label keys were found: Use only one: either %s or %s",
						errMsgTitle, uniqueNodeLabelKey, nodeLblKey,
					)
			}
			// store the node label key to be compared against
			// others in next iteration
			uniqueNodeLabelKey = nodeLblKey

			if nodeToDesiredDiskCount[nodeLblVal] != nil {
				return nil,
					errors.Errorf(
						"%s: Invalid CSPI list: Duplicate node label found: %s %s",
						errMsgTitle, nodeLblKey, nodeLblVal,
					)
			}

			nodeToDesiredDiskCount[nodeLblVal] = cspi.DiskCount
			nodeToDesiredDiskCapacity[nodeLblVal] = cspi.DiskCapacity
		}
	}

	// evaluate eligible block devices from the available list
	for bdName, bd := range availableBlockDevices {
		if bd.Object == nil {
			continue
		}
		// TODO (@amitkumardas):
		// set exact fields for node name
		bdNodeName, found, err :=
			unstructured.NestedString(bd.Object, "spec", "nodeName")
		if err != nil {
			return nil,
				errors.Wrapf(
					err,
					"%s: Block device %s", errMsgTitle, bdName,
				)
		}
		if !found {
			continue
		}
		kDiskCount := nodeToKeeperDiskCount[bdNodeName]
		if kDiskCount == *nodeToDesiredDiskCount[bdNodeName] {
			// no need to do anything for this node
			// since the eligible disks are found already
			continue
		}
		// TODO (@amitkumardas):
		// set exact fields for diskCapacity
		bdCapacity, found, err :=
			unstructured.NestedString(bd.Object, "spec", "diskCapacity")
		if err != nil {
			return nil,
				errors.Wrapf(
					err,
					"%s: Block device %s", errMsgTitle, bdName,
				)
		}
		if !found {
			return nil,
				errors.Errorf(
					"%s: Missing disk capacity: Block device %s", errMsgTitle, bdName,
				)
		}

		bdCapacityQty, err := resource.ParseQuantity(bdCapacity)
		if err != nil {
			return nil,
				errors.Wrapf(err, "%s: Block device %s", errMsgTitle, bdName)
		}
		bdCapacityQtyPtr := &bdCapacityQty
		cmp := bdCapacityQtyPtr.Cmp(nodeToDesiredDiskCapacity[bdNodeName])
		if cmp < 0 {
			// TODO (@amitkumardas):
			// Log here V(3 or 4)
			continue
		}

		// after all above validations this device is approved
		// as eligible to being used to create cstor pool instance
		nodeToKeeperDiskCount[bdNodeName] = kDiskCount + 1
		kBlockDeviceNames := nodeToEligibleBlockDevices[bdNodeName]
		nodeToEligibleBlockDevices[bdNodeName] = append(kBlockDeviceNames, bdName)
	}

	// build the cspi keeper config
	for nodeName, bdNames := range nodeToEligibleBlockDevices {
		cspiKConfigs = append(cspiKConfigs, CSPIKeeperConfig{
			NodeLabel: map[string]string{
				uniqueNodeLabelKey: nodeName,
			},
			DesiredDiskCount:    nodeToDesiredDiskCount[nodeName],
			DesiredDiskCapacity: nodeToDesiredDiskCapacity[nodeName],
			ActualBlockDevices:  bdNames,
		})
	}
	return cspiKConfigs, nil
}
