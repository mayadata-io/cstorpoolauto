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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// SyncCSPC reconciles to a desired cstor pool cluster
// resource
//
// TODO (@amitkumardas):
// Unit Tests
func SyncCSPC(req *GenericHookRequest) (*GenericHookResponse, error) {
	var errMsgTitle = "Failed to sync cspc"

	// initialize the hook response
	resp := &GenericHookResponse{}

	wraw, err := req.Watch.MarshalJSON()
	if err != nil {
		// TODO (@amitkumardas):
		// Publish this error as TTL event against GenericController
		// This should avoid getting into logs
		return nil, errors.Wrapf(err, errMsgTitle)
	}

	var desired CSPCAuto
	dDecoder := json.NewDecoder(bytes.NewReader(wraw))
	err = dDecoder.Decode(&desired)
	if err != nil {
		return nil, err
	}

	// TODO (@amitkumardas):
	// 1/ set correct apiGroup, kind
	// 2/ check if namespace & name needs to be provided
	// to find the correct resource
	cspc := req.Attachments.FindResourceByGroupKindName(
		"openebs.io/v1alpha1",
		"CStorPoolCluster",
		desired.GetName(),
	)
	if cspc != nil {
		// nothing needs to be done, since cspc for this
		// cspcauto was created earlier
		//
		// NOTE:
		// Reconciling cspc is not supported yet
		resp.SkipReconcile = true
		return resp, nil
	}

	// TODO (@amitkumardas):
	// 1/ set correct apiGroup & kind
	// 2/ check if namespace & name needs to be provided
	// to find the correct resource
	// 3/ set status of the watch to reflect this
	cspcAutoKeeperAtt := req.Attachments.FindResourceByGroupKindName(
		"dao.mayadata.io/v1alpha1",
		"CSPCAutoKeeper",
		desired.GetName(),
	)
	if cspcAutoKeeperAtt == nil {
		// nothing to be done since cspc auto keeper is
		// not yet created
		resp.SkipReconcile = true
		return resp, nil
	}

	cspcAutoKeeperRaw, err := cspcAutoKeeperAtt.MarshalJSON()
	if err != nil {
		// TODO (@amitkumardas):
		// 1/ Log & 2/ Publish this error as a watch status
		return nil, errors.Wrapf(err, "%s: CSPCAuto %s", errMsgTitle, desired.GetName())
	}

	var observed CSPCAutoKeeper
	oDecoder := json.NewDecoder(bytes.NewReader(cspcAutoKeeperRaw))
	err = oDecoder.Decode(&observed)
	if err != nil {
		return nil, errors.Wrapf(err, "%s: CSPCAuto %s", errMsgTitle, desired.GetName())
	}

	// TODO (@amitkumardas):
	// 1/ Set this error as a status in watch
	// 2/ Log this error
	// 3/ Set skip reconcile to true
	observedDevices, match, err := getObservedDeviceListIfStatesMatch(&desired, &observed)
	if err != nil {
		return nil, errors.Wrapf(err, errMsgTitle)
	}
	// TODO (@amitkumardas):
	// 1/ set status of watch to reflect this
	if !match {
		// nothing to be done since cspc auto keeper is
		// still being synchronised with cspc auto
		resp.SkipReconcile = true
		resp.ResyncAfterSeconds = 3
		return resp, nil
	}

	// build cspc and return it as an attachment to be reconciled
	// by metac
	cspc = buildCSPC(desired, observedDevices)
	resp.Attachments = append(resp.Attachments, cspc)
	return resp, nil
}

// Get the eligible & available block devices if desired state i.e.
// cspcauto matches with observed state i.e. cspcautokeeper
//
// NOTE:
//	This logic handles validation while trying to get the eligible
// block devices
func getObservedDeviceListIfStatesMatch(
	desired *CSPCAuto,
	observed *CSPCAutoKeeper,
) ([]CSPIKeeperConfig, bool, error) {
	if len(desired.Spec.CSPIList.Items) == 0 {
		return nil, false,
			errors.Errorf("Invalid cspc auto %s: Nil cspi list", desired.GetName())
	}

	// length match has to be exactly equal;no less; no more
	if len(desired.Spec.CSPIList.Items) != len(observed.Spec.CSPIList.Items) {
		// desired (i.e. cspcauto) to observed (i.e. cspcautokeeper) is not a match yet
		// TODO (@amitkumardas):
		// Log this as a debug log i.e. level 2
		return nil, false, nil
	}

	// below logic parses the desired cstor pool instance(s)
	var uniqueNodeLabelKey string
	nodeToDesiredDiskCount := make(map[string]*int)
	for _, cspi := range desired.Spec.CSPIList.Items {
		if len(cspi.NodeLabel) != 1 {
			return nil, false,
				errors.Errorf(
					"Invalid cspc auto %s: Only one node label per cspi is supported: Found %d",
					desired.GetName(), len(cspi.NodeLabel),
				)
		}
		for nodeLblKey, nodeLblVal := range cspi.NodeLabel {
			if uniqueNodeLabelKey != "" && uniqueNodeLabelKey != nodeLblKey {
				return nil, false,
					errors.Errorf(
						"Invalid cspc auto %s: Only one node label key supported: Found %s & %s",
						desired.GetName(), uniqueNodeLabelKey, nodeLblKey,
					)
			}
			// save it to compare against next cstor pool instance
			uniqueNodeLabelKey = nodeLblKey
			// was this node label used earlier by other cstor pool instance
			if nodeToDesiredDiskCount[nodeLblVal] != nil {
				return nil, false,
					errors.Errorf(
						"Invalid cspi list in cspc auto %s: Duplicate node label value %s found",
						desired.GetName(), nodeLblVal,
					)
			}
			if cspi.DiskCount == nil || *cspi.DiskCount == 0 {
				return nil, false,
					errors.Errorf(
						"Invalid cspi disk count: Zero disk count not supported: CSPC auto %s: Node label %s %s",
						desired.GetName(), nodeLblKey, nodeLblVal,
					)
			}
			nodeToDesiredDiskCount[nodeLblVal] = cspi.DiskCount
		}
	}

	// below logic parses the observed cstor pool instances
	// via the availability of block devices
	keeperNodes := make(map[string]bool)
	for _, cspiKConfig := range observed.Spec.CSPIList.Items {
		if len(cspiKConfig.NodeLabel) != 1 {
			return nil, false,
				errors.Errorf(
					"Invalid cspc auto keeper %s: Only one node label per cspi is supported: Found %d",
					observed.GetName(), len(cspiKConfig.NodeLabel),
				)
		}
		for _, nodeLblVal := range cspiKConfig.NodeLabel {
			// was this node label already used by an earlier observed cstor pool instance
			if keeperNodes[nodeLblVal] {
				return nil, false,
					errors.Errorf(
						"Invalid cspi list in cspc auto keeper %s: Duplicate node label value %s found",
						observed.GetName(), nodeLblVal,
					)
			}
			if *nodeToDesiredDiskCount[nodeLblVal] != len(cspiKConfig.ActualBlockDevices) {
				// desired to observed is still a no match
				// TODO (@amitkumardas):
				// Log this as debug log i.e. level 2
				return nil, false, nil
			}
			keeperNodes[nodeLblVal] = true
		}
	}

	// at this point desired state matches the observed state
	return observed.Spec.CSPIList.Items, true, nil
}

// buildCSPC constructs a CSPC instance based on the desired
// & observed cstor pool instances
func buildCSPC(desired CSPCAuto, observed []CSPIKeeperConfig) *unstructured.Unstructured {
	// TODO (@amitkumardas):
	// 1/ Need to support different pool types
	// 	e.g. stripe, mirror, raidz, etc
	// 2/ Need to support various pool config
	//	e.g. cachefile, overprovisioning, compression
	// 3/ Need to support deployment/infra related requirements
	//	e.g. CPU, memory limits per container, etc
	getStripeRaidGroupsFromKeepConfigItem := func(kConfig CSPIKeeperConfig) []interface{} {
		raidGroup := map[string]interface{}{
			"type":         "stripe",
			"isWriteCache": "false",
			"isSpare":      "false",
			"isReadCache":  "false",
		}
		var blockDevices []interface{}
		for _, bDevice := range kConfig.ActualBlockDevices {
			blockDevices = append(blockDevices, map[string]string{
				"blockDeviceName": bDevice,
			})
		}
		unstructured.SetNestedSlice(raidGroup, blockDevices, "blockDevices")
		return []interface{}{raidGroup}
	}
	getPoolSpecsFromKeeperConfigList := func(kConfigs []CSPIKeeperConfig) []interface{} {
		var poolSpecs []interface{}
		for _, kconf := range kConfigs {
			poolSpecs = append(poolSpecs, map[string]interface{}{
				"nodeSelector": kconf.NodeLabel,
				"raidGroups":   getStripeRaidGroupsFromKeepConfigItem(kconf),
			})
		}
		return poolSpecs
	}
	cspc := &unstructured.Unstructured{}
	cspc.SetUnstructuredContent(map[string]interface{}{
		"apiVersion": "openebs.io/v1alpha1",
		"kind":       "CStorPoolCluster",
		"metadata": map[string]interface{}{
			"name":      desired.GetName(),
			"namespace": desired.GetNamespace(),
		},
		"spec": map[string]interface{}{
			"pools": getPoolSpecsFromKeeperConfigList(observed),
		},
	})
	return cspc
}
