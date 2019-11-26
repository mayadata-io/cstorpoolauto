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

// SyncStorageAndBlockDevice reconciles cspcauto request by applying
// corresponding storage resource(s) and updating corresponding
// BlockDevices with annotation.
//
// NOTE:
//	The block devices that are available as part of the provided
// request
func SyncStorageAndBlockDevice(req *GenericHookRequest) (*GenericHookResponse, error) {

	errMsgTitle := "Failed to sync storage & block device"

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
		return nil, errors.Wrapf(err, errMsgTitle)
	}

	// TODO (@amitkumardas):
	// Set this error as a status in the watch
	if len(desired.Spec.CSPIList.Items) == 0 {
		return nil,
			errors.Errorf(
				"%s: Invalid CSPCAuto %s: Nil CSPI list", errMsgTitle, desired.GetName(),
			)
	}

	blockDevices := req.Attachments.FilterResourceListByGroupKind(
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

	storages := req.Attachments.FilterResourceListByGroupKind(
		// TODO (@amitkumardas):
		// Set correct apiVersion
		"dao.mayadata.io/v1alpha1", "Storage",
	)

	// TODO (@amitkumardas):
	// Write rest of the business logic
	p, err := NewCSPCAutoParser(desired)
	if err != nil {
		return nil, err
	}

	missing, err := p.FindMissingDevices(storages, blockDevices)
	if err != nil {
		// This error should instead be handled & set against
		// the watch's status
		return nil, err
	}

	if len(missing) == 0 {
		resp.SkipReconcile = true
		return resp, nil
	}

	newStors := buildStorages(desired, missing)
	resp.Attachments = append(resp.Attachments, newStors...)
	resp.ResyncAfterSeconds = 3
	return resp, nil
}

func buildStorages(
	cspcauto CSPCAuto,
	disks map[string]CSPIDiskConfig,
) []*unstructured.Unstructured {
	var storages []*unstructured.Unstructured
	for nodeName, disk := range disks {
		stor := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"generateName": cspcauto.Name,
					"namespace":    cspcauto.Namespace,
					"annotations":  cspcauto.Annotations,
				},
				"spec": map[string]interface{}{
					"capacity": disk.DiskCapacity,
					"nodeName": nodeName,
				},
			},
		}
		storages = append(storages, stor)
	}
	return storages
}
