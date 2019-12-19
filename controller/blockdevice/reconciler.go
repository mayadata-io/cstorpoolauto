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

package blockdevice

import (
	"github.com/golang/glog"
	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"openebs.io/metac/controller/generic"

	"cstorpoolauto/k8s"
	"cstorpoolauto/types"
	"cstorpoolauto/util/metac"
	stringutil "cstorpoolauto/util/string"
)

type reconcileErrHandler struct {
	storage      *unstructured.Unstructured
	hookResponse *generic.SyncHookResponse
}

func (h *reconcileErrHandler) handle(err error) {
	// Error has been handled elaborately. This logic ensures
	// error message is propagated to the resource & hence seen via
	// 'kubectl get storage -oyaml'.
	//
	// In addition, errors are logged as well.
	glog.Errorf(
		"Failed to associate a BlockDevice with Storage %s %s: %+v",
		h.storage.GetNamespace(), h.storage.GetName(), err,
	)

	conds, mergeErr :=
		k8s.MergeStatusConditions(
			h.storage,
			types.MakeStorageToBlockDeviceAssociationErrCond(err),
		)
	if mergeErr != nil {
		glog.Errorf(
			"Can't set status conditions on Storage %s %s: %+v",
			h.storage.GetNamespace(), h.storage.GetName(), mergeErr,
		)
		// Note: Merge error will reset the conditions which will make
		// things worse since various controllers will be reconciling
		// based on these conditions.
		//
		// Hence it is better to set response status as nil to let metac
		// preserve old status conditions if any.
		h.hookResponse.Status = nil
	} else {
		// response status will be set against the watch's status by metac
		h.hookResponse.Status = map[string]interface{}{}
		h.hookResponse.Status["phase"] = types.StatusPhaseError
		h.hookResponse.Status["conditions"] = conds
	}

	// stop further reconciliation since there was an error
	h.hookResponse.SkipReconcile = true
}

// Sync implements the idempotent logic to reconcile
// the association of a Storage resource with corresponding
// BlockDevice resource.
//
// NOTE:
// 	SyncHookRequest is the payload received as part of reconcile
// request. Similarly, SyncHookResponse is the payload sent as a
// response as part of reconcile request.
//
// NOTE:
//	SyncHookRequest uses Storage as the watched resource.
// SyncHookResponse has the resources that forms the desired state
// w.r.t the watched resource.
//
// NOTE:
//	Returning error will panic this process. We would rather want this
// controller to run continuously. Hence, the errors are logged and at
// the same time, these errors are posted against Storage's status field.
func Sync(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	if request == nil {
		return errors.Errorf(
			"Failed to associate BlockDevice with Storage: Nil request found",
		)
	}
	if response == nil {
		return errors.Errorf(
			"Failed to associate BlockDevice with Storage: Nil response found",
		)
	}

	glog.V(3).Infof(
		"Will associate BlockDevice with Storage: %s", metac.GetDetailsFromRequest(request),
	)

	// construct the error handler
	errHandler := &reconcileErrHandler{
		storage:      request.Watch,
		hookResponse: response,
	}

	var cstorClusterStoragetSet *unstructured.Unstructured
	var pvc *unstructured.Unstructured
	for _, attachment := range request.Attachments.List() {
		if attachment.GetKind() == string(types.KindBlockDevice) {
			// No need to add BlockDevices to response now
			//
			// They will be attached after the reconciliation
			continue
		}
		if attachment.GetKind() == string(types.KindCStorClusterStorageSet) {
			// verify further if this belongs to the Storage i.e. watch
			uid, _ := k8s.GetAnnotationForKey(
				request.Watch.GetAnnotations(), types.AnnKeyCStorClusterStorageSetUID,
			)
			if uid == string(attachment.GetUID()) {
				// this is the expected CStorClusterStorageSet
				cstorClusterStoragetSet = attachment
			}
		}
		if attachment.GetKind() == string(types.KindPersistentVolumeClaim) {
			// verify further if this belongs to the Storage i.e. watch
			uid, _ := k8s.GetAnnotationForKey(
				attachment.GetAnnotations(), types.AnnKeyStorageUID,
			)
			if uid == string(request.Watch.GetUID()) {
				// this is the expected PersistentVolumeClaim
				pvc = attachment
			}
		}
		// add attachments as-is if they are not of kind BlockDevice
		response.Attachments = append(response.Attachments, attachment)
	}

	if cstorClusterStoragetSet == nil {
		errHandler.handle(errors.Errorf("CStorClusterStorageSet instance is missing"))
		return nil
	}

	if pvc == nil {
		glog.V(3).Infof("Will skip association of BlockDevice with Storage: Missing PVC")
		response.SkipReconcile = true
		return nil
	}

	reconciler := &Reconciler{
		Storage:           request.Watch,
		PVC:               pvc,
		StorageSet:        cstorClusterStoragetSet,
		ObservedResources: request.Attachments.List(),
	}
	op, err := reconciler.Reconcile()
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	response.Attachments = append(response.Attachments, op.DesiredBlockDevices...)

	// TODO (@amitkumardas):
	//
	// Can't set status as this creates a never ending hot loop
	// In other words, this updates the watch & reconciliations
	// of this watch due to other controllers get impacted
	//response.Status = op.Status

	glog.V(2).Infof(
		"BlockDevice was associated with Storage %s %s successfully: %s",
		request.Watch.GetNamespace(), request.Watch.GetName(),
		metac.GetDetailsFromResponse(response),
	)

	return nil
}

// Reconciler enables reconciliation of Storage instance
type Reconciler struct {
	StorageSet        *unstructured.Unstructured
	Storage           *unstructured.Unstructured
	PVC               *unstructured.Unstructured
	ObservedResources []*unstructured.Unstructured
}

// ReconcileResponse forms the response due to reconciliation of
// CStorClusterStorageSet
type ReconcileResponse struct {
	DesiredBlockDevices []*unstructured.Unstructured
	Status              map[string]interface{}
}

// Reconcile observed state of CStorClusterStorageSet to its desired
// state
func (r *Reconciler) Reconcile() (ReconcileResponse, error) {
	associator := &StorageToBlockDeviceAssociator{
		Storage:           r.Storage,
		StorageSet:        r.StorageSet,
		PVC:               r.PVC,
		ObservedResources: r.ObservedResources,
	}
	desiredBlockDevices, err := associator.Associate()
	if err != nil {
		return ReconcileResponse{}, err
	}
	// prepare the status to be set against the storage instance
	//status, err := r.getStorageStatusAsNoError()
	//if err != nil {
	//	return ReconcileResponse{}, err
	//}
	// build & return reconcile response
	return ReconcileResponse{
		DesiredBlockDevices: desiredBlockDevices,
		//Status:              status,
	}, nil
}

// getStorageStatusAsNoError sets no error in status & returns
// the updated status
//
// TODO (@amitkumardas):
// Need to rethink on error handling
// This functon is not used currently
func (r *Reconciler) getStorageStatusAsNoError() (map[string]interface{}, error) {
	// get the existing status.phase
	phase, found, err :=
		unstructured.NestedString(r.Storage.UnstructuredContent(), "status", "phase")
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to get status.phase")
	}
	if !found {
		return nil, errors.Errorf("Invalid storage: Can't find status.phase")
	}
	// get updated conditions
	conds, err := k8s.MergeStatusConditions(
		r.Storage,
		map[string]interface{}{
			"type":             types.StorageToBlockDeviceAssociationErrorCondition,
			"status":           types.ConditionIsAbsent,
			"lastObservedTime": metav1.Now(),
		},
	)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"phase":      phase,
		"conditions": conds,
	}, nil
}

// StorageToBlockDeviceAssociator associates a Storage instance
// with corresponding BlockDevice
type StorageToBlockDeviceAssociator struct {
	Storage           *unstructured.Unstructured
	StorageSet        *unstructured.Unstructured
	PVC               *unstructured.Unstructured
	ObservedResources []*unstructured.Unstructured
}

// Associate will first filter the matching BlockDevice(s
// and then add Storage related annotations against each
// device.
func (p *StorageToBlockDeviceAssociator) Associate() ([]*unstructured.Unstructured, error) {
	var final []*unstructured.Unstructured
	// extract PV name from PVC
	//
	// NOTE: PVC needs to be bound to a PV for this to happen
	pvName, found, err :=
		unstructured.NestedString(p.PVC.UnstructuredContent(), "spec", "volumeName")
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"Failed to fetch spec.volumeName from PVC %s %s",
			p.PVC.GetNamespace(), p.PVC.GetName(),
		)
	}
	observedBlockDevices := p.getObservedBlockDevices()
	if !found || pvName == "" {
		glog.V(3).Infof(
			"Will skip BlockDevice association: PV not found for PVC %s %s: Storage %s %s",
			p.PVC.GetNamespace(), p.PVC.GetName(), p.Storage.GetNamespace(), p.Storage.GetName(),
		)
		// TODO (@amitkumardas):
		// This logic needs to be revisited after enhancements & bug fixes
		// at metac
		//
		// Bug:
		// 	Metac has a bug that updates all returned
		// resources if these resources are configured
		// with updateAny true & update strategy set
		// to InPlace.
		//
		// Workaround:
		// 	We shall return empty block devices which means
		// metac will try to delete the observed block devices
		// but will fail since deleteAny is not enabled
		//return observedBlockDevices, nil
		return []*unstructured.Unstructured{}, nil
	}
	// TODO (@amitkumardas):
	// 	Read above note w.r.t bug & enhancement
	//
	//matchingBlockDevices, nonMatchingBlockDevices, err :=	p.filterBlockDevicesWithPVName(observedBlockDevices, pvName)
	matchingBlockDevices, _, err :=
		p.filterBlockDevicesWithPVName(observedBlockDevices, pvName)
	if err != nil {
		return nil, err
	}
	if len(matchingBlockDevices) == 0 {
		glog.V(3).Infof(
			"Will skip BlockDevice association: No matching BlockDevice for PV %s: Storage %s %s",
			pvName, p.Storage.GetNamespace(), p.Storage.GetName(),
		)
		// TODO (@amitkumardas):
		// 	Read above note w.r.t bug & enhancement
		//
		// return observedBlockDevices, nil
		return []*unstructured.Unstructured{}, nil
	}
	if len(matchingBlockDevices) > 1 {
		return nil, errors.Errorf(
			"Found %d BlockDevices with PV %s: Want exactly one BlockDevice",
			len(matchingBlockDevices), pvName,
		)
	}
	annotated, err := p.annotateBlockDevicesIfUnclaimed(matchingBlockDevices)
	if err != nil {
		return nil, err
	}
	// TODO (@amitkumardas):
	// 	Read above note w.r.t bug & enhancement
	// We shall return only the matching device
	//
	//final = append(final, nonMatchingBlockDevices...)
	return append(final, annotated...), nil
}

func (p *StorageToBlockDeviceAssociator) getObservedBlockDevices() []*unstructured.Unstructured {
	var blockDevices []*unstructured.Unstructured
	for _, resource := range p.ObservedResources {
		if resource.GetKind() == string(types.KindBlockDevice) {
			blockDevices = append(blockDevices, resource)
		}
	}
	return blockDevices
}

func (p *StorageToBlockDeviceAssociator) filterBlockDevicesWithPVName(
	blockDevices []*unstructured.Unstructured,
	pvName string,
) ([]*unstructured.Unstructured, []*unstructured.Unstructured, error) {
	var match []*unstructured.Unstructured
	var nomatch []*unstructured.Unstructured
	for _, device := range blockDevices {
		found, err := p.isBlockDeviceMatchWithPVName(device, pvName)
		if err != nil {
			return nil, nil, err
		}
		if found {
			match = append(match, device)
		} else {
			nomatch = append(nomatch, device)
		}
	}
	return match, nomatch, nil
}

func (p *StorageToBlockDeviceAssociator) isBlockDeviceMatchWithPVName(
	device *unstructured.Unstructured, pvName string,
) (bool, error) {
	// extract devlinks
	devlinks, found, err :=
		unstructured.NestedSlice(device.UnstructuredContent(), "spec", "devlinks")
	if err != nil {
		return false, errors.Wrapf(
			err,
			"Failed to fetch spec.devlinks from BlockDevice %s %s",
			device.GetNamespace(), device.GetName(),
		)
	}
	if !found || len(devlinks) == 0 {
		glog.V(3).Infof("Can't find spec.devlinks for BlockDevice %s %s",
			device.GetNamespace(), device.GetName(),
		)
		return false, nil
	}

	// find if any devlink relates to PV name
	for idx, devlink := range devlinks {
		links, found, err :=
			unstructured.NestedStringSlice(
				map[string]interface{}{"devlink": devlink}, "devlink", "links",
			)
		if err != nil {
			return false, errors.Wrapf(
				err,
				"Failed to fetch spec.devlinks[%d].links from BlockDevice %s %s",
				idx, device.GetNamespace(), device.GetName(),
			)
		}
		if !found || len(links) == 0 {
			glog.V(3).Infof("Can't find spec.devlinks[%d].links for BlockDevice %s %s",
				idx, device.GetNamespace(), device.GetName(),
			)
			continue
		}
		linkList := stringutil.List(links)
		if linkList.Contains(pvName) {
			glog.V(3).Infof("BlockDevice %s %s matches PV %s: Links [%s]",
				device.GetNamespace(), device.GetName(),
				pvName, linkList,
			)
			return true, nil
		}
	}
	return false, nil
}

func (p *StorageToBlockDeviceAssociator) annotateBlockDevicesIfUnclaimed(
	devices []*unstructured.Unstructured,
) ([]*unstructured.Unstructured, error) {
	var annotated []*unstructured.Unstructured
	for _, device := range devices {
		isUnclaimFound, err := p.isBlockDeviceUnclaimed(device)
		if err != nil {
			return nil, err
		}
		if !isUnclaimFound {
			// TODO (@amitkumardas):
			// 	Read previous notes on bug & enhancement required
			// at metac
			//
			// add device without any changes to annotations
			// if this device is not in unclaimed state
			// annotated = append(annotated, device)
			continue
		}
		// proceed further for unclaimed device only
		// extract CStorClusterPlan UID from CStorClusterStorageSet
		cstorClusterPlanUID, found := k8s.GetAnnotationForKey(
			p.StorageSet.GetAnnotations(),
			types.AnnKeyCStorClusterPlanUID,
		)
		if !found || cstorClusterPlanUID == "" {
			return nil, errors.Errorf(
				"Can't find CStorClusterPlan UID from StorageSet %s %s",
				p.StorageSet.GetNamespace(), p.StorageSet.GetName(),
			)
		}
		// add CStorClusterStorageSet UID to device's
		// existing annotations
		newAnns := k8s.MergeToAnnotations(
			types.AnnKeyCStorClusterStorageSetUID, string(p.StorageSet.GetUID()),
			device.GetAnnotations(),
		)
		// add CStorClusterPlan UID to device's
		// existing annotations
		newAnns = k8s.MergeToAnnotations(
			types.AnnKeyCStorClusterPlanUID, cstorClusterPlanUID,
			newAnns,
		)

		new := &unstructured.Unstructured{}
		new.SetAPIVersion(device.GetAPIVersion())
		new.SetKind(device.GetKind())
		new.SetNamespace(device.GetNamespace())
		new.SetName(device.GetName())
		new.SetAnnotations(newAnns)

		glog.V(2).Infof(
			"BlockDevice %s %s associated / annotated successfully: CStorClusterStorageSet %s: CStorClusterPlan %s",
			device.GetNamespace(),
			device.GetName(),
			p.StorageSet.GetUID(),
			cstorClusterPlanUID,
		)
		// add device that is updated with annotations
		annotated = append(annotated, new)
	}
	return annotated, nil
}

func (p *StorageToBlockDeviceAssociator) isBlockDeviceUnclaimed(
	device *unstructured.Unstructured,
) (bool, error) {
	status, found, err :=
		unstructured.NestedString(device.UnstructuredContent(), "status", "claimState")
	if err != nil {
		return false, err
	}
	if !found || status == "" {
		return false, errors.Errorf(
			"Can't find status.claimState for BlockDevice %s %s",
			device.GetNamespace(), device.GetName(),
		)
	}
	glog.V(3).Infof(
		"BlockDevice %s %s has claim state %s: Storage %s %s",
		device.GetNamespace(), device.GetName(),
		status,
		p.Storage.GetNamespace(), p.Storage.GetName(),
	)
	return status == string(types.BlockDeviceUnclaimed), nil
}
