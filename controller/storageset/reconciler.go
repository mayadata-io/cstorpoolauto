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

package storageset

import (
	"encoding/json"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"openebs.io/metac/controller/generic"

	"cstorpoolauto/k8s"
	"cstorpoolauto/types"
)

type reconcileErrHandler struct {
	storageSet   *unstructured.Unstructured
	hookResponse *generic.SyncHookResponse
}

func (h *reconcileErrHandler) handle(err error) {
	// Error has been handled elaborately. This logic ensures
	// error message is propagated to the resource & hence seen via
	// 'kubectl get CStorClusterStorageSet -oyaml'.
	//
	// In addition, errors are logged as well.
	glog.Errorf(
		"Failed to reconcile CStorClusterStorageSet %s: %v", h.storageSet.GetName(), err,
	)

	conds, mergeErr :=
		k8s.MergeStatusConditions(
			h.storageSet,
			types.MakeCStorClusterStorageSetReconcileErrCond(err),
		)
	if mergeErr != nil {
		glog.Errorf(
			"Failed to reconcile CStorClusterStorageSet %s: Can't set status conditions: %v",
			h.storageSet.GetName(), mergeErr,
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
		h.hookResponse.Status["phase"] = types.CStorClusterStorageSetStatusPhaseError
		h.hookResponse.Status["conditions"] = conds
	}

	// stop further reconciliation since there was an error
	h.hookResponse.SkipReconcile = true
}

// Sync implements the idempotent logic reconcile
// CStorClusterStorageSet
//
// NOTE:
// 	SyncHookRequest is the payload received as part of reconcile
// request. Similarly, SyncHookResponse is the payload sent as a
// response as part of reconcile request.
//
// NOTE:
//	SyncHookRequest uses CStorClusterStorageSet as the watched resource.
// SyncHookResponse has the resources that forms the desired state
// w.r.t the watched resource.
//
// NOTE:
//	Returning error will panic this process. We would rather want this
// controller to run continuously. Hence, the errors are logged and at
// the same time, these errors are posted against CStorClusterStorageSet
// status.
func Sync(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	response = &generic.SyncHookResponse{}

	// construct the error handler
	errHandler := &reconcileErrHandler{
		storageSet:   request.Watch,
		hookResponse: response,
	}

	var observedStorages []*unstructured.Unstructured
	for _, attachment := range request.Attachments.List() {
		if attachment.GetKind() == string(k8s.KindStorage) {
			// verify further if this belongs to the current watch
			uid, _ := k8s.GetAnnotationForKey(
				attachment.GetAnnotations(), types.AnnKeyCStorClusterStorageSetUID,
			)
			if string(request.Watch.GetUID()) == uid {
				// this is a desired Storage
				observedStorages = append(observedStorages, attachment)
				// we don't want to add this Storage now
				// but later after reconciliation
				continue
			}
		}
		// add attachments as-is if they are not of kind Storage
		response.Attachments = append(response.Attachments, attachment)
	}

	reconciler, err := NewReconciler(request.Watch, observedStorages)
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	op, err := reconciler.Reconcile()
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	response.Attachments = append(response.Attachments, op.DesiredStorages...)
	response.Status = op.Status
	return nil
}

// Reconciler enables reconciliation of CStorClusterStorageSet instance
type Reconciler struct {
	CStorClusterStorageSet *types.CStorClusterStorageSet
	ObservedStorages       []*unstructured.Unstructured
}

// ReconcileResponse forms the response due to reconciliation of
// CStorClusterStorageSet
type ReconcileResponse struct {
	DesiredStorages []*unstructured.Unstructured
	Status          map[string]interface{}
}

// NewReconciler returns a new instance of reconciler
func NewReconciler(
	storageSet *unstructured.Unstructured,
	observedStorages []*unstructured.Unstructured,
) (*Reconciler, error) {
	// transform storageset from unstructured to typed
	var cstorClusterStorageSetTyped *types.CStorClusterStorageSet
	cstorClusterStorageSetRaw, err := storageSet.MarshalJSON()
	if err != nil {
		return nil, errors.Wrapf(err, "Can't marshal CStorClusterStorageSet")
	}
	err = json.Unmarshal(cstorClusterStorageSetRaw, cstorClusterStorageSetTyped)
	if err != nil {
		return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterStorageSet")
	}
	// use above constructed object to build Reconciler instance
	return &Reconciler{
		CStorClusterStorageSet: cstorClusterStorageSetTyped,
		ObservedStorages:       observedStorages,
	}, nil
}

// Reconcile observed state of CStorClusterStorageSet to its desired
// state
func (r *Reconciler) Reconcile() (ReconcileResponse, error) {
	planner := NewStoragePlanner(r.CStorClusterStorageSet, r.ObservedStorages)
	desiredStorages, err := planner.Plan()
	if err != nil {
		return ReconcileResponse{}, err
	}
	return ReconcileResponse{
		DesiredStorages: desiredStorages,
		Status:          types.MakeCStorClusterStorageSetStatusToOnline(r.CStorClusterStorageSet),
	}, nil
}

// StoragePlanner ensures if any Storage instances need to be
// created, removed, updated or perhaps does not require any
// changes at all.
type StoragePlanner struct {
	ObservedStorages []*unstructured.Unstructured
	StorageSetUID    k8stypes.UID
	DesiredCount     resource.Quantity
	DesiredCapacity  resource.Quantity
	DesiredNodeName  string
	DesiredNamespace string
}

// NewStoragePlanner returns a new instance of StoragePlanner
func NewStoragePlanner(
	storageSet *types.CStorClusterStorageSet,
	observedStorages []*unstructured.Unstructured,
) *StoragePlanner {
	// initialize the planner
	return &StoragePlanner{
		ObservedStorages: observedStorages,
		StorageSetUID:    storageSet.GetUID(),
		DesiredCount:     storageSet.Spec.Disk.Count,
		DesiredCapacity:  storageSet.Spec.Disk.Capacity,
		DesiredNodeName:  storageSet.Spec.Node.Name,
		DesiredNamespace: storageSet.GetNamespace(),
	}
}

// Plan provides the desired Storages
func (p *StoragePlanner) Plan() ([]*unstructured.Unstructured, error) {
	var finalStorages []*unstructured.Unstructured
	if int64(len(p.ObservedStorages)) < p.DesiredCount.Value() {
		// create the difference
		createObjs := p.create(p.DesiredCount.Value() - int64(len(p.ObservedStorages)))
		finalStorages = append(finalStorages, createObjs...)
	}
	for _, storage := range p.ObservedStorages {
		if int64(len(finalStorages)) == p.DesiredCount.Value() {
			break
		}
		// update to desired characteristics
		err := p.update(storage)
		if err != nil {
			return nil, err
		}
		finalStorages = append(finalStorages, storage)
	}
	return finalStorages, nil
}

func (p *StoragePlanner) create(count int64) []*unstructured.Unstructured {
	var desiredStorages []*unstructured.Unstructured
	var i int64
	for i = 0; i < count; i++ {
		new := &unstructured.Unstructured{}
		new.SetUnstructuredContent(map[string]interface{}{
			"metadata": map[string]interface{}{
				"apiVersion":   "dao.mayadata.io/v1alpha1",
				"kind":         "Storage",
				"generateName": "ccsset-", // ccsset -> CStorClusterStorageSet
				"namespace":    p.DesiredNamespace,
				"annotations": map[string]interface{}{
					string(types.AnnKeyCStorClusterStorageSetUID): p.StorageSetUID,
				},
			},
			"spec": map[string]interface{}{
				"capacity": p.DesiredCapacity,
				"nodeName": p.DesiredNodeName,
			},
		})
		desiredStorages = append(desiredStorages, new)
	}
	return desiredStorages
}

func (p *StoragePlanner) update(storage *unstructured.Unstructured) error {
	err := unstructured.SetNestedMap(
		storage.UnstructuredContent(),
		map[string]interface{}{
			"capacity": p.DesiredCapacity,
			"nodeName": p.DesiredNodeName,
		},
		"spec",
	)
	if err != nil {
		return err
	}
	return nil
}
