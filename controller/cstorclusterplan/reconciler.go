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

package cstorclusterplan

import (
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"
	"openebs.io/metac/controller/generic"

	"cstorpoolauto/k8s"
	"cstorpoolauto/types"
	"cstorpoolauto/util/metac"
)

type reconcileErrHandler struct {
	clusterPlan  *unstructured.Unstructured
	hookResponse *generic.SyncHookResponse
}

func (h *reconcileErrHandler) handle(err error) {
	// Error has been handled elaborately. This logic ensures
	// error message is propagated to the resource & hence seen via
	// 'kubectl get CStorClusterPlan -oyaml'.
	//
	// In addition, errors are logged as well.
	glog.Errorf(
		"Failed to reconcile CStorClusterPlan %s %s: %+v",
		h.clusterPlan.GetNamespace(), h.clusterPlan.GetName(), err,
	)

	conds, mergeErr :=
		k8s.MergeStatusConditions(
			h.clusterPlan,
			types.MakeCStorClusterPlanReconcileErrCond(err),
		)
	if mergeErr != nil {
		glog.Errorf(
			"Failed to reconcile CStorClusterPlan %s %s: Can't set status conditions: %+v",
			h.clusterPlan.GetNamespace(), h.clusterPlan.GetName(), mergeErr,
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
		h.hookResponse.Status["phase"] = types.CStorClusterPlanStatusPhaseError
		h.hookResponse.Status["conditions"] = conds
	}
	// this will stop further reconciliation by metac since there was an error
	h.hookResponse.SkipReconcile = true
}

// Sync implements the idempotent logic to reconcile CStorClusterPlan
//
// NOTE:
// 	SyncHookRequest is the payload received as part of reconcile
// request. Similarly, SyncHookResponse is the payload sent as a
// response as part of reconcile request.
//
// NOTE:
//	SyncHookRequest uses CStorClusterPlan as the watched resource.
// SyncHookResponse has the resources that forms the desired state
// w.r.t the watched resource.
//
// NOTE:
//	Returning error will panic this process. We would rather want this
// controller to run continuously. Hence, the errors are logged and at
// the same time, these errors are posted against CStorClusterPlan's
// status.
func Sync(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	if request == nil {
		return errors.Errorf("Failed to reconcile CStorClusterPlan: Nil request found")
	}
	if response == nil {
		return errors.Errorf("Failed to reconcile CStorClusterPlan: Nil response found")
	}

	glog.V(3).Infof(
		"Will reconcile CStorClusterPlan %s %s:",
		request.Watch.GetNamespace(), request.Watch.GetName(),
	)

	// construct the error handler
	errHandler := &reconcileErrHandler{
		clusterPlan:  request.Watch,
		hookResponse: response,
	}

	var observedStorageSets []*unstructured.Unstructured
	var cstorClusterConfig *unstructured.Unstructured
	var desiredCStorClusterConfigUID string
	desiredCStorClusterConfigUID, _ = k8s.GetAnnotationForKey(
		request.Watch.GetAnnotations(), types.AnnKeyCStorClusterConfigUID,
	)
	for _, attachment := range request.Attachments.List() {
		if attachment.GetKind() == string(types.KindCStorClusterStorageSet) {
			// verify further if CStorClusterStorageSet belongs to current watch
			uid, _ := k8s.GetAnnotationForKey(
				attachment.GetAnnotations(), types.AnnKeyCStorClusterPlanUID,
			)
			if string(request.Watch.GetUID()) == uid {
				// this is a desired CStorClusterStorageSet
				observedStorageSets = append(observedStorageSets, attachment)
				// we don't want to add this CStorClusterStorageSet now
				// but later after reconciliation
				continue
			}
		}
		if attachment.GetKind() == string(types.KindCStorClusterConfig) {
			// verify further if CStorClusterConfig belongs to current watch
			if string(attachment.GetUID()) == desiredCStorClusterConfigUID {
				// this is a desired CStorClusterConfig
				cstorClusterConfig = attachment
			}
		}
		// add attachments to response if they are not of kind
		// CStorClusterStorageSet
		response.Attachments = append(response.Attachments, attachment)
	}
	if cstorClusterConfig == nil {
		errHandler.handle(errors.Errorf("Missing CStorClusterConfig attachment"))
		return nil
	}

	reconciler, err := NewReconciler(request.Watch, cstorClusterConfig, observedStorageSets)
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	op, err := reconciler.Reconcile()
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	response.Attachments = append(response.Attachments, op.DesiredStorageSets...)
	response.Status = op.Status

	glog.V(2).Infof(
		"CStorClusterPlan %s %s reconciled successfully: %s",
		request.Watch.GetNamespace(), request.Watch.GetName(),
		metac.GetDetailsFromResponse(response),
	)

	return nil
}

// Reconciler enables reconciliation of CStorClusterPlan instance
type Reconciler struct {
	CStorClusterPlan    *types.CStorClusterPlan
	CStorClusterConfig  *types.CStorClusterConfig
	ObservedStorageSets []*unstructured.Unstructured
}

// ReconcileResponse forms the response due to reconciliation of
// CStorClusterPlan
type ReconcileResponse struct {
	DesiredStorageSets []*unstructured.Unstructured
	Status             map[string]interface{}
}

// NewReconciler returns a new instance of reconciler
func NewReconciler(
	clusterPlan *unstructured.Unstructured,
	clusterConfig *unstructured.Unstructured,
	observedStorageSets []*unstructured.Unstructured,
) (*Reconciler, error) {
	// transforms cluster plan from unstructured to typed
	var cstorClusterPlanTyped types.CStorClusterPlan
	cstorClusterPlanRaw, err := clusterPlan.MarshalJSON()
	if err != nil {
		return nil, errors.Wrapf(err, "Can't marshal CStorClusterPlan")
	}
	err = json.Unmarshal(cstorClusterPlanRaw, &cstorClusterPlanTyped)
	if err != nil {
		return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterPlan")
	}
	// transforms cluster config from unstructured to typed
	var cstorClusterConfigTyped types.CStorClusterConfig
	cstorClusterConfigRaw, err := clusterConfig.MarshalJSON()
	if err != nil {
		return nil, errors.Wrapf(err, "Can't marshal CStorClusterConfig")
	}
	err = json.Unmarshal(cstorClusterConfigRaw, &cstorClusterConfigTyped)
	if err != nil {
		return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterConfig")
	}
	// use above constructed objects to build Reconciler instance
	return &Reconciler{
		CStorClusterPlan:    &cstorClusterPlanTyped,
		CStorClusterConfig:  &cstorClusterConfigTyped,
		ObservedStorageSets: observedStorageSets,
	}, nil
}

// Reconcile observed state of CStorClusterPlan to its desired
// state
func (r *Reconciler) Reconcile() (ReconcileResponse, error) {
	planner, err := NewStorageSetsPlanner(
		r.CStorClusterPlan,
		r.ObservedStorageSets,
	)
	if err != nil {
		return ReconcileResponse{}, err
	}
	desiredStorageSets, err := planner.Plan(r.CStorClusterConfig)
	if err != nil {
		return ReconcileResponse{}, err
	}
	return ReconcileResponse{
		DesiredStorageSets: desiredStorageSets,
		Status: types.MakeCStorClusterPlanToOnlineWithNoReconcileErr(
			r.CStorClusterPlan,
		),
	}, nil
}

// StorageSetsPlanner ensures if CStorClusterStorageSet instance(s)
// need to be created, deleted, updated or perhaps does not require
// any changes at all.
type StorageSetsPlanner struct {
	ClusterPlan *types.CStorClusterPlan

	// NOTE:
	// All the maps in this structure have node UID as their keys
	ObservedStorageSetObjs map[string]*unstructured.Unstructured
	ObservedStorageSets    map[string]bool

	IsCreate map[string]bool // map of newly desired nodes
	IsRemove map[string]bool // map of nodes that are no more needed
	IsNoop   map[string]bool // map of nodes that are desired & are already in-use

	PlannedNodeNames map[string]string // map of desired node names
	Updates          map[string]string // map of not needed to newly desired nodes
}

// NewStorageSetsPlanner returns a new instance of
// StorageSetPlanner.
//
// NOTE:
//	This function builds all the plans to needed to create,
// remove, update & noop StorageSets.
func NewStorageSetsPlanner(
	clusterPlan *types.CStorClusterPlan,
	observedStorageSets []*unstructured.Unstructured,
) (*StorageSetsPlanner, error) {
	// initialize the planner
	planner := &StorageSetsPlanner{
		ClusterPlan:            clusterPlan,
		ObservedStorageSetObjs: map[string]*unstructured.Unstructured{},
		ObservedStorageSets:    map[string]bool{},
		IsCreate:               map[string]bool{},
		IsRemove:               map[string]bool{},
		IsNoop:                 map[string]bool{},
		PlannedNodeNames:       map[string]string{},
		Updates:                map[string]string{},
	}
	for _, storageSet := range observedStorageSets {
		nodeUID, found, err := unstructured.NestedString(
			storageSet.UnstructuredContent(), "spec", "node", "uid",
		)
		if err != nil {
			return nil, errors.Wrapf(
				err,
				"Failed to get spec.node.uid: StorageSet %s %s",
				storageSet.GetNamespace(), storageSet.GetName(),
			)
		}
		if !found || nodeUID == "" {
			return nil, errors.Errorf(
				"Invalid StorageSet %s %s: Missing spec.node.uid",
				storageSet.GetNamespace(), storageSet.GetName(),
			)
		}
		planner.ObservedStorageSets[nodeUID] = true
		planner.ObservedStorageSetObjs[nodeUID] = storageSet
	}
	for _, plannedNode := range clusterPlan.Spec.Nodes {
		// store node uid to name mapping
		planner.PlannedNodeNames[string(plannedNode.UID)] = plannedNode.Name
		// planned nodes need to get into some bucket
		if planner.ObservedStorageSets[string(plannedNode.UID)] {
			// this node is desired and is observed
			planner.IsNoop[string(plannedNode.UID)] = true
		} else {
			// this node is desired and is not observed
			planner.IsCreate[string(plannedNode.UID)] = true
		}
	}
	// there may be more observed nodes than what is
	// planned currently
	for observedNodeUID := range planner.ObservedStorageSets {
		if planner.IsNoop[observedNodeUID] || planner.IsCreate[observedNodeUID] {
			continue
		}
		planner.IsRemove[observedNodeUID] = true
	}
	// build update inventory i.e. move observed storageset's
	// from old to a newly desired node based on create & remove
	// inventories
	//
	// NOTE:
	//	This essentially is the logic to detach disks (as specified
	// in the StorageSets) from old nodes & attach them to newly
	// desired nodes.
	for removeNodeUID := range planner.IsRemove {
		for createNodeUID := range planner.IsCreate {
			planner.Updates[removeNodeUID] = createNodeUID
			// nullify create & remove inventory since
			// they are accomodated by update inventory
			planner.IsRemove[removeNodeUID] = false
			planner.IsCreate[createNodeUID] = false
		}
	}
	return planner, nil
}

// Plan provides the list of desired StorageSets
func (p *StorageSetsPlanner) Plan(config *types.CStorClusterConfig) ([]*unstructured.Unstructured, error) {
	var finalStorageSets []*unstructured.Unstructured
	noopObjs := p.noop()
	createObjs := p.create(config)
	p.remove()
	updateObjs, err := p.update()
	if err != nil {
		return nil, err
	}
	finalStorageSets = append(finalStorageSets, noopObjs...)
	finalStorageSets = append(finalStorageSets, createObjs...)
	finalStorageSets = append(finalStorageSets, updateObjs...)
	return finalStorageSets, nil
}

// noop returns the list of CStorClusterStorageSet(s) that do
// **not** require any changes. In other words their desired &
// observed states matches.
func (p *StorageSetsPlanner) noop() []*unstructured.Unstructured {
	var storageSets []*unstructured.Unstructured
	for uid, isnoop := range p.IsNoop {
		if !isnoop {
			continue
		}
		storageSets = append(storageSets, p.ObservedStorageSetObjs[uid])
	}
	return storageSets
}

// create returns a list of CStorClusterStorageSet(s) that will
// get created in the cluster
func (p *StorageSetsPlanner) create(config *types.CStorClusterConfig) []*unstructured.Unstructured {
	var storageSets []*unstructured.Unstructured
	for nodeUID, iscreate := range p.IsCreate {
		if !iscreate {
			continue
		}
		storageSet := &unstructured.Unstructured{}
		storageSet.SetUnstructuredContent(map[string]interface{}{
			"metadata": map[string]interface{}{
				"apiVersion":   string(types.APIVersionDAOMayaDataV1Alpha1),
				"kind":         string(types.KindCStorClusterStorageSet),
				"generateName": "ccplan-", // ccplan -> CStorClusterPlan
				"namespace":    p.ClusterPlan.GetNamespace(),
				"annotations": map[string]interface{}{
					string(types.AnnKeyCStorClusterPlanUID): p.ClusterPlan.GetUID(),
				},
			},
			"spec": map[string]interface{}{
				"node": map[string]interface{}{
					"name": p.PlannedNodeNames[nodeUID],
					"uid":  nodeUID,
				},
				"disk": map[string]interface{}{
					"capacity": config.Spec.DiskConfig.MinCapacity,
					"count":    config.Spec.DiskConfig.MinCount,
				},
				"externalProvisioner": map[string]interface{}{
					"csiAttacherName":  config.Spec.DiskConfig.ExternalProvisioner.CSIAttacherName,
					"storageClassName": config.Spec.DiskConfig.ExternalProvisioner.StorageClassName,
				},
			},
		})
		storageSets = append(storageSets, storageSet)
	}
	return storageSets
}

// remove will remove the list of CStorClusterStorageSet(s) that
// are no more required.
//
// NOTE:
// 	This is a noop function since metac will remove the
// resources if they were part of the request but are not
// sent in the response
func (p *StorageSetsPlanner) remove() {
	for uid, isremove := range p.IsRemove {
		if !isremove {
			continue
		}
		// log it for debuggability purposes
		glog.V(3).Infof(
			"Will remove CStorClusterStorageSet %s %s having node uid %s",
			p.ObservedStorageSetObjs[uid].GetNamespace(),
			p.ObservedStorageSetObjs[uid].GetName(),
			uid,
		)
	}
}

// update will return a list of modified CStorClusterStorageSet(s)
// which in turn will get updated at the cluster.
func (p *StorageSetsPlanner) update() ([]*unstructured.Unstructured, error) {
	var updatedStorageSets []*unstructured.Unstructured
	for oldNodeUID, newNodeUID := range p.Updates {
		storageSet := p.ObservedStorageSetObjs[oldNodeUID]
		copy := storageSet.DeepCopy()
		// set new node details
		node := map[string]string{
			"name": p.PlannedNodeNames[newNodeUID],
			"uid":  newNodeUID,
		}
		err := unstructured.SetNestedField(copy.Object, node, "spec", "node")
		if err != nil {
			return nil, err
		}
		updatedStorageSets = append(updatedStorageSets, copy)
	}
	return updatedStorageSets, nil
}
