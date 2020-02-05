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

	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
	"mayadata.io/cstorpoolauto/util/metac"
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
		unstruct.MergeStatusConditions(
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
	desiredCStorClusterConfigUID, _ = unstruct.GetValueForKey(
		request.Watch.GetAnnotations(), types.AnnKeyCStorClusterConfigUID,
	)
	for _, attachment := range request.Attachments.List() {
		if attachment.GetKind() == string(types.KindCStorClusterStorageSet) {
			// verify further if CStorClusterStorageSet belongs to current watch
			uid, _ := unstruct.GetValueForKey(
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

	// TODO (@amitkumardas):
	// Can't set status as this creates a never ending hot loop
	// In other words, this updates the watch & reconciliations
	// of this watch due to other controllers get impacted
	//response.Status = op.Status

	glog.V(2).Infof(
		"CStorClusterPlan %s %s reconciled successfully: %s",
		request.Watch.GetNamespace(), request.Watch.GetName(),
		metac.GetDetailsFromResponse(response),
	)

	return nil
}

// Reconciler enables reconciliation of CStorClusterPlan instance
type Reconciler struct {
	ClusterPlan         *types.CStorClusterPlan
	ClusterConfig       *types.CStorClusterConfig
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
		ClusterPlan:         &cstorClusterPlanTyped,
		ClusterConfig:       &cstorClusterConfigTyped,
		ObservedStorageSets: observedStorageSets,
	}, nil
}

// Reconcile observed state of CStorClusterPlan to its desired
// state
func (r *Reconciler) Reconcile() (*ReconcileResponse, error) {
	planner, err := NewStorageSetsPlanner(
		r.ClusterPlan,
		r.ClusterConfig,
		r.ObservedStorageSets,
	)
	if err != nil {
		return nil, err
	}
	desiredStorageSets, err := planner.Plan()
	if err != nil {
		return nil, err
	}
	return &ReconcileResponse{
		DesiredStorageSets: desiredStorageSets,
		Status: types.MakeCStorClusterPlanToOnlineWithNoReconcileErr(
			r.ClusterPlan,
		),
	}, nil
}

// StorageSetListPlanner ensures if CStorClusterStorageSet instance(s)
// need to be created, deleted, updated or perhaps does not require
// any changes at all.
type StorageSetListPlanner struct {
	ClusterPlan   *types.CStorClusterPlan
	ClusterConfig *types.CStorClusterConfig

	// NOTE:
	// 	All the maps here represent desired state w.r.t. Node
	//
	// NOTE:
	// 	Each map has **Node UID** as its key
	ObservedStorageSetObjs map[string]*unstructured.Unstructured
	ObservedStorageSets    map[string]bool

	IsNodeCreate map[string]bool // map of newly desired nodes
	IsNodeRemove map[string]bool // map of nodes that are no more needed
	IsNodeNoop   map[string]bool // map of nodes with no change i.e. nodes already in-use

	PlannedNodeNames map[string]string // map of desired node names
	NodeUpdates      map[string]string // map of not needed to newly desired nodes
}

// NewStorageSetsPlanner returns a new instance of
// StorageSetPlanner.
//
// NOTE:
//	This function builds all the plans to needed to create,
// remove, update & noop StorageSets.
//
// TODO (@amitkumardas):
//	Unit Tests is a must
func NewStorageSetsPlanner(
	clusterPlan *types.CStorClusterPlan,
	clusterConfig *types.CStorClusterConfig,
	observedStorageSets []*unstructured.Unstructured,
) (*StorageSetListPlanner, error) {
	// initialize the planner
	planner := &StorageSetListPlanner{
		ClusterPlan:            clusterPlan,
		ClusterConfig:          clusterConfig,
		ObservedStorageSetObjs: map[string]*unstructured.Unstructured{},
		ObservedStorageSets:    map[string]bool{},
		IsNodeCreate:           map[string]bool{},
		IsNodeRemove:           map[string]bool{},
		IsNodeNoop:             map[string]bool{},
		PlannedNodeNames:       map[string]string{},
		NodeUpdates:            map[string]string{},
	}
	// logic to categorise storage sets indexed by their node UID
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
	// logic to create new categories based on changes w.r.t node UID
	for _, plannedNode := range clusterPlan.Spec.Nodes {
		// store node uid to name mapping
		planner.PlannedNodeNames[string(plannedNode.UID)] = plannedNode.Name
		// planned nodes need to get into some bucket
		if planner.ObservedStorageSets[string(plannedNode.UID)] {
			// this node is observed at cluster and still remains as desired
			planner.IsNodeNoop[string(plannedNode.UID)] = true
		} else {
			// this node is newly desired i.e. is not observed at cluster currently
			planner.IsNodeCreate[string(plannedNode.UID)] = true
		}
	}
	// there may be more observed nodes than what is
	// planned currently
	for observedNodeUID := range planner.ObservedStorageSets {
		if planner.IsNodeNoop[observedNodeUID] || planner.IsNodeCreate[observedNodeUID] {
			// nothing needs to be done if this node UID is
			// present as a desired node in one of the following
			// categories:
			// 	1/ noop i.e. node is already used & should continue to be used or,
			// 	2/ node will get created & participate in cstor pool cluster
			continue
		}
		planner.IsNodeRemove[observedNodeUID] = true
	}
	// build update inventory i.e. move observed storageset's
	// from old to a newly desired node based on create & remove
	// inventories
	//
	// NOTE:
	//	This essentially is the logic to detach disks (as specified
	// in CStorClusterStorageSet) from old nodes & attach them
	// to newly desired nodes.
	for removeNodeUID := range planner.IsNodeRemove {
		for createNodeUID := range planner.IsNodeCreate {
			if !planner.IsNodeRemove[removeNodeUID] {
				// break out of the inner loop
				break
			}
			if !planner.IsNodeCreate[createNodeUID] {
				// continue with next item of this inner loop
				continue
			}
			// plan the replacement
			planner.NodeUpdates[removeNodeUID] = createNodeUID
			// nullify nodes from create & remove inventory since
			// they are now accomodated by update inventory
			planner.IsNodeRemove[removeNodeUID] = false
			planner.IsNodeCreate[createNodeUID] = false
		}
	}
	return planner, nil
}

// Plan provides the list of desired StorageSets
func (p *StorageSetListPlanner) Plan() ([]*unstructured.Unstructured, error) {
	var finalStorageSets []*unstructured.Unstructured
	syncedObjs, err := p.sync()
	if err != nil {
		return nil, err
	}
	createdObjs := p.create()
	p.remove()
	updatedObjs, err := p.updateNode()
	if err != nil {
		return nil, err
	}
	finalStorageSets = append(finalStorageSets, syncedObjs...)
	finalStorageSets = append(finalStorageSets, createdObjs...)
	finalStorageSets = append(finalStorageSets, updatedObjs...)
	return finalStorageSets, nil
}

// sync returns the list of CStorClusterStorageSet(s)
// with synced state.
func (p *StorageSetListPlanner) sync() ([]*unstructured.Unstructured, error) {
	var storageSets []*unstructured.Unstructured
	for uid, isNodeNoop := range p.IsNodeNoop {
		if !isNodeNoop {
			continue
		}

		glog.V(3).Infof(
			"Will sync CStorClusterStorageSet %q / %q",
			p.ObservedStorageSetObjs[uid].GetNamespace(),
			p.ObservedStorageSetObjs[uid].GetName(),
		)

		storageSets = append(
			storageSets,
			p.getDesiredStorageSet(
				p.ObservedStorageSetObjs[uid].GetName(), uid,
			),
		)
	}
	return storageSets, nil
}

// create returns a list of newly desired CStorClusterStorageSet(s)
// that will get created in the cluster
func (p *StorageSetListPlanner) create() []*unstructured.Unstructured {
	var storageSets []*unstructured.Unstructured
	for nodeUID, iscreate := range p.IsNodeCreate {
		if !iscreate {
			continue
		}
		glog.V(2).Infof(
			// log it for debuggability purposes
			"Will create CStorClusterStorageSet with node uid %s", nodeUID,
		)
		// TODO (@amitkumardas):
		// Does use of an UID result in name over shooting
		// its max length?
		//
		// NOTE:
		//	This naming adheres to **deterministic naming
		// principle** which handles simultaneous
		// reconciliations of desired state to result in
		// creation of only one instance of CStorClusterStorageSet
		storageSetName := p.ClusterPlan.GetName() + "-" + nodeUID
		storageSets = append(
			storageSets, p.getDesiredStorageSet(storageSetName, nodeUID),
		)
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
func (p *StorageSetListPlanner) remove() {
	for uid, isremove := range p.IsNodeRemove {
		if !isremove {
			continue
		}
		// log it for debuggability purposes
		glog.V(2).Infof(
			"Will remove CStorClusterStorageSet %s %s having node uid %s",
			p.ObservedStorageSetObjs[uid].GetNamespace(),
			p.ObservedStorageSetObjs[uid].GetName(),
			uid,
		)
	}
}

// updateNode will return a list of modified
// CStorClusterStorageSet(s) which will form the new desired state.
func (p *StorageSetListPlanner) updateNode() ([]*unstructured.Unstructured, error) {
	var updatedStorageSets []*unstructured.Unstructured
	for oldNodeUID, newNodeUID := range p.NodeUpdates {
		storageSet := p.ObservedStorageSetObjs[oldNodeUID]

		glog.V(3).Infof(
			// log it for debuggability purposes
			"Will update CStorClusterStorageSet %q / %q from node uid %q to %q",
			storageSet.GetNamespace(), storageSet.GetName(), oldNodeUID, newNodeUID,
		)

		updatedStorageSets = append(
			updatedStorageSets,
			p.getDesiredStorageSet(storageSet.GetName(), newNodeUID),
		)
	}
	return updatedStorageSets, nil
}

// getDesiredStorageSet returns the desired state of StorageSet
// based on the given node UID.
//
// NOTE:
//	The returned instance is idempotent and hence can be used during
// create & update operations
func (p *StorageSetListPlanner) getDesiredStorageSet(
	storageSetName string, nodeUID string,
) *unstructured.Unstructured {
	storageSet := &unstructured.Unstructured{}
	storageSet.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":      storageSetName,
			"namespace": p.ClusterPlan.GetNamespace(),
		},
		"spec": map[string]interface{}{
			"node": map[string]interface{}{
				"name": p.PlannedNodeNames[nodeUID],
				"uid":  nodeUID,
			},
			"disk": map[string]interface{}{
				"capacity": p.ClusterConfig.Spec.DiskConfig.MinCapacity,
				"count":    p.ClusterConfig.Spec.DiskConfig.MinCount,
			},
			"externalDiskConfig": map[string]interface{}{
				"csiAttacherName":  p.ClusterConfig.Spec.DiskConfig.ExternalDiskConfig.CSIAttacherName,
				"storageClassName": p.ClusterConfig.Spec.DiskConfig.ExternalDiskConfig.StorageClassName,
			},
		},
	})
	// create annotations that refers to the instance which
	// triggered creation of this storage set i.e. CStorClusterPlan
	storageSet.SetAnnotations(
		map[string]string{
			types.AnnKeyCStorClusterPlanUID: string(p.ClusterPlan.GetUID()),
		},
	)
	// below is the right way to set APIVersion & Kind
	storageSet.SetAPIVersion(string(types.APIVersionDAOMayaDataV1Alpha1))
	storageSet.SetKind(string(types.KindCStorClusterStorageSet))
	return storageSet
}
