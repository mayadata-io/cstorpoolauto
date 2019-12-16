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

package cstorpoolcluster

import (
	"encoding/json"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"openebs.io/metac/controller/generic"

	"cstorpoolauto/k8s"
	"cstorpoolauto/types"
)

// reconcileErrHandler logs the error & updates these errors
// against CStorClusterPlan status conditions
type reconcileErrHandler struct {
	clusterPlan *unstructured.Unstructured
	response    *generic.SyncHookResponse
}

func (h *reconcileErrHandler) handle(err error) {
	// Error has been handled elaborately. This logic ensures
	// error message is propagated to the resource & hence seen via
	// 'kubectl get CStorClusterPlan -oyaml'.
	//
	// In addition, errors are logged as well.
	glog.Errorf(
		"Failed to apply CStorPoolCluster for CStorClusterPlan %s %s: %v",
		h.clusterPlan.GetNamespace(), h.clusterPlan.GetName(), err,
	)

	conds, mergeErr :=
		k8s.MergeStatusConditions(
			h.clusterPlan, types.MakeCStorClusterPlanCSPCApplyErrCond(err),
		)
	if mergeErr != nil {
		glog.Errorf(
			"Can't set status conditions for CStorClusterPlan %s %s: %v",
			h.clusterPlan.GetNamespace(), h.clusterPlan.GetName(), mergeErr,
		)
		// Note: Merge error will reset the conditions which will make
		// things worse since various controllers will be reconciling
		// based on these conditions.
		//
		// Hence it is better to set response status to nil to let metac
		// preserve old status conditions if any.
		h.response.Status = nil
	} else {
		// response status will be set against the watch's status by metac
		h.response.Status = map[string]interface{}{}
		h.response.Status["phase"] = types.CStorClusterPlanStatusPhaseError
		h.response.Status["conditions"] = conds
	}
	// will skip reconciliation process at metac since there was an error
	h.response.SkipReconcile = true
}

// Sync implements the idempotent logic to apply a CStorPoolCluster
// resource given a CStorClusterPlan resource. CStorClusterPlan is
// the watched resource.
//
// NOTE:
// 	SyncHookRequest is the payload received as part of reconcile
// request from metac. Similarly, SyncHookResponse is the payload
// sent as a response as part of reconcile request.
//
// NOTE:
//	SyncHookRequest uses CStorClusterPlan as the watched resource.
// SyncHookResponse has the resources that forms the desired state
// w.r.t the watched resource.
//
// NOTE:
//	Returning error will panic this process. We would rather want this
// controller to run continuously. Hence, the errors are logged and at
// the same time, these errors are posted against the watch here
// CStorClusterPlan status conditions.
func Sync(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	if request == nil {
		return errors.Errorf(
			"Failed to apply CStorPoolCluster for CStorClusterPlan: Nil request found",
		)
	}
	if response == nil {
		return errors.Errorf(
			"Failed to apply CStorPoolCluster for CStorClusterPlan: Nil response found",
		)
	}

	// construct the error handler
	errHandler := &reconcileErrHandler{
		clusterPlan: request.Watch,
		response:    response,
	}

	var desiredCStorPoolCluster *unstructured.Unstructured
	var observedClusterConfig *unstructured.Unstructured
	var observedBlockDevices []*unstructured.Unstructured
	var observedStorageSets []*unstructured.Unstructured
	for _, attachment := range request.Attachments.List() {
		if attachment.GetKind() == string(k8s.KindCStorPoolCluster) {
			// verify further if this belongs to the current watch
			// i.e. CStorClusterPlan
			uid, _ := k8s.GetAnnotationForKey(
				attachment.GetAnnotations(), types.AnnKeyCStorClusterPlanUID,
			)
			if string(request.Watch.GetUID()) == uid {
				// this is the desired CStorPoolCluster
				desiredCStorPoolCluster = attachment
				// we don't want to add to response now but later
				// after its reconcile logic is executed
				continue
			}
		}
		if attachment.GetKind() == string(k8s.KindBlockDevice) {
			// verify further if this belongs to the current watch
			// i.e. CStorClusterPlan
			uid, _ := k8s.GetAnnotationForKey(
				attachment.GetAnnotations(), types.AnnKeyCStorClusterPlanUID,
			)
			if string(request.Watch.GetUID()) == uid {
				// this is one of the desired BlockDevice(s)
				observedBlockDevices = append(observedBlockDevices, attachment)
			}
		}
		if attachment.GetKind() == string(k8s.KindCStorClusterStorageSet) {
			// verify further if this belongs to the current watch
			// i.e. CStorClusterPlan
			uid, _ := k8s.GetAnnotationForKey(
				attachment.GetAnnotations(), types.AnnKeyCStorClusterPlanUID,
			)
			if string(request.Watch.GetUID()) == uid {
				// this is one of the desired CStorClusterStorageSet(s)
				observedStorageSets = append(observedStorageSets, attachment)
			}
		}
		if attachment.GetKind() == string(k8s.KindCStorClusterConfig) {
			// verify further if this belongs to the current watch
			// i.e. CStorClusterPlan
			uid, _ := k8s.GetAnnotationForKey(
				request.Watch.GetAnnotations(), types.AnnKeyCStorClusterConfigUID,
			)
			if string(attachment.GetUID()) == uid {
				// this is the desired CStorClusterConfig
				observedClusterConfig = attachment
			}
		}
		// add the received attachments to response if it is not
		// our desired CStorPoolCluster
		response.Attachments = append(response.Attachments, attachment)
	}

	if observedClusterConfig == nil {
		return errors.Errorf("CStorClusterConfig instance was not found")
	}

	reconciler, err := NewReconciler(ReconcilerConfig{
		CStorClusterPlan:        request.Watch,
		DesiredCStorPoolCluster: desiredCStorPoolCluster,
		ObservedClusterConfig:   observedClusterConfig,
		ObservedStorageSets:     observedStorageSets,
		ObservedBlockDevices:    observedBlockDevices,
	})
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	op, err := reconciler.Reconcile()
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	// Cluster may or may not be **ready** to create a CStorPoolCluster
	if op.DesiredCStorPoolCluster != nil {
		response.Attachments = append(response.Attachments, op.DesiredCStorPoolCluster)
	} else {
		// will stop further reconciliation at metac since cluster is
		// not ready to create CStorPoolCluster
		response.SkipReconcile = true
	}
	response.Status = op.Status
	return nil
}

// Reconciler enables reconciliation of CStorClusterPlan instance
type Reconciler struct {
	CStorClusterPlan        *types.CStorClusterPlan
	DesiredCStorPoolCluster *unstructured.Unstructured
	ObservedClusterConfig   *unstructured.Unstructured
	ObservedStorageSets     []*unstructured.Unstructured
	ObservedBlockDevices    []*unstructured.Unstructured
}

// ReconcilerConfig is a helper structure used to create a
// new instance of Reconciler
type ReconcilerConfig struct {
	CStorClusterPlan        *unstructured.Unstructured
	DesiredCStorPoolCluster *unstructured.Unstructured
	ObservedClusterConfig   *unstructured.Unstructured
	ObservedStorageSets     []*unstructured.Unstructured
	ObservedBlockDevices    []*unstructured.Unstructured
}

// ReconcileResponse forms the response due to reconciliation of
// CStorClusterPlan
type ReconcileResponse struct {
	DesiredCStorPoolCluster *unstructured.Unstructured
	Status                  map[string]interface{}
}

// NewReconciler returns a new instance of reconciler
func NewReconciler(conf ReconcilerConfig) (*Reconciler, error) {
	// transform CStorClusterPlan from unstructured to typed
	var cstorClusterPlanTyped *types.CStorClusterPlan
	cstorClusterPlanRaw, err := conf.CStorClusterPlan.MarshalJSON()
	if err != nil {
		return nil, errors.Wrapf(err, "Can't marshal CStorClusterPlan")
	}
	err = json.Unmarshal(cstorClusterPlanRaw, cstorClusterPlanTyped)
	if err != nil {
		return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterPlan")
	}
	// use above constructed object to build Reconciler instance
	return &Reconciler{
		CStorClusterPlan:        cstorClusterPlanTyped,
		DesiredCStorPoolCluster: conf.DesiredCStorPoolCluster,
		ObservedClusterConfig:   conf.ObservedClusterConfig,
		ObservedStorageSets:     conf.ObservedStorageSets,
		ObservedBlockDevices:    conf.ObservedBlockDevices,
	}, nil
}

// Reconcile observed state of CStorClusterStorageSet to its desired
// state
func (r *Reconciler) Reconcile() (ReconcileResponse, error) {
	planner := Planner{
		CStorClusterPlan:        r.CStorClusterPlan,
		DesiredCStorPoolCluster: r.DesiredCStorPoolCluster,
		ObservedClusterConfig:   r.ObservedClusterConfig,
		ObservedStorageSets:     r.ObservedStorageSets,
		ObservedBlockDevices:    r.ObservedBlockDevices,
	}
	desiredCStorPoolCluster, err := planner.Plan()
	if err != nil {
		return ReconcileResponse{}, err
	}
	return ReconcileResponse{
		DesiredCStorPoolCluster: desiredCStorPoolCluster,
		Status:                  r.getClusterPlanStatusAsNoError(),
	}, nil
}

func (r *Reconciler) getClusterPlanStatusAsNoError() map[string]interface{} {
	types.MergeNoCSPCApplyErrorOnCStorClusterPlan(r.CStorClusterPlan)
	return map[string]interface{}{
		"phase":      r.CStorClusterPlan.Status.Phase,
		"conditions": r.CStorClusterPlan.Status.Conditions,
	}
}

// Planner ensures if any CStorPoolCluster instance need
// to be created, or updated or perhaps does not require
// any change.
type Planner struct {
	CStorClusterPlan        *types.CStorClusterPlan
	DesiredCStorPoolCluster *unstructured.Unstructured
	ObservedClusterConfig   *unstructured.Unstructured
	ObservedStorageSets     []*unstructured.Unstructured
	ObservedBlockDevices    []*unstructured.Unstructured

	// Node name to StorageSet UID
	nodeToStorageSets map[string]string

	// StorageSet UID to desired disk count
	storageSetToDesiredDiskCount map[string]resource.Quantity

	// StorageSet UID to BlockDevice names
	storageSetToBlockDevices map[string][]string

	desiredRAIDType string
}

func (p *Planner) init() error {
	var initFuncs = []func() error{
		p.initStorageSetMappings,
		p.initStorageSetToBlockDevices,
		p.initDesiredRAIDType,
	}
	for _, fn := range initFuncs {
		err := fn()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Planner) isReady() bool {
	var isReadyFuncs = []func() bool{
		p.isReadyByNodeCount,
		p.isReadyByNodeDiskCount,
	}
	for _, isready := range isReadyFuncs {
		if !isready() {
			return false
		}
	}
	return true
}

// isReadyByNodeCount will return false if cluster
// does not have desired nodes
//
// NOTE:
// 	This check avoids continuous disruptions to the pool.
//
// TODO: (@amitkumardas):
//	This may not be appropriate in a level triggered
// reconciliation since CStorPoolCluster should get
// reconciled as state changes.
func (p *Planner) isReadyByNodeCount() bool {
	desiredNodeCount := len(p.CStorClusterPlan.Spec.Nodes)
	if desiredNodeCount == 0 {
		glog.V(3).Infof(
			"Will skip applying CStorPoolCluster %s %s: 0 desired nodes",
			p.CStorClusterPlan.GetNamespace(), p.CStorClusterPlan.GetName(),
		)
		return false
	}
	if desiredNodeCount != len(p.ObservedStorageSets) {
		glog.V(3).Infof(
			"Will skip applying CStorPoolCluster %s %s: Desired Node(s) %d: Observed StorageSet(s) %d",
			p.CStorClusterPlan.GetNamespace(),
			p.CStorClusterPlan.GetName(),
			desiredNodeCount,
			len(p.ObservedStorageSets),
		)
		return false
	}
	return true
}

// isReadyByNodeDiskCount will return false if cluster
// does not have desired disks
//
// NOTE:
// 	This check avoids continuous disruptions to the pool.
//
// TODO: (@amitkumardas):
//	This may not be appropriate in a level triggered
// reconciliation since CStorPoolCluster should get
// reconciled as state changes.
func (p *Planner) isReadyByNodeDiskCount() bool {
	for storageSetUID, desiredDiskCount := range p.storageSetToDesiredDiskCount {
		observedDeviceCount := int64(len(p.storageSetToBlockDevices[storageSetUID]))
		if desiredDiskCount.CmpInt64(observedDeviceCount) != 0 {
			glog.V(3).Infof(
				"Will skip applying CStorPoolCluster %s %s: Desired Disk(s) %s: Observed Disks(s) %d: StorageSet UID %s",
				p.CStorClusterPlan.GetNamespace(),
				p.CStorClusterPlan.GetName(),
				desiredDiskCount.String(),
				observedDeviceCount,
				storageSetUID,
			)
			return false
		}
	}
	return true
}

// initDesiredRAIDType extracts raid type from CStorClusterConfig
// and sets it as the desired raid type to create CStorPoolCluster
func (p *Planner) initDesiredRAIDType() error {
	raidType, found, err := unstructured.NestedString(
		p.ObservedClusterConfig.UnstructuredContent(), "spec", "poolConfig", "raidType",
	)
	if err != nil {
		return errors.Wrapf(
			err,
			"Failed to get spec.poolConfig.raidType from CStorClusterConfig",
		)
	}
	if !found || raidType == "" {
		return errors.Errorf("RAID type not found in CStorClusterConfig")
	}
	p.desiredRAIDType = raidType
	return nil
}

// initStorageSetMappings builds various mappings based on
// CStorClusterStorageSet UID.
//
// NOTE:
// - Maps desired node name to CStorClusterStorageSet UID
// - Maps CStorClusterStorageSet UID to desired device count
func (p *Planner) initStorageSetMappings() error {
	p.nodeToStorageSets = map[string]string{}
	p.storageSetToDesiredDiskCount = map[string]resource.Quantity{}
	for _, sSet := range p.ObservedStorageSets {
		// node to list of StorageSet
		nodeName, found, err :=
			unstructured.NestedString(sSet.UnstructuredContent(), "spec", "node", "name")
		if err != nil {
			return err
		}
		if !found || nodeName == "" {
			return errors.Errorf(
				"Can't find spec.node.name from StorageSet %s %s:",
				sSet.GetNamespace(), sSet.GetName(),
			)
		}
		p.nodeToStorageSets[nodeName] = string(sSet.GetUID())

		// StorageSet to desired disk count
		diskCount, found, err :=
			unstructured.NestedString(sSet.UnstructuredContent(), "spec", "disk", "count")
		if err != nil {
			return err
		}
		if !found || diskCount == "" {
			return errors.Errorf(
				"Can't find spec.disk.count from StorageSet %s %s:",
				sSet.GetNamespace(), sSet.GetName(),
			)
		}
		diskCountParsed, err := resource.ParseQuantity(diskCount)
		if err != nil {
			return errors.Wrapf(
				err,
				"Failed to parse spec.disk.count %s from StoragetSet %s %s",
				diskCount, sSet.GetNamespace(), sSet.GetName(),
			)
		}
		p.storageSetToDesiredDiskCount[string(sSet.GetUID())] = diskCountParsed
	}
	return nil
}

// initStorageSetToBlockDevices maps CStorClusterStorageSet UID
// to desired BlockDevice(s)
func (p *Planner) initStorageSetToBlockDevices() error {
	p.storageSetToBlockDevices = map[string][]string{}
	for _, device := range p.ObservedBlockDevices {
		sSetUID, found := k8s.GetAnnotationForKey(
			device.GetAnnotations(), types.AnnKeyCStorClusterStorageSetUID,
		)
		if !found || sSetUID == "" {
			return errors.Errorf(
				"Can't find CStorClusterStorageSet UID at %s in BlockDevice %s %s",
				types.AnnKeyCStorClusterStorageSetUID, device.GetNamespace(), device.GetName(),
			)
		}
		desiredDevices := p.storageSetToBlockDevices[sSetUID]
		desiredDevices = append(desiredDevices, device.GetName())
		p.storageSetToBlockDevices[sSetUID] = desiredDevices
	}
	return nil
}

// buildDesiredBlockDevicesByNodeName builds that fragment of the
// CStorPoolCluster spec that deals with specifying block devices.
// The resulting fragment is based on the given node name.
func (p *Planner) buildDesiredBlockDevicesByNodeName(nodeName string) []interface{} {
	var desiredBlockDevices []interface{}
	storageSetUID := p.nodeToStorageSets[nodeName]
	blockDeviceNames := p.storageSetToBlockDevices[storageSetUID]
	for _, deviceName := range blockDeviceNames {
		new := map[string]interface{}{
			"blockDeviceName": deviceName,
		}
		desiredBlockDevices = append(desiredBlockDevices, new)
	}
	return desiredBlockDevices
}

// buildDesiredPoolByNodeName builds that fragment of CStorPoolCluster
// that deals with specifying a single pool instance. The resulting
// fragment is based on the given node name.
func (p *Planner) buildDesiredPoolByNodeName(nodeName string) interface{} {
	return map[string]interface{}{
		"nodeSelector": map[string]string{
			"kubernetes.io/hostname": nodeName,
		},
		"raidGroups": []interface{}{
			map[string]interface{}{
				"type":         p.desiredRAIDType,
				"isWriteCache": false,
				"isSpare":      false,
				"isReadCache":  false,
				"blockDevices": p.buildDesiredBlockDevicesByNodeName(nodeName),
			},
		},
		"poolConfig": map[string]interface{}{
			"defaultRaidGroupType": p.desiredRAIDType,
			"overProvisioning":     false,
			"compression":          "off",
		},
	}
}

// buildDesiredPools builds that fragment of CStorPoolCluster
// that deals with specifying all the desired pool instances.
func (p *Planner) buildDesiredPools() []interface{} {
	var pools []interface{}
	for node := range p.nodeToStorageSets {
		pool := p.buildDesiredPoolByNodeName(node)
		pools = append(pools, pool)
	}
	return pools
}

// Plan builds the desired CStorPoolCluster (i.e. CSPC) instance
func (p *Planner) Plan() (*unstructured.Unstructured, error) {
	err := p.init()
	if err != nil {
		return nil, err
	}
	if !p.isReady() {
		// no need to proceed further since cluster is not
		// ready to reconcile CStorPoolCluster
		return nil, nil
	}
	desired := &unstructured.Unstructured{}
	desired.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"apiVersion": "openebs.io/v1alpha1",
			"kind":       "CStorPoolCluster",
			"name":       p.CStorClusterPlan.GetName(),
			"namespace":  p.CStorClusterPlan.GetNamespace(),
			"annotations": map[string]interface{}{
				string(types.AnnKeyCStorClusterPlanUID):   p.CStorClusterPlan.GetUID(),
				string(types.AnnKeyCStorClusterConfigUID): p.ObservedClusterConfig.GetUID(),
			},
		},
		"spec": map[string]interface{}{
			"pools": p.buildDesiredPools(),
		},
	})
	return desired, nil
}
