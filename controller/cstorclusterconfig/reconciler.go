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

package cstorclusterconfig

import (
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"openebs.io/metac/controller/generic"

	"mayadata.io/cstorpoolauto/common/metac"
	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
)

const (
	// DefaultMinPoolCount is the default value for minimum pool
	// count
	//
	// NOTE:
	//	This value is used when min pool count is not set
	//
	// NOTE:
	//	The final default value is determined based on various
	// factors e.g. current state of cluster, etc. in addition to
	// this constant number.
	DefaultMinPoolCount int64 = 3
)

// DefaultMinDiskCapacity is the default min disk capacity
var DefaultMinDiskCapacity resource.Quantity = resource.MustParse("100Gi")

type reconcileErrHandler struct {
	clusterConfig *unstructured.Unstructured
	hookResponse  *generic.SyncHookResponse
}

func (h *reconcileErrHandler) handle(err error) {
	glog.Errorf(
		"Failed to reconcile CStorClusterConfig %q / %q: %+v",
		h.clusterConfig.GetNamespace(), h.clusterConfig.GetName(), err,
	)

	// this will stop further reconciliation at metac since there was
	// an error
	h.hookResponse.SkipReconcile = true
}

// Sync implements the idempotent logic to set CStorClusterConfig
// with its defaults i.e. defaulting controller. In addition,
// it applies a CStorClusterPlan resource to trigger the workflow
// of creating cstor pool instances across a set of eligible nodes
// in the current cluster.
//
// NOTE:
// 	SyncHookRequest is the payload received as part of reconcile
// request. Similarly, SyncHookResponse is the payload sent as a
// response as part of reconcile request.
//
// NOTE:
//	SyncHookRequest uses CStorClusterConfig as the watched resource.
// SyncHookResponse has the resources that forms the desired state
// w.r.t the watched resource.
//
// NOTE:
//	Returning error will panic this process. We would rather want this
// controller to run continuously. Hence, the errors are logged.
func Sync(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	if request == nil {
		return errors.Errorf("Failed to reconcile CStorClusterConfig: Nil request found")
	}
	if response == nil {
		return errors.Errorf("Failed to reconcile CStorClusterConfig: Nil response found")
	}
	// Nothing needs to be done if there are no attachments in request
	//
	// NOTE:
	// 	It is expected to have CStorClusterConfig as an attachment
	// resource as well as the resource under watch.
	if request.Attachments == nil || request.Attachments.IsEmpty() {
		glog.V(3).Infof(
			"Will skip reconciliation: Nil attachments: CStorClusterConfig %q / %q",
			request.Watch.GetNamespace(), request.Watch.GetName(),
		)
		response.SkipReconcile = true
		return nil
	}

	glog.V(3).Infof(
		"Will reconcile CStorClusterConfig %q / %q:",
		request.Watch.GetNamespace(), request.Watch.GetName(),
	)

	// construct the error handler
	errHandler := &reconcileErrHandler{
		clusterConfig: request.Watch,
		hookResponse:  response,
	}

	var cstorClusterConfigObj *unstructured.Unstructured
	var cstorClusterPlanObj *unstructured.Unstructured
	for _, attachment := range request.Attachments.List() {
		// this watch resource must be present in the list of attachments
		if request.Watch.GetUID() == attachment.GetUID() &&
			attachment.GetKind() == string(types.KindCStorClusterConfig) {
			// this is the required CStorClusterConfig
			cstorClusterConfigObj = attachment
			// add this to the response later after completion of its
			// reconcile logic
			continue
		}
		if attachment.GetKind() == string(types.KindCStorClusterPlan) {
			// verify further if CStorClusterPlan is what we are looking
			uid, _ := unstruct.GetValueForKey(
				attachment.GetAnnotations(), types.AnnKeyCStorClusterConfigUID,
			)
			if string(request.Watch.GetUID()) == uid {
				// this is the desired CStorClusterPlan
				cstorClusterPlanObj = attachment
				continue
			}
		}
		response.Attachments = append(response.Attachments, attachment)
	}

	if cstorClusterConfigObj == nil {
		errHandler.handle(
			errors.Errorf("Can't reconcile: CStorClusterConfig not found in attachments"),
		)
		return nil
	}

	// reconciler is the one that will perform reconciliation of
	// CStorClusterConfig resource
	reconciler, err :=
		NewReconciler(
			cstorClusterConfigObj,
			cstorClusterPlanObj,
			request.Attachments.List(),
		)
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	op, err := reconciler.Reconcile()
	if err != nil {
		errHandler.handle(err)
		return nil
	}
	if op.SkipReconcile {
		// skip reconciliation at metac
		response.SkipReconcile = true
		glog.V(3).Infof(
			"Will skip reconciliation: %s: CStorClusterConfig %q / %q",
			op.SkipReason, request.Watch.GetNamespace(), request.Watch.GetName(),
		)
		return nil
	}

	// add updated CStorClusterConfig & CStorClusterConfigPlan to response
	response.Attachments = append(response.Attachments, op.CStorClusterConfig)
	response.Attachments = append(response.Attachments, op.CStorClusterPlan)

	glog.V(2).Infof(
		"CStorClusterConfig %s %s reconciled successfully: %s",
		request.Watch.GetNamespace(), request.Watch.GetName(),
		metac.GetDetailsFromResponse(response),
	)
	return nil
}

// Reconciler enables reconciliation of CStorClusterConfig instance
type Reconciler struct {
	ClusterConfig *types.CStorClusterConfig
	ClusterPlan   *types.CStorClusterPlan
	Resources     []*unstructured.Unstructured
	NodePlanner   *NodePlanner

	// values that get validated / defaulted before finally get into
	// the desired state
	minPoolCount    int64
	maxPoolCount    int64
	poolRAIDType    types.PoolRAIDType
	minDiskCount    int64
	minDiskCapacity int64

	// nodes that form the desired CStorClusterPlan
	desiredNodes []types.CStorClusterPlanNode
}

// ReconcileResponse is a helper struct used to form the response
// of a successful reconciliation
type ReconcileResponse struct {
	CStorClusterConfig *unstructured.Unstructured
	CStorClusterPlan   *unstructured.Unstructured
	SkipReconcile      bool
	SkipReason         string
}

// NewReconciler returns a new instance of Reconciler
func NewReconciler(
	clusterConfig *unstructured.Unstructured,
	clusterPlan *unstructured.Unstructured,
	resources []*unstructured.Unstructured,
) (*Reconciler, error) {
	r := &Reconciler{
		Resources: resources,
		NodePlanner: &NodePlanner{
			Resources: resources,
		},
	}

	// transform CStorClusterConfig from unstructured to typed
	var clusterConfigTyped types.CStorClusterConfig
	err := unstruct.UnstructToTyped(clusterConfig, &clusterConfigTyped)
	if err != nil {
		return nil, err
	}

	// update the reconciler instance with config & related fields
	r.ClusterConfig = &clusterConfigTyped
	r.NodePlanner.NodeSelector = r.ClusterConfig.Spec.AllowedNodes

	// transform CStorClusterPlan from unstructured to typed
	if clusterPlan != nil {
		var clusterPlanTyped types.CStorClusterPlan
		err := unstruct.UnstructToTyped(clusterPlan, &clusterPlanTyped)
		if err != nil {
			return nil, err
		}
		// update the reconciler instance with typed CStorClusterPlan
		r.ClusterPlan = &clusterPlanTyped
	}

	return r, nil
}

// Reconcile runs through the reconciliation logic
//
// NOTE:
//	Due care has been taken to let this logic be idempotent
func (r *Reconciler) Reconcile() (ReconcileResponse, error) {
	if !r.isExternalDiskConfig() {
		// this controller is meant for external disk config only
		return ReconcileResponse{
			SkipReconcile: true,
			SkipReason:    "External disk config not found",
		}, nil
	}
	syncFns := []func() error{
		r.syncClusterConfig,
		r.syncClusterPlan,
	}
	for _, syncFn := range syncFns {
		err := syncFn()
		if err != nil {
			return ReconcileResponse{}, err
		}
	}
	return r.makeReconcileResponse(), nil
}

// makeReconcileResponse builds reconcile response
func (r *Reconciler) makeReconcileResponse() ReconcileResponse {
	return ReconcileResponse{
		CStorClusterConfig: r.getDesiredClusterConfig(),
		CStorClusterPlan:   r.getDesiredClusterPlan(r.desiredNodes),
	}
}

// syncClusterPlan synchronises the CStorClusterPlan resource
// based on current specifications at CStorClusterConfig object
// and observed nodes at the cluster
//
// NOTE:
//	This should be invoked only after CStorClusterConfig is set
// with defaults if required.
func (r *Reconciler) syncClusterPlan() error {
	var observedNodes []types.CStorClusterPlanNode
	if r.ClusterPlan != nil {
		observedNodes = r.ClusterPlan.Spec.Nodes
	}
	// Plan should be invoked only after CStorClusterConfig is
	// set with defaults.
	//
	// The CStorClusterConfig defaults are passed via
	// NodePlannerConfig to help finding the eligible nodes
	// that are fit to form CStorPoolCluster
	nodes, err := r.NodePlanner.Plan(NodePlannerConfig{
		ObservedNodes: observedNodes,
		MinPoolCount:  *resource.NewQuantity(r.minPoolCount, resource.DecimalExponent),
		MaxPoolCount:  *resource.NewQuantity(r.maxPoolCount, resource.DecimalExponent),
	})
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.Errorf("No elgible nodes were found")
	}
	r.desiredNodes = nodes
	return nil
}

func (r *Reconciler) getDesiredClusterPlan(
	desiredNodes []types.CStorClusterPlanNode,
) *unstructured.Unstructured {
	plan := &unstructured.Unstructured{}
	plan.SetUnstructuredContent(
		map[string]interface{}{
			"spec": map[string]interface{}{
				"nodes": types.MakeListMapOfPlanNodes(desiredNodes),
			},
		},
	)
	plan.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   string(types.GroupDAOMayaDataIO),
		Version: string(types.VersionV1Alpha1),
		Kind:    string(types.KindCStorClusterPlan),
	})
	// name & namespace are same as CStorClusterConfig
	plan.SetName(r.ClusterConfig.GetName())
	plan.SetNamespace(r.ClusterConfig.GetNamespace())
	// create annotations that refer to CStorClusterConfig UID
	plan.SetAnnotations(map[string]string{
		types.AnnKeyCStorClusterConfigUID: string(r.ClusterConfig.GetUID()),
	})

	return plan
}

func (r *Reconciler) syncClusterConfig() error {
	// these default funcs follow a pre-defined order
	//
	// NOTE:
	// 	Ensure this ordering is **not** changed
	setDefaultFns := []func() error{
		// pre checks
		r.validateDiskConfig,
		r.validateExternalDiskConfig,
		// set to defaults if not set
		r.setMinPoolCountIfNotSet,
		r.setMaxPoolCountIfNotSet,
		r.setRAIDTypeIfNotSet,
		r.setMinDiskCountIfNotSet,
		r.setMinDiskCapacityIfNotSet,
		// post checks
		r.validateRAIDType,
		r.validateMinDiskCount,
	}
	for _, setDefaultFn := range setDefaultFns {
		err := setDefaultFn()
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) getDesiredClusterConfig() *unstructured.Unstructured {
	config := &unstructured.Unstructured{}
	config.SetUnstructuredContent(
		map[string]interface{}{
			"spec": map[string]interface{}{
				"minPoolCount": r.minPoolCount,
				"maxPoolCount": r.maxPoolCount,
				"diskConfig": map[string]interface{}{
					"minCapacity": r.minDiskCapacity,
					"minCount":    r.minDiskCount,
				},
				"poolConfig": map[string]interface{}{
					"raidType": string(r.poolRAIDType),
				},
			},
		},
	)
	config.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   string(types.GroupDAOMayaDataIO),
		Version: string(types.VersionV1Alpha1),
		Kind:    string(types.KindCStorClusterConfig),
	})
	// name & namespace are same as CStorClusterConfig
	config.SetName(r.ClusterConfig.GetName())
	config.SetNamespace(r.ClusterConfig.GetNamespace())
	return config
}

// setMinPoolCountIfNotSet sets the min pool counts. Minimum pool
// count is set to the lowest value amongst the following:
// - Number of worker nodes
// - Number of eligible nodes from node selector policy
// - A default min value
func (r *Reconciler) setMinPoolCountIfNotSet() error {
	var minPoolCount int64
	minPoolCount = r.ClusterConfig.Spec.MinPoolCount.Value()
	if minPoolCount < 0 {
		return errors.Errorf(
			"Invalid MinPoolCount %d: Want positive value", minPoolCount,
		)
	}
	if minPoolCount > 0 {
		// don't set default value if it is already configured
		r.minPoolCount = minPoolCount
		return nil
	}
	// at this point min pool count may not have been set
	// & hence should be having 0 as its value
	//
	// TODO (@amitkumardas):
	//	It might be better to have min pool count as a pointer
	// to differentiate between a value that is not set vs.
	// value set to 0
	availableNodeCount := r.NodePlanner.GetAllNodeCount()
	eligibleNodeCount, err := r.NodePlanner.GetAllowedNodeCountOrCached()
	if err != nil {
		return err
	}
	// start by setting min pool count to default value
	minPoolCount = DefaultMinPoolCount
	if availableNodeCount < minPoolCount {
		// use the lowest of two
		minPoolCount = availableNodeCount
	}
	if eligibleNodeCount < minPoolCount {
		// use the lowest of two
		minPoolCount = eligibleNodeCount
	}
	// if min comes out to be 0 or negative then we don't
	// support it
	//
	// TODO (@amitkumardas):
	//	0 may be a valid value that we might want to consider
	// e.g. scale down to 0 pools
	if minPoolCount <= 0 {
		return errors.Errorf(
			"Preferred nodes not found: MinPoolCount evaluates to %d:",
			minPoolCount,
		)
	}
	// store the min pool count to be returned later as desired state
	r.minPoolCount = minPoolCount
	return nil
}

func (r *Reconciler) setMaxPoolCountIfNotSet() error {
	var maxPoolCount int64
	var minPoolCount int64
	// it is expected to have minPoolCount field to be
	// already set before invoking this method
	minPoolCount = r.minPoolCount
	maxPoolCount = r.ClusterConfig.Spec.MaxPoolCount.Value()
	if maxPoolCount == 0 {
		// max pool count is not set, so set it as min + 2
		// & return
		r.maxPoolCount = minPoolCount + 2
		return nil
	}
	// check further if min is greater than max which is an error
	if minPoolCount > maxPoolCount {
		return errors.Errorf(
			"MaxPoolCount %d can't be less than MinPoolCount %d",
			maxPoolCount,
			minPoolCount,
		)
	}
	// store the value to be returned later as the desired state
	r.maxPoolCount = maxPoolCount
	return nil
}

func (r *Reconciler) setRAIDTypeIfNotSet() error {
	r.poolRAIDType = r.ClusterConfig.Spec.PoolConfig.RAIDType
	if r.poolRAIDType == "" {
		r.poolRAIDType = types.PoolRAIDTypeDefault
	}
	return nil
}

func (r *Reconciler) setMinDiskCountIfNotSet() error {
	var minDiskCount int64
	minDiskCount = r.ClusterConfig.Spec.DiskConfig.MinCount.Value()
	if minDiskCount < 0 {
		return errors.Errorf(
			"Invalid MinDiskCount %d: Want positive value", minDiskCount,
		)
	}
	if minDiskCount > 0 {
		// no need to set default since it is already configured
		r.minDiskCount = minDiskCount
		return nil
	}
	// at this point, min disk count was not set
	// hence, set it to default min value based on RAIDType
	//
	// TODO (@amitkumardas):
	//  It may be good to use pointer to represent MinCount.
	// This will help in differentiating a value that was not
	// set vs. a value that was set to 0.
	r.minDiskCount =
		types.RAIDTypeToDefaultMinDiskCount[r.poolRAIDType]
	return nil
}

func (r *Reconciler) setMinDiskCapacityIfNotSet() error {
	var minDiskCapacity int64
	minDiskCapacity = r.ClusterConfig.Spec.DiskConfig.MinCapacity.Value()
	if minDiskCapacity < 0 {
		return errors.Errorf(
			"Invalid MinDiskCapacity %s: Want positive value",
			r.ClusterConfig.Spec.DiskConfig.MinCapacity.String(),
		)
	}
	if minDiskCapacity > 0 {
		// no need to set default since it is already configured
		r.minDiskCapacity = minDiskCapacity
		return nil
	}
	r.minDiskCapacity = DefaultMinDiskCapacity.Value()
	return nil
}

func (r *Reconciler) validateRAIDType() error {
	// verify if the RAID type that was set against the resource is valid
	switch r.poolRAIDType {
	case types.PoolRAIDTypeStripe, types.PoolRAIDTypeMirror,
		types.PoolRAIDTypeRAIDZ, types.PoolRAIDTypeRAIDZ2:
		// do nothing
	default:
		return errors.Errorf(
			"Invalid RAID type %s", r.poolRAIDType,
		)
	}
	return nil
}

func (r *Reconciler) validateMinDiskCount() error {
	diskCount := r.minDiskCount
	if diskCount == 0 {
		return errors.Errorf(
			"Invalid min disk count '0'",
		)
	}
	defaultCount := types.RAIDTypeToDefaultMinDiskCount[r.poolRAIDType]
	if defaultCount == 0 {
		return errors.Errorf(
			"Can't eval default disk count: RAID type %q is not set", r.poolRAIDType,
		)
	}
	if diskCount%defaultCount != 0 {
		return errors.Errorf(
			"Invalid disk count %d: Want multiples of %d", diskCount, defaultCount,
		)
	}
	return nil
}

// isExternalDiskConfig returns true if this CStorClusterConfig
// needs to make use of external disk config
//
// NOTE:
// 	This returns true if both local as well as external disk configs
// are not set.
func (r *Reconciler) isExternalDiskConfig() bool {
	if r.ClusterConfig.Spec.DiskConfig.ExternalDiskConfig != nil {
		return true
	}
	// defaults to external disk config if local disk config is not set
	return r.ClusterConfig.Spec.DiskConfig.LocalDiskConfig == nil
}

func (r *Reconciler) validateDiskConfig() error {
	if r.ClusterConfig.Spec.DiskConfig.ExternalDiskConfig != nil &&
		r.ClusterConfig.Spec.DiskConfig.LocalDiskConfig != nil {
		return errors.Errorf(
			"Invalid disk config: Either external or local config needed, not both",
		)
	}
	return nil
}

func (r *Reconciler) validateExternalDiskConfig() error {
	if r.ClusterConfig.Spec.DiskConfig.ExternalDiskConfig == nil {
		return errors.Errorf("Nil external disk config")
	}
	if r.ClusterConfig.Spec.DiskConfig.ExternalDiskConfig.CSIAttacherName == "" ||
		r.ClusterConfig.Spec.DiskConfig.ExternalDiskConfig.StorageClassName == "" {
		return errors.Errorf(
			"Invalid external disk config: Both csi attacher & storageclass are required",
		)
	}
	return nil
}
