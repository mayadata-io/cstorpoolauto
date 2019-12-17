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
	"k8s.io/apimachinery/pkg/util/json"
	"openebs.io/metac/controller/generic"

	"cstorpoolauto/k8s"
	"cstorpoolauto/types"
	"cstorpoolauto/util/metac"
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

// RAIDTypeToDefaultMinDiskCount maps pool instance's raid type
// to its default minimum disk count
var RAIDTypeToDefaultMinDiskCount = map[types.PoolRAIDType]int64{
	types.PoolRAIDTypeMirror: 2,
	types.PoolRAIDTypeStripe: 1,
	types.PoolRAIDTypeRAIDZ:  3,
	types.PoolRAIDTypeRAIDZ2: 6,
}

type reconcileErrHandler struct {
	clusterConfig *unstructured.Unstructured
	hookResponse  *generic.SyncHookResponse
}

func (h *reconcileErrHandler) handle(err error) {
	// Error has been handled elaborately. This logic ensures
	// error message is propagated to the resource & hence seen via
	// 'kubectl get CStorClusterConfig -oyaml'.
	//
	// In addition, logging has been done to check for error messages
	// from this pod's logs.
	glog.Errorf(
		"Failed to reconcile CStorClusterConfig %s %s: %+v",
		h.clusterConfig.GetNamespace(), h.clusterConfig.GetName(), err,
	)

	conds, mergeErr :=
		k8s.MergeStatusConditions(
			h.clusterConfig, types.MakeCStorClusterConfigReconcileErrCond(err),
		)
	if mergeErr != nil {
		glog.Errorf(
			"Failed to reconcile CStorClusterConfig %s %s: Can't set status conditions: %+v",
			h.clusterConfig.GetNamespace(), h.clusterConfig.GetName(), mergeErr,
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
		h.hookResponse.Status["phase"] = types.CStorClusterConfigStatusPhaseError
		h.hookResponse.Status["conditions"] = conds
	}
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
// controller to run continuously. Hence, the errors are logged and at
// the same time, these errors are posted against CStorClusterConfig's
// status.
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
		response.SkipReconcile = true
		return nil
	}

	glog.V(3).Infof(
		"Will reconcile CStorClusterConfig %s %s:",
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
			uid, _ := k8s.GetAnnotationForKey(
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
	CStorClusterConfig *types.CStorClusterConfig
	CStorClusterPlan   *types.CStorClusterPlan
	Resources          []*unstructured.Unstructured
	NodePlanner        *NodePlanner
}

// ReconcileResponse is a helper struct used to form the response
// of a successful reconciliation
type ReconcileResponse struct {
	CStorClusterConfig *unstructured.Unstructured
	CStorClusterPlan   *unstructured.Unstructured
}

// NewReconciler returns a new instance of Reconciler
func NewReconciler(
	clusterConfig *unstructured.Unstructured,
	clusterPlan *unstructured.Unstructured,
	resources []*unstructured.Unstructured,
) (*Reconciler, error) {
	// transform CStorClusterConfig from unstructured to typed
	var cstorClusterConfigTyped types.CStorClusterConfig
	cstorClusterConfigRaw, err := clusterConfig.MarshalJSON()
	if err != nil {
		return nil, errors.Wrapf(err, "Can't marshal CStorClusterConfig")
	}
	err = json.Unmarshal(cstorClusterConfigRaw, &cstorClusterConfigTyped)
	if err != nil {
		return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterConfig")
	}

	// transform CStorClusterPlan from unstructured to typed
	var cstorClusterPlanTyped types.CStorClusterPlan
	if clusterPlan != nil {
		cstorClusterPlanRaw, err := clusterPlan.MarshalJSON()
		if err != nil {
			return nil, errors.Wrapf(err, "Can't marshal CStorClusterPlan")
		}
		err = json.Unmarshal(cstorClusterPlanRaw, &cstorClusterPlanTyped)
		if err != nil {
			return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterPlan")
		}
	}

	return &Reconciler{
		CStorClusterConfig: &cstorClusterConfigTyped,
		CStorClusterPlan:   &cstorClusterPlanTyped,
		Resources:          resources,
		NodePlanner: &NodePlanner{
			NodeSelector: cstorClusterConfigTyped.Spec.AllowedNodes,
			Resources:    resources,
		},
	}, nil
}

// Reconcile runs through the reconciliation logic
//
// NOTE:
//	Due care has been taken to let this logic be idempotent
func (r *Reconciler) Reconcile() (ReconcileResponse, error) {
	syncFns := []func() error{
		r.validateClusterConfigAndSetDefaultsIfNotSet,
		r.syncClusterPlan,
	}
	for _, syncFn := range syncFns {
		err := syncFn()
		if err != nil {
			return ReconcileResponse{}, err
		}
	}
	r.resetClusterConfigReconcileErrorIfAny()
	return r.makeReconcileResponse()
}

// makeReconcileResponse builds reconcile response
func (r *Reconciler) makeReconcileResponse() (ReconcileResponse, error) {
	// convert updated CStorClusterConfig from typed to unstruct
	clusterConfigRaw, err := json.Marshal(r.CStorClusterConfig)
	if err != nil {
		return ReconcileResponse{},
			errors.Wrapf(err, "Can't marshal CStorClusterConfig")
	}
	var clusterConfig unstructured.Unstructured
	err = json.Unmarshal(clusterConfigRaw, &clusterConfig)
	if err != nil {
		return ReconcileResponse{},
			errors.Wrapf(err, "Can't unmarshal CStorClusterConfig")
	}
	// convert updated CStorClusterConfigPlan from typed to unstruct
	clusterConfigPlanRaw, err := json.Marshal(r.CStorClusterPlan)
	if err != nil {
		return ReconcileResponse{},
			errors.Wrapf(err, "Can't marshal CStorClusterPlan")
	}
	var clusterConfigPlan unstructured.Unstructured
	err = json.Unmarshal(clusterConfigPlanRaw, &clusterConfigPlan)
	if err != nil {
		return ReconcileResponse{},
			errors.Wrapf(err, "Can't unmarshal CStorClusterPlan")
	}

	return ReconcileResponse{
		CStorClusterConfig: &clusterConfig,
		CStorClusterPlan:   &clusterConfigPlan,
	}, nil
}

// resetClusterConfigReconcileErrorIfAny removes ReconcileError
// if any associated with CStorClusterConfig. This removes
// ReconcileError if it ever happened during in previous
// reconciliations.
func (r *Reconciler) resetClusterConfigReconcileErrorIfAny() {
	types.MergeNoReconcileErrorOnCStorClusterConfig(r.CStorClusterConfig)
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
	if r.CStorClusterPlan != nil {
		observedNodes = r.CStorClusterPlan.Spec.Nodes
	} else {
		r.CStorClusterPlan = &types.CStorClusterPlan{}
		r.CStorClusterPlan.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   types.GroupDAOMayaDataIO,
			Version: types.VersionV1Alpha1,
			Kind:    string(types.KindCStorClusterPlan),
		})
		// name & namespace are same as CStorClusterConfig
		r.CStorClusterPlan.SetName(r.CStorClusterConfig.GetName())
		r.CStorClusterPlan.SetNamespace(r.CStorClusterConfig.GetNamespace())
		// set CStorClusterConfig UID as an annotation for easy
		// mapping at later stages of the workflow
		r.CStorClusterPlan.SetAnnotations(
			k8s.MergeToAnnotations(
				types.AnnKeyCStorClusterConfigUID,
				string(r.CStorClusterConfig.GetUID()),
				r.CStorClusterPlan.GetAnnotations(),
			),
		)
	}

	// Plan should be invoked only after CStorClusterConfig is
	// set with defaults.
	//
	// These defaults are passed via NodePlannerConfig to help
	// finding the eligible nodes that are fit to form
	// CStorPoolCluster
	desired, err := r.NodePlanner.Plan(NodePlannerConfig{
		ObservedNodes: observedNodes,
		MinPoolCount:  r.CStorClusterConfig.Spec.MinPoolCount,
		MaxPoolCount:  r.CStorClusterConfig.Spec.MaxPoolCount,
	})
	if err != nil {
		return err
	}
	if len(desired) == 0 {
		return errors.Errorf("No elgible nodes were found")
	}
	// desired nodes are set against CStorClusterPlan & not
	// against CStorClusterConfig
	//
	// NOTE: (design decision)
	//	We could have used these resulting/desired nodes to be
	// set against cstorClusterConfig.status. However, we
	// prefered to use a dedicated resource i.e. CStorClusterPlan.
	// A dedicated resource will provide a granular workflow
	// to build up the CStorPoolCluster resource.
	r.CStorClusterPlan.Spec.Nodes = desired
	return nil
}

func (r *Reconciler) validateClusterConfigAndSetDefaultsIfNotSet() error {
	// these default funcs follow a pre-defined order
	//
	// NOTE:
	// 	Ensure this ordering is **not** changed
	setDefaultFns := []func() error{
		r.validateDiskExternalProvisioner,
		r.setMinPoolCountIfNotSet,
		r.setMaxPoolCountIfNotSet,
		r.setRAIDTypeIfNotSet,
		r.setMinDiskCountIfNotSet,
		r.setMinDiskCapacityIfNotSet,
	}
	for _, setDefaultFn := range setDefaultFns {
		err := setDefaultFn()
		if err != nil {
			return err
		}
	}
	return nil
}

// setMinPoolCountIfNotSet sets the min pool counts. Minimum pool
// count is set to the lowest value amongst the following:
// - Number of worker nodes
// - Number of eligible nodes from node selector policy
// - A default min value
func (r *Reconciler) setMinPoolCountIfNotSet() error {
	if r.CStorClusterConfig.Spec.MinPoolCount.CmpInt64(0) == -1 {
		return errors.Errorf(
			"Invalid MinPoolCount %d: Want positive value",
			r.CStorClusterConfig.Spec.MinPoolCount.Value(),
		)
	}
	if r.CStorClusterConfig.Spec.MinPoolCount.CmpInt64(0) != 0 {
		// don't set default value if it is already configured
		return nil
	}
	availableNodeCount := r.NodePlanner.GetAllNodeCount()
	eligibleNodeCount, err := r.NodePlanner.GetAllowedNodeCount()
	if err != nil {
		return err
	}
	// start by setting min pool count to default value
	minPoolCount := DefaultMinPoolCount
	if availableNodeCount < minPoolCount {
		// use the lowest value
		minPoolCount = availableNodeCount
	}
	if eligibleNodeCount < minPoolCount {
		// use the lowest value
		minPoolCount = eligibleNodeCount
	}
	if minPoolCount <= 0 {
		return errors.Errorf("MinPoolCount can't be 0: Preferred nodes not found")
	}
	// set the min value
	r.CStorClusterConfig.Spec.MinPoolCount.Set(minPoolCount)
	return nil
}

func (r *Reconciler) setMaxPoolCountIfNotSet() error {
	if r.CStorClusterConfig.Spec.MaxPoolCount.CmpInt64(0) == 0 {
		// max pool count is not set, so set it as min + 2
		r.CStorClusterConfig.Spec.MaxPoolCount.Set(
			r.CStorClusterConfig.Spec.MinPoolCount.Value() + 2,
		)
		return nil
	}
	if r.CStorClusterConfig.Spec.MinPoolCount.Cmp(r.CStorClusterConfig.Spec.MaxPoolCount) == 1 {
		return errors.Errorf(
			"MaxPoolCount %d can't be less than MinPoolCount %d",
			r.CStorClusterConfig.Spec.MaxPoolCount.Value(),
			r.CStorClusterConfig.Spec.MinPoolCount.Value(),
		)
	}
	return nil
}

func (r *Reconciler) setRAIDTypeIfNotSet() error {
	if r.CStorClusterConfig.Spec.PoolConfig.RAIDType == "" {
		r.CStorClusterConfig.Spec.PoolConfig.RAIDType = types.PoolRAIDTypeDefault
		return nil
	}
	switch r.CStorClusterConfig.Spec.PoolConfig.RAIDType {
	case types.PoolRAIDTypeStripe, types.PoolRAIDTypeMirror,
		types.PoolRAIDTypeRAIDZ, types.PoolRAIDTypeRAIDZ2:
		// do nothing
	default:
		return errors.Errorf(
			"Invalid RAID type %s", r.CStorClusterConfig.Spec.PoolConfig.RAIDType,
		)
	}
	return nil
}

func (r *Reconciler) setMinDiskCountIfNotSet() error {
	if r.CStorClusterConfig.Spec.DiskConfig.MinCount.CmpInt64(0) == -1 {
		return errors.Errorf(
			"Invalid MinDiskCount %d: Want positive value",
			r.CStorClusterConfig.Spec.DiskConfig.MinCount.Value(),
		)
	}
	if r.CStorClusterConfig.Spec.DiskConfig.MinCount.CmpInt64(0) == 1 {
		// no need to set default since it is already configured
		return nil
	}
	r.CStorClusterConfig.Spec.DiskConfig.MinCount.Set(
		RAIDTypeToDefaultMinDiskCount[r.CStorClusterConfig.Spec.PoolConfig.RAIDType],
	)
	return nil
}

func (r *Reconciler) setMinDiskCapacityIfNotSet() error {
	if r.CStorClusterConfig.Spec.DiskConfig.MinCapacity.CmpInt64(0) == -1 {
		return errors.Errorf(
			"Invalid MinDiskCapacity %s: Want positive value",
			r.CStorClusterConfig.Spec.DiskConfig.MinCapacity.String(),
		)
	}
	if r.CStorClusterConfig.Spec.DiskConfig.MinCapacity.CmpInt64(0) != 0 {
		// no need to set default since it is already configured
		return nil
	}
	r.CStorClusterConfig.Spec.DiskConfig.MinCapacity = DefaultMinDiskCapacity
	return nil
}

func (r *Reconciler) validateDiskExternalProvisioner() error {
	if r.CStorClusterConfig.Spec.DiskConfig.ExternalProvisioner.CSIAttacherName == "" ||
		r.CStorClusterConfig.Spec.DiskConfig.ExternalProvisioner.StorageClassName == "" {
		return errors.Errorf(
			"Invalid disk ExternalProvisioner: Both csi attacher & storageclass are required",
		)
	}
	return nil
}
