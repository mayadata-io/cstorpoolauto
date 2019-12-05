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

package reconciler

import (
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/json"
	"openebs.io/metac/controller/generic"

	"cstorpoolauto/controller/clusterconfig/node"
	"cstorpoolauto/k8s"
	"cstorpoolauto/types"
)

const (
	// DefaultMinPoolCount is the default value for minimum pool
	// count
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
		"Failed to reconcile CStorClusterConfig %s: %v", h.clusterConfig.GetName(), err,
	)

	conds, mergeErr :=
		k8s.MergeStatusConditions(
			h.clusterConfig, types.MakeCStorClusterConfigReconcileErrCond(err),
		)
	if mergeErr != nil {
		glog.Errorf(
			"Failed to reconcile CStorClusterConfig %s: Can't set status conditions: %v",
			h.clusterConfig.GetName(), mergeErr,
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

	// stop further reconciliation since there was an error
	h.hookResponse.SkipReconcile = true
}

// Sync implements the idempotent logic to set CStorClusterConfig
// with its defaults. CStorClusterConfig updated with defaults is
// sent as response attachment. However, any error while updating
// with defaults is sent as response status.
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
	response = &generic.SyncHookResponse{}

	// nothing needs to be done if request is empty
	if request == nil || request.Attachments == nil || request.Attachments.IsEmpty() {
		response.SkipReconcile = true
		return nil
	}

	// construct the error handler
	errHandler := &reconcileErrHandler{
		clusterConfig: request.Watch,
		hookResponse:  response,
	}

	var cstorClusterConfigObj *unstructured.Unstructured
	var cstorClusterPlanObj *unstructured.Unstructured

	for _, attachment := range request.Attachments.List() {
		// watched resource is also present in attachments
		if request.Watch.GetUID() == attachment.GetUID() {
			// keep this to update during reconcile & add the
			// updated copy to the response's attachments
			cstorClusterConfigObj = attachment
			continue
		}
		if attachment.GetKind() == string(k8s.KindCStorClusterPlan) {
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

	// invoke the logic to set defaults against CStorClusterConfig
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

	return nil
}

// Reconciler enables reconciliation of CStorClusterConfig instance
type Reconciler struct {
	CStorClusterConfig *types.CStorClusterConfig
	CStorClusterPlan   *types.CStorClusterPlan
	Attachments        []*unstructured.Unstructured
	NodeEvaluator      *node.CStorClusterConfigNodeEvaluator
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
	attachments []*unstructured.Unstructured,
) (*Reconciler, error) {
	// transform cluster config from unstructured to typed
	var cstorClusterConfigTyped types.CStorClusterConfig
	cstorClusterConfigRaw, err := clusterConfig.MarshalJSON()
	if err != nil {
		return nil, errors.Wrapf(err, "Can't marshal CStorClusterConfig")
	}
	err = json.Unmarshal(cstorClusterConfigRaw, &cstorClusterConfigTyped)
	if err != nil {
		return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterConfig")
	}

	// transforms cluster config plan from unstructured to typed
	var cstorClusterPlanTyped *types.CStorClusterPlan
	if clusterPlan != nil {
		cstorClusterPlanRaw, err := clusterPlan.MarshalJSON()
		if err != nil {
			return nil, errors.Wrapf(err, "Can't marshal CStorClusterPlan")
		}
		err = json.Unmarshal(cstorClusterPlanRaw, cstorClusterPlanTyped)
		if err != nil {
			return nil, errors.Wrapf(err, "Can't unmarshal CStorClusterPlan")
		}
	}

	return &Reconciler{
		CStorClusterConfig: &cstorClusterConfigTyped,
		CStorClusterPlan:   cstorClusterPlanTyped,
		Attachments:        attachments,
		NodeEvaluator: &node.CStorClusterConfigNodeEvaluator{
			CStorClusterConfig: &cstorClusterConfigTyped,
			Attachments:        attachments,
		},
	}, nil
}

// Reconcile runs through the reconciliation logic
//
// NOTE:
//	Due care has been taken to let this logic be idempotent
func (s *Reconciler) Reconcile() (ReconcileResponse, error) {
	syncFns := []func() error{
		s.validateClusterConfigAndSetDefaultsIfNotSet,
		s.buildClusterPlan,
	}
	for _, syncFn := range syncFns {
		err := syncFn()
		if err != nil {
			return ReconcileResponse{}, err
		}
	}
	s.resetClusterConfigReconcileErrorIfAny()
	return s.makeReconcileResponse()
}

// makeReconcileResponse builds reconcile response
func (s *Reconciler) makeReconcileResponse() (ReconcileResponse, error) {
	// convert updated CStorClusterConfig from typed to unstruct
	clusterConfigRaw, err := json.Marshal(s.CStorClusterConfig)
	if err != nil {
		return ReconcileResponse{}, err
	}
	var clusterConfig unstructured.Unstructured
	err = json.Unmarshal(clusterConfigRaw, &clusterConfig)
	if err != nil {
		return ReconcileResponse{}, err
	}
	// convert updated CStorClusterConfigPlan from typed to unstruct
	clusterConfigPlanRaw, err := json.Marshal(s.CStorClusterPlan)
	if err != nil {
		return ReconcileResponse{}, err
	}
	var clusterConfigPlan unstructured.Unstructured
	err = json.Unmarshal(clusterConfigPlanRaw, &clusterConfigPlan)
	if err != nil {
		return ReconcileResponse{}, err
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
func (s *Reconciler) resetClusterConfigReconcileErrorIfAny() {
	types.MergeNoReconcileErrorOnCStorClusterConfig(s.CStorClusterConfig)
}

// buildClusterPlan builds a CStorClusterPlan resource
func (s *Reconciler) buildClusterPlan() error {

	var observedNodes []types.CStorClusterPlanNode
	if s.CStorClusterPlan != nil {
		observedNodes = s.CStorClusterPlan.Spec.Nodes
	} else {
		s.CStorClusterPlan = &types.CStorClusterPlan{}
		s.CStorClusterPlan.SetName(s.CStorClusterConfig.GetName())
		s.CStorClusterPlan.SetNamespace(s.CStorClusterConfig.GetNamespace())
		s.CStorClusterPlan.SetAnnotations(
			k8s.MergeToAnnotations(
				types.AnnKeyCStorClusterConfigUID,
				string(s.CStorClusterConfig.GetUID()),
				s.CStorClusterPlan.GetAnnotations(),
			),
		)
	}

	desired, err := s.NodeEvaluator.EvaluateDesiredNodes(node.EvaluationConfig{
		ObservedNodes: observedNodes,
		MinPoolCount:  s.CStorClusterConfig.Spec.MinPoolCount,
		MaxPoolCount:  s.CStorClusterConfig.Spec.MaxPoolCount,
	})
	if err != nil {
		return err
	}
	s.CStorClusterPlan.Spec.Nodes = desired
	return nil
}

func (s *Reconciler) validateClusterConfigAndSetDefaultsIfNotSet() error {
	// these default funcs follow a pre-defined order
	// ensure this ordering is not changed
	setDefaultFns := []func() error{
		s.validateDiskExternalProvisioner,
		s.setMinPoolCountIfNotSet,
		s.setMaxPoolCountIfNotSet,
		s.setRAIDTypeIfNotSet,
		s.setMinDiskCountIfNotSet,
		s.setMinDiskCapacityIfNotSet,
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
func (s *Reconciler) setMinPoolCountIfNotSet() error {
	if s.CStorClusterConfig.Spec.MinPoolCount.CmpInt64(0) == -1 {
		return errors.Errorf(
			"Invalid min pool count %d: Want positive value",
			s.CStorClusterConfig.Spec.MinPoolCount.Value(),
		)
	}
	if s.CStorClusterConfig.Spec.MinPoolCount.CmpInt64(0) != 0 {
		// don't set default value if it is already configured
		return nil
	}
	availableNodeCount := s.NodeEvaluator.GetNodeCount()
	eligibleNodeCount, err := s.NodeEvaluator.GetEligibleNodeCount()
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
		minPoolCount = eligibleNodeCount
	}
	if minPoolCount <= 0 {
		return errors.Errorf("Min pool count can't be 0: Preferred nodes not found")
	}
	// set min
	s.CStorClusterConfig.Spec.MinPoolCount.Set(minPoolCount)
	return nil
}

func (s *Reconciler) setMaxPoolCountIfNotSet() error {
	if s.CStorClusterConfig.Spec.MaxPoolCount.CmpInt64(0) == 0 {
		// max pool count is not set, so set it as min + 2
		s.CStorClusterConfig.Spec.MaxPoolCount.Set(
			s.CStorClusterConfig.Spec.MinPoolCount.Value() + 2,
		)
		return nil
	}
	if s.CStorClusterConfig.Spec.MinPoolCount.Cmp(s.CStorClusterConfig.Spec.MaxPoolCount) == 1 {
		return errors.Errorf("MaxPoolCount can't be less than MinPoolCount")
	}
	return nil
}

func (s *Reconciler) setRAIDTypeIfNotSet() error {
	if s.CStorClusterConfig.Spec.PoolConfig.RAIDType == "" {
		s.CStorClusterConfig.Spec.PoolConfig.RAIDType = types.PoolRAIDTypeDefault
		return nil
	}
	switch s.CStorClusterConfig.Spec.PoolConfig.RAIDType {
	case types.PoolRAIDTypeStripe, types.PoolRAIDTypeMirror,
		types.PoolRAIDTypeRAIDZ, types.PoolRAIDTypeRAIDZ2:
		// do nothing
	default:
		return errors.Errorf(
			"Invalid RAID type %s", s.CStorClusterConfig.Spec.PoolConfig.RAIDType,
		)
	}
	return nil
}

func (s *Reconciler) setMinDiskCountIfNotSet() error {
	if s.CStorClusterConfig.Spec.DiskConfig.MinCount.CmpInt64(0) == -1 {
		return errors.Errorf("Invalid min disk count: Want positive value")
	}
	if s.CStorClusterConfig.Spec.DiskConfig.MinCount.CmpInt64(0) == 1 {
		// no need to set default since it is already configured
		return nil
	}
	s.CStorClusterConfig.Spec.DiskConfig.MinCount.Set(
		RAIDTypeToDefaultMinDiskCount[s.CStorClusterConfig.Spec.PoolConfig.RAIDType],
	)
	return nil
}

func (s *Reconciler) setMinDiskCapacityIfNotSet() error {
	if s.CStorClusterConfig.Spec.DiskConfig.MinCapacity.CmpInt64(0) == -1 {
		return errors.Errorf("Invalid min disk capacity: Want positive value")
	}
	if s.CStorClusterConfig.Spec.DiskConfig.MinCapacity.CmpInt64(0) != 0 {
		// no need to set default since it is already configured
		return nil
	}
	s.CStorClusterConfig.Spec.DiskConfig.MinCapacity = DefaultMinDiskCapacity
	return nil
}

func (s *Reconciler) validateDiskExternalProvisioner() error {
	if s.CStorClusterConfig.Spec.DiskConfig.ExternalProvisioner.CSIAttacherName == "" ||
		s.CStorClusterConfig.Spec.DiskConfig.ExternalProvisioner.StorageClassName == "" {
		return errors.Errorf(
			"Invalid disk external provisioner: Both csi attacher & storageclass are required",
		)
	}
	return nil
}
