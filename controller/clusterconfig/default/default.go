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

package ccdefault

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

type cstorClusterConfigDefaultErrHandler struct {
	clusterConfig *unstructured.Unstructured
	hookResponse  *generic.SyncHookResponse
}

func (h *cstorClusterConfigDefaultErrHandler) handle(err error) {
	// Error has been handled elaborately. This logic ensures
	// error message is propagated to the resource & hence seen via
	// 'kubectl get CStorClusterConfig -oyaml'.
	//
	// In addition, logging has been done to check for error messages
	// from this pod's logs.
	glog.Errorf(
		"Failed to set defaults: CStorClusterConfig %s: %v", h.clusterConfig.GetName(), err,
	)

	conds, mergeErr :=
		k8s.MergeStatusConditions(h.clusterConfig, types.MakeErrorSettingDefaultsCondition(err))
	if mergeErr != nil {
		glog.Errorf(
			"Failed to set status conditions: CStorClusterConfig %s: %v",
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
		h.hookResponse.Status["phase"] = "Error"
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
// w.r.t the watched resource. In this case CStorClusterConfig again
// is the resource that forms the response.
func Sync(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	response = &generic.SyncHookResponse{}

	// nothing needs to be done if request is empty
	if request == nil || request.Attachments == nil || request.Attachments.IsEmpty() {
		response.SkipReconcile = true
		return nil
	}

	for _, attachment := range request.Attachments.List() {
		// watch & attachment needs to be same to proceed with setting
		// of defaults
		if request.Watch.GetUID() != attachment.GetUID() {
			// just add the attachment & return if its not same as watch
			response.Attachments = append(response.Attachments, attachment)
			continue
		}

		// construct the error handler
		errHandler := &cstorClusterConfigDefaultErrHandler{
			clusterConfig: request.Watch,
			hookResponse:  response,
		}

		// invoke the logic to set defaults against CStorClusterConfig
		dSetter, err :=
			NewCStorClusterConfigDefaultSetter(request.Watch, request.Attachments.List())
		if err != nil {
			errHandler.handle(err)
			return nil
		}

		cstorClusterConfigWithDefaults, err := dSetter.sync()
		if err != nil {
			errHandler.handle(err)
			return nil
		}

		response.Attachments = append(response.Attachments, cstorClusterConfigWithDefaults)
	}

	return nil
}

// CStorClusterConfigDefaultSetter enables setting defaults
// against the CStorClusterConfig instance
type CStorClusterConfigDefaultSetter struct {
	CStorClusterConfig *types.CStorClusterConfig
	Attachments        []*unstructured.Unstructured

	NodeEvaluator *node.CStorClusterConfigNodeEvaluator
}

// NewCStorClusterConfigDefaultSetter returns a new instance of
// CStorClusterConfigDefaultSetter
func NewCStorClusterConfigDefaultSetter(
	clusterConfig *unstructured.Unstructured,
	attachments []*unstructured.Unstructured,
) (*CStorClusterConfigDefaultSetter, error) {
	cstorClusterConfigRaw, err := clusterConfig.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var cstorClusterConfig types.CStorClusterConfig
	err = json.Unmarshal(cstorClusterConfigRaw, &cstorClusterConfig)
	if err != nil {
		return nil, err
	}
	return &CStorClusterConfigDefaultSetter{
		CStorClusterConfig: &cstorClusterConfig,
		Attachments:        attachments,
		NodeEvaluator: &node.CStorClusterConfigNodeEvaluator{
			CStorClusterConfig: &cstorClusterConfig,
			Attachments:        attachments,
		},
	}, nil
}

func (s *CStorClusterConfigDefaultSetter) sync() (*unstructured.Unstructured, error) {
	syncFns := []func() error{
		s.setDefaults,
		s.setDesiredPoolNodes,
	}
	for _, syncFn := range syncFns {
		err := syncFn()
		if err != nil {
			return nil, err
		}
	}
	s.resetDefaultingErrorIfAny()
	return s.getUpdated()
}

// getUpdated gets the CStorClusterConfig instance updated with
// defaults & status
func (s *CStorClusterConfigDefaultSetter) getUpdated() (*unstructured.Unstructured, error) {
	// get the updated CStorClusterConfig & return
	raw, err := json.Marshal(s.CStorClusterConfig)
	if err != nil {
		return nil, err
	}
	var cstorClusterConfigWithDefaults unstructured.Unstructured
	err = json.Unmarshal(raw, &cstorClusterConfigWithDefaults)
	if err != nil {
		return nil, err
	}
	return &cstorClusterConfigWithDefaults, nil
}

// resetDefaultingErrorIfAny removes any error associated while
// setting defaults if it ever happened during in previous attempts.
func (s *CStorClusterConfigDefaultSetter) resetDefaultingErrorIfAny() {
	types.SetNoErrorSettingDefaultsCondition(s.CStorClusterConfig)
}

// setDesiredPoolNodes sets CStorClusterConfig with desired nodes
func (s *CStorClusterConfigDefaultSetter) setDesiredPoolNodes() error {
	desired, err := s.NodeEvaluator.EvaluateDesiredNodes(node.EvaluationConfig{
		ObservedNodes: s.CStorClusterConfig.Status.Nodes,
		MinPoolCount:  s.CStorClusterConfig.Spec.MinPoolCount,
		MaxPoolCount:  s.CStorClusterConfig.Spec.MaxPoolCount,
	})
	if err != nil {
		return err
	}
	s.CStorClusterConfig.Status.Nodes = desired
	return nil
}

func (s *CStorClusterConfigDefaultSetter) setDefaults() error {
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
func (s *CStorClusterConfigDefaultSetter) setMinPoolCountIfNotSet() error {
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

func (s *CStorClusterConfigDefaultSetter) setMaxPoolCountIfNotSet() error {
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

func (s *CStorClusterConfigDefaultSetter) setRAIDTypeIfNotSet() error {
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

func (s *CStorClusterConfigDefaultSetter) setMinDiskCountIfNotSet() error {
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

func (s *CStorClusterConfigDefaultSetter) setMinDiskCapacityIfNotSet() error {
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

func (s *CStorClusterConfigDefaultSetter) validateDiskExternalProvisioner() error {
	if s.CStorClusterConfig.Spec.DiskConfig.ExternalProvisioner.CSIAttacherName == "" ||
		s.CStorClusterConfig.Spec.DiskConfig.ExternalProvisioner.StorageClassName == "" {
		return errors.Errorf(
			"Invalid disk external provisioner: Both csi attacher & storageclass are required",
		)
	}
	return nil
}
