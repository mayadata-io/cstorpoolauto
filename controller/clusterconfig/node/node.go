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

package node

import (
	"cstorpoolauto/k8s"
	"cstorpoolauto/types"
	"sort"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"openebs.io/metac/controller/common/selector"
)

// ByCreationTime implements sort.Interface based on the
// CreationTime field.
type ByCreationTime []*unstructured.Unstructured

func (a ByCreationTime) Len() int { return len(a) }
func (a ByCreationTime) Less(i, j int) bool {
	return a[i].GetCreationTimestamp().Time.Sub(a[j].GetCreationTimestamp().Time) < 0
}
func (a ByCreationTime) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// List is a helper struct that exposes operations
// against a list of unstructured instances that should
// be of kind Node
type List []*unstructured.Unstructured

// PickByCount returns a list of node based on the
// provided count
func (l List) PickByCount(count int64) []*unstructured.Unstructured {
	var picks []*unstructured.Unstructured
	var i int64
	for i = 0; i < count; i++ {
		picks = append(picks, l[i])
	}
	return picks
}

// FindNodeFromNameAndUID returns the unstructured node instance
// given the name & uid
func (l List) FindNodeFromNameAndUID(name string, uid k8stypes.UID) *unstructured.Unstructured {
	for _, available := range l {
		if available.GetName() == name && available.GetUID() == uid {
			return available
		}
	}
	return nil
}

// RemoveRecentByCountFromPlannedNodes removes the newly created
// nodes based on the given count from the given list
func (l List) RemoveRecentByCountFromPlannedNodes(
	count int64, given []types.CStorClusterPlanNode,
) ([]types.CStorClusterPlanNode, error) {
	var kNodes []*unstructured.Unstructured
	for _, givenNode := range given {
		kNode := l.FindNodeFromNameAndUID(givenNode.Name, givenNode.UID)
		if kNode == nil {
			return nil, errors.Errorf(
				"Can't remove: Unable to get details of node %s %s",
				givenNode.Name,
				givenNode.UID,
			)
		}
		kNodes = append(kNodes, kNode)
	}
	sort.Sort(ByCreationTime(kNodes))
	// remove the recently created ones
	// remove based on the given count
	var updatedList []*unstructured.Unstructured
	newList := List(append(updatedList, kNodes[count:]...))
	return newList.AsCStorClusterPlanNodes(), nil
}

// PickByCountThatExcludePlannedNodes returns a list of nodes
// as per the given count & are not part of the provided
// nodes
func (l List) PickByCountThatExcludePlannedNodes(
	count int64, given []types.CStorClusterPlanNode,
) ([]types.CStorClusterPlanNode, error) {
	var otherNodes []types.CStorClusterPlanNode
	var fillCount int64
	for _, givenNode := range given {
		for _, availableNode := range l {
			if givenNode.Name != availableNode.GetName() ||
				givenNode.UID != availableNode.GetUID() {
				// no match implies this is not part of given nodes
				otherNodes = append(otherNodes,
					types.CStorClusterPlanNode{
						Name: availableNode.GetName(),
						UID:  availableNode.GetUID(),
					},
				)
				fillCount++
				if fillCount == count {
					// can return here since logic is satified
					return otherNodes, nil
				}
			}
		}
	}
	return nil, errors.Errorf("Can't find %d number of nodes", count)
}

// PickByCountThatIncludePlannedNodes returns a list of nodes
// based on the given count & should include all the provided
// nodes. In other words, the return list is a superset of
// given nodes i.e. includes.
func (l List) PickByCountThatIncludePlannedNodes(
	count int64, includes []types.CStorClusterPlanNode,
) ([]types.CStorClusterPlanNode, error) {

	var includeCount int64
	var finalNodes []types.CStorClusterPlanNode

	includeCount = int64(len(includes))
	finalNodes = append(finalNodes, includes...)
	if count < includeCount {
		return nil, errors.Errorf(
			"Can't pick planned nodes: Expected count less than expected planned nodes",
		)
	}
	if count == includeCount {
		return finalNodes, nil
	}
	otherNodes, err := l.PickByCountThatExcludePlannedNodes(count-includeCount, includes)
	if err != nil {
		return nil, err
	}
	return append(finalNodes, otherNodes...), nil
}

// HasNameAndUID returns true if this node list has the given
// name as well as uid
func (l List) HasNameAndUID(name string, uid k8stypes.UID) bool {
	for _, node := range l {
		if node.GetName() == name && node.GetUID() == uid {
			return true
		}
	}
	return false
}

// AsCStorClusterPlanNodes tranforms itself as a list
// types.CStorClusterPlanNode instances
func (l List) AsCStorClusterPlanNodes() []types.CStorClusterPlanNode {
	var statusNodes []types.CStorClusterPlanNode
	for _, node := range l {
		statusNodes = append(statusNodes, types.CStorClusterPlanNode{
			Name: node.GetName(),
			UID:  node.GetUID(),
		})
	}
	return statusNodes
}

// CStorClusterConfigNodeEvaluator exposes nodes
// information based on CStorClusterConfig instance
type CStorClusterConfigNodeEvaluator struct {
	CStorClusterConfig *types.CStorClusterConfig
	Attachments        []*unstructured.Unstructured

	// nodes that match the node selector terms
	eligibleNodes []*unstructured.Unstructured
}

// EvaluationConfig stores node & related information
// to help in evaluating desired nodes to form the cstor
// pool cluster
type EvaluationConfig struct {
	ObservedNodes []types.CStorClusterPlanNode
	MinPoolCount  resource.Quantity
	MaxPoolCount  resource.Quantity
}

// GetNodes returns the nodes from the list of attachments
func (s *CStorClusterConfigNodeEvaluator) GetNodes() []*unstructured.Unstructured {
	var nodes []*unstructured.Unstructured
	for _, attachment := range s.Attachments {
		if attachment.GetKind() == string(k8s.KindNode) {
			nodes = append(nodes, attachment)
		}
	}
	return nodes
}

// GetNodeCount returns the number of nodes from the list
// of attachments
func (s *CStorClusterConfigNodeEvaluator) GetNodeCount() int64 {
	var count int64
	for _, attachment := range s.Attachments {
		if attachment.GetKind() == string(k8s.KindNode) {
			count++
		}
	}
	return count
}

// GetEligibleNodes gets the eligible nodes based on
// node selector terms specified in CStorClusterConfig
//
// NOTE:
//	This caches the resulting eligible nodes which is
// helpful for GetEligibleNodesOrCached invocations.
func (s *CStorClusterConfigNodeEvaluator) GetEligibleNodes() ([]*unstructured.Unstructured, error) {
	nodes := s.GetNodes()

	if len(s.CStorClusterConfig.Spec.AllowedNodes.SelectorTerms) == 0 {
		// all nodes are eligible since there is no preference
		// i.e. no selector terms were specified
		s.eligibleNodes = nodes
		return s.eligibleNodes, nil
	}

	for _, nodeAttachment := range nodes {
		eval := selector.Evaluation{
			Target: nodeAttachment,
			Terms:  s.CStorClusterConfig.Spec.AllowedNodes.SelectorTerms,
		}
		match, err := eval.RunMatch()
		if err != nil {
			return nil, err
		}
		if match {
			s.eligibleNodes = append(s.eligibleNodes, nodeAttachment)
		}
	}
	return s.eligibleNodes, nil
}

// GetEligibleNodesOrCached gets the eligible nodes based on
// node selector terms specified in CStorClusterConfig
func (s *CStorClusterConfigNodeEvaluator) GetEligibleNodesOrCached() ([]*unstructured.Unstructured, error) {
	if len(s.eligibleNodes) != 0 {
		// used the cached info
		return s.eligibleNodes, nil
	}
	return s.GetEligibleNodes()
}

// GetEligibleNodeCount gets the eligible nodes based on
// node selector terms specified in CStorClusterConfig
func (s *CStorClusterConfigNodeEvaluator) GetEligibleNodeCount() (int64, error) {
	nodes, err := s.GetEligibleNodesOrCached()
	if err != nil {
		return 0, err
	}
	var count int64
	for range nodes {
		count++
	}
	return count, nil
}

// EvaluateDesiredNodes runs through the evaluation config to
// determine the latest desired nodes that should form the cstor
// pool cluster
func (s *CStorClusterConfigNodeEvaluator) EvaluateDesiredNodes(
	conf EvaluationConfig,
) ([]types.CStorClusterPlanNode, error) {
	eligibleNodes, err := s.GetEligibleNodesOrCached()
	if err != nil {
		return nil, err
	}
	eligibleNodeList := List(eligibleNodes)
	if len(conf.ObservedNodes) == 0 {
		// this is the first time desired nodes are getting evaluated
		desired := eligibleNodeList.PickByCount(conf.MinPoolCount.Value())
		return List(desired).AsCStorClusterPlanNodes(), nil
	}
	// logic for observed nodes i.e. these nodes were evaluated
	// to be fit to form cstor pool cluster during previous
	// reconciliations
	var includes []types.CStorClusterPlanNode
	var includeCount int64
	for _, observedNode := range conf.ObservedNodes {
		if eligibleNodeList.HasNameAndUID(observedNode.Name, observedNode.UID) {
			// observed node is still eligible
			// include this once again to make the cluster re-building
			// less disruptive; its best to avoid cluster rebuild if its
			// not required
			includes = append(includes, observedNode)
			includeCount++
		}
	}
	if includeCount >= conf.MinPoolCount.Value() &&
		includeCount <= conf.MaxPoolCount.Value() {
		// This node list may or may not be same as previous
		// calculations. However, above logic should make rebuild
		// less disruptive.
		return includes, nil
	}
	if includeCount > conf.MaxPoolCount.Value() {
		// need to remove some nodes from the list
		// since this max nodes is capped by max pool count
		return eligibleNodeList.RemoveRecentByCountFromPlannedNodes(
			includeCount-conf.MaxPoolCount.Value(),
			includes,
		)
	}
	return eligibleNodeList.PickByCountThatIncludePlannedNodes(
		conf.MinPoolCount.Value(),
		includes,
	)
}
