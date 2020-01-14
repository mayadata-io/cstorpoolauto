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
	"cstorpoolauto/types"
	"sort"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"
	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
	"openebs.io/metac/controller/common/selector"
)

// ByCreationTime implements sort.Interface based on the
// CreationTime field.
type ByCreationTime []*unstructured.Unstructured

func (u ByCreationTime) Len() int { return len(u) }

// Less will return true if i-th element has creation time
// earlier than j-th item. In other words true will be returned
// if i-th item is older than j-th item.
func (u ByCreationTime) Less(i, j int) bool {
	if len(u) == 0 || len(u)-1 < i || len(u)-1 < j {
		return false
	}

	var iMetaTime metav1.Time
	var errMsg = "Failed to get creation timestamp: Index %d: Found %t: %+v"
	iCT, found, err :=
		unstructured.NestedString(u[i].Object, "metadata", "creationTimestamp")
	if err != nil || !found {
		panic(errors.Errorf(errMsg, i, found, err))
	}
	err = iMetaTime.UnmarshalQueryParameter(iCT)
	if err != nil {
		panic(errors.Errorf(errMsg, i, found, err))
	}

	var jMetaTime metav1.Time
	jCT, found, err := unstructured.NestedString(u[j].Object, "metadata", "creationTimestamp")
	if err != nil || !found {
		panic(errors.Errorf(errMsg, j, found, err))
	}
	err = jMetaTime.UnmarshalQueryParameter(jCT)
	if err != nil {
		panic(errors.Errorf(errMsg, j, found, err))
	}
	iTime := iMetaTime.Time
	jTime := jMetaTime.Time
	// return true if both are equal or iTime is older than jTime
	return iTime.Equal(jTime) || iTime.Before(jTime)
}

func (u ByCreationTime) Swap(i, j int) { u[i], u[j] = u[j], u[i] }

// NodeList is a helper struct that exposes operations
// against a list of unstructured instances that should
// be of kind Node
type NodeList []*unstructured.Unstructured

// TryPickUptoCount returns a list of node based on the
// provided count
func (l NodeList) TryPickUptoCount(count int64) []*unstructured.Unstructured {
	nodeCount := int64(len(l))
	if nodeCount == 0 {
		return nil
	}
	var picks []*unstructured.Unstructured
	var i int64
	for i = 0; i < count; i++ {
		picks = append(picks, l[i])
		if nodeCount == i+1 {
			break
		}
	}
	return picks
}

// FindByNameAndUID returns the node instance based on the
// given name & uid
func (l NodeList) FindByNameAndUID(name string, uid k8stypes.UID) *unstructured.Unstructured {
	for _, node := range l {
		if node.GetName() == name && node.GetUID() == uid {
			return node
		}
	}
	return nil
}

// RemoveRecentByCountFromPlannedNodes removes the newly created
// nodes based on the given count from the given list
func (l NodeList) RemoveRecentByCountFromPlannedNodes(
	count int64, given []types.CStorClusterPlanNode,
) ([]types.CStorClusterPlanNode, error) {
	var plannedNodes []*unstructured.Unstructured
	for _, node := range given {
		uNode := l.FindByNameAndUID(node.Name, node.UID)
		if uNode == nil {
			return nil, errors.Errorf(
				"Can't remove node %s %s: Node not found", node.Name, node.UID,
			)
		}
		plannedNodes = append(plannedNodes, uNode)
	}
	sort.Sort(ByCreationTime(plannedNodes))
	// remove the recently created ones
	// remove based on the given count
	var updatedList []*unstructured.Unstructured
	newList := NodeList(append(updatedList, plannedNodes[count:]...))
	return newList.AsCStorClusterPlanNodes(), nil
}

// PickByCountAndNotInPlannedNodes returns a list of nodes
// as per the given count & are not part of the provided
// nodes
func (l NodeList) PickByCountAndNotInPlannedNodes(
	count int64, exclude []types.CStorClusterPlanNode,
) ([]types.CStorClusterPlanNode, error) {
	var result []types.CStorClusterPlanNode
	var fillCount int64
	for _, excludeNode := range exclude {
		for _, availableNode := range l {
			if excludeNode.Name != availableNode.GetName() ||
				excludeNode.UID != availableNode.GetUID() {
				// no match implies this node is eligible to be included
				result = append(result,
					types.CStorClusterPlanNode{
						Name: availableNode.GetName(),
						UID:  availableNode.GetUID(),
					},
				)
				fillCount++
				if fillCount == count {
					// can return here since we have required
					// number of nodes
					return result, nil
				}
			}
		}
	}
	return nil, errors.Errorf("Can't find eligible nodes: Want %d Got %d", count, fillCount)
}

// PickByCountAndIncludeAllPlannedNodes returns a list of nodes
// based on the given count & should include all the
// provided list of nodes.
//
// NOTE:
// 	Given count must be >= provided list of nodes
//
// In other words, the return list is a superset of given list
// of nodes.
func (l NodeList) PickByCountAndIncludeAllPlannedNodes(
	count int64, include []types.CStorClusterPlanNode,
) ([]types.CStorClusterPlanNode, error) {
	var includeCount int64
	var finalNodes []types.CStorClusterPlanNode

	includeCount = int64(len(include))
	if count < includeCount {
		return nil, errors.Errorf(
			"Can't pick node(s): Desired count %d must be >= include count %d",
			count, includeCount,
		)
	}
	if count == includeCount {
		// no need to add any extra nodes
		// return all the given nodes
		return include, nil
	}
	// start by adding all the given nodes to the final list
	finalNodes = append(finalNodes, include...)
	// pick other nodes i.e. ensure no duplicates
	otherNodes, err := l.PickByCountAndNotInPlannedNodes(count-includeCount, include)
	if err != nil {
		return nil, err
	}
	return append(finalNodes, otherNodes...), nil
}

// HasNameAndUID returns true if this node list has the given
// name as well as uid
func (l NodeList) HasNameAndUID(name string, uid k8stypes.UID) bool {
	for _, node := range l {
		if node.GetName() == name && node.GetUID() == uid {
			return true
		}
	}
	return false
}

// AsCStorClusterPlanNodes tranforms itself as a list of
// types.CStorClusterPlanNode objects
func (l NodeList) AsCStorClusterPlanNodes() []types.CStorClusterPlanNode {
	var planNodes []types.CStorClusterPlanNode
	for _, node := range l {
		planNodes = append(planNodes, types.CStorClusterPlanNode{
			Name: node.GetName(),
			UID:  node.GetUID(),
		})
	}
	return planNodes
}

// NodePlanner determines the eligible nodes fit to
// form CStorPoolCluster based on node selector terms
// as well as current observed state.
type NodePlanner struct {
	NodeSelector metac.ResourceSelector
	Resources    []*unstructured.Unstructured

	// nodes that match the node selector terms
	allowedNodes []*unstructured.Unstructured

	// functions that make it easy to mock this structure
	planFn                func(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error)
	getAllNodeCountFn     func() int64
	getAllowedNodeCountFn func() (int64, error)
}

// NodePlannerConfig contains observed nodes & related
// information to help in determining the desired nodes
// eligible to form CStorPoolCluster
type NodePlannerConfig struct {
	ObservedNodes []types.CStorClusterPlanNode
	MinPoolCount  resource.Quantity
	MaxPoolCount  resource.Quantity
}

// GetAllNodes returns the nodes from the list of resources
func (s *NodePlanner) GetAllNodes() []*unstructured.Unstructured {
	var nodes []*unstructured.Unstructured
	for _, attachment := range s.Resources {
		if attachment.GetKind() == string(types.KindNode) {
			nodes = append(nodes, attachment)
		}
	}
	return nodes
}

// GetAllNodeCount returns the number of nodes from the list
// of resources
func (s *NodePlanner) GetAllNodeCount() int64 {
	if s.getAllNodeCountFn != nil {
		return s.getAllNodeCountFn()
	}
	return int64(len(s.GetAllNodes()))
}

// GetAllowedNodes filters the allowed nodes based on
// node selector terms
//
// NOTE:
//	This caches the resulting eligible nodes which is
// helpful for GetEligibleNodesOrCached invocations.
func (s *NodePlanner) GetAllowedNodes() ([]*unstructured.Unstructured, error) {
	var allowed []*unstructured.Unstructured
	allnodes := s.GetAllNodes()
	if len(s.NodeSelector.SelectorTerms) == 0 {
		// all nodes are allowed since there is no preference
		// i.e. no selector terms were specified
		s.allowedNodes = allnodes
		return s.allowedNodes, nil
	}
	for _, node := range allnodes {
		eval := selector.Evaluation{
			Target: node,
			Terms:  s.NodeSelector.SelectorTerms,
		}
		match, err := eval.RunMatch()
		if err != nil {
			return nil, err
		}
		if match {
			allowed = append(allowed, node)
		}
	}
	s.allowedNodes = allowed
	return s.allowedNodes, nil
}

// GetAllowedNodesOrCached filters the allowed nodes based on
// node selector terms
func (s *NodePlanner) GetAllowedNodesOrCached() ([]*unstructured.Unstructured, error) {
	if len(s.allowedNodes) != 0 {
		// used the cached info
		return s.allowedNodes, nil
	}
	return s.GetAllowedNodes()
}

// GetAllowedNodeCount gets the allowed nodes based on
// node selector terms
func (s *NodePlanner) GetAllowedNodeCount() (int64, error) {
	if s.getAllowedNodeCountFn != nil {
		return s.getAllowedNodeCountFn()
	}
	allowedNodes, err := s.GetAllowedNodesOrCached()
	if err != nil {
		return 0, err
	}
	return int64(len(allowedNodes)), nil
}

// Plan runs through node planner config to determine
// the latest desired nodes that should form the CStorPoolCluster
func (s *NodePlanner) Plan(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error) {
	if s.planFn != nil {
		return s.planFn(conf)
	}
	return s.plan(conf)
}

// plan runs through node planner config to determine
// the latest desired nodes that should form the CStorPoolCluster
func (s *NodePlanner) plan(conf NodePlannerConfig) ([]types.CStorClusterPlanNode, error) {
	allowedNodes, err := s.GetAllowedNodesOrCached()
	if err != nil {
		return nil, err
	}
	allowedNodeList := NodeList(allowedNodes)
	if len(conf.ObservedNodes) == 0 {
		// this is the first time desired nodes are getting evaluated
		desired := allowedNodeList.TryPickUptoCount(conf.MinPoolCount.Value())
		return NodeList(desired).AsCStorClusterPlanNodes(), nil
	}
	// logic for observed nodes i.e. these nodes were evaluated
	// to be fit to form cstor pool cluster during previous
	// reconciliations
	var includes []types.CStorClusterPlanNode
	var includeCount int64
	for _, observedNode := range conf.ObservedNodes {
		if allowedNodeList.HasNameAndUID(observedNode.Name, observedNode.UID) {
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
		// We have the desired nodes
		//
		// NOTE:
		// 	This desired node list may or may not be same as previous
		// calculations. However, above logic should make rebuild
		// less disruptive.
		return includes, nil
	}
	if includeCount > conf.MaxPoolCount.Value() {
		// We have more nodes than what is desired
		//
		// Hence, remove nodes from the list based on
		// max pool count
		return allowedNodeList.RemoveRecentByCountFromPlannedNodes(
			includeCount-conf.MaxPoolCount.Value(),
			includes,
		)
	}
	// At this point, we need more nodes that what we
	// have. We shall include all the previous nodes (due to
	// previous reconciliations) and add new nodes to satisfy
	// the current desired plan
	//
	// NOTE:
	//	This logic ensures the total desired nodes is exactly
	// equal to the min pool count.
	return allowedNodeList.PickByCountAndIncludeAllPlannedNodes(
		conf.MinPoolCount.Value(),
		includes,
	)
}
