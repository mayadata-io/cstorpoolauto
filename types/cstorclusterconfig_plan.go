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

package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// CStorClusterConfigPlan is a kubernetes custom resource that plans
// the resources especially nodes to form the CStorPoolCluster
type CStorClusterConfigPlan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec CStorClusterConfigPlanSpec `json:"spec"`
}

// CStorClusterConfigPlanSpec has the plan details required to form
// CStorPoolCluster
type CStorClusterConfigPlanSpec struct {
	ClusterConfigReference CStorClusterConfigReference  `json:"clusterConfigReference"`
	Nodes                  []CStorClusterConfigPlanNode `json:"nodes"`
}

// CStorClusterConfigPlanNode has the node details that is used to
// form CStorPoolCluster
type CStorClusterConfigPlanNode struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

// CStorClusterConfigReference refers to CStorClusterConfig
// which is the trigger to CStorClusterPlan
type CStorClusterConfigReference struct {
	UID types.UID `json:"uid"`
}
