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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CStorClusterStorageSet is a kubernetes custom resource
// that provisions storage w.r.t. a cstor cluster pool
type CStorClusterStorageSet struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   CStorClusterStorageSetSpec   `json:"spec"`
	Status CStorClusterStorageSetStatus `json:"status"`
}

// CStorClusterStorageSetSpec has the storage details required
// to form CStorPoolCluster's pool capacity
type CStorClusterStorageSetSpec struct {
	Node               CStorClusterPlanNode       `json:"node"`
	Disk               CStorClusterStorageSetDisk `json:"disk"`
	ExternalDiskConfig ExternalDiskConfig         `json:"externalDiskConfig"`
}

// CStorClusterStorageSetDisk represents storage disk properties
// that will be be attached to the node & hence be part of the
// cstor cluster pool
type CStorClusterStorageSetDisk struct {
	Capacity resource.Quantity `json:"capacity"`
	Count    resource.Quantity `json:"count"`
}

// CStorClusterStorageSetStatus represents the current state of
// CStorClusterStorageSet
type CStorClusterStorageSetStatus struct {
	Phase      CStorClusterStorageSetStatusPhase       `json:"phase"`
	Conditions []CStorClusterStorageSetStatusCondition `json:"conditions"`
}

// CStorClusterStorageSetStatusPhase reports the current phase of
// CStorClusterStorageSet
type CStorClusterStorageSetStatusPhase string

const (
	// CStorClusterStorageSetStatusPhaseError indicates error in
	// CStorClusterStorageSet
	CStorClusterStorageSetStatusPhaseError CStorClusterStorageSetStatusPhase = "Error"

	// CStorClusterStorageSetStatusPhaseOnline indicates
	// CStorClusterStorageSet in Online state i.e. no error or warning
	CStorClusterStorageSetStatusPhaseOnline CStorClusterStorageSetStatusPhase = "Online"
)

// CStorClusterStorageSetStatusCondition represents a condition
// that represents the current state of CStorClusterStorageSet
type CStorClusterStorageSetStatusCondition struct {
	Type             ConditionType  `json:"type"`
	Status           ConditionState `json:"status"`
	Reason           string         `json:"reason,omitempty"`
	LastObservedTime string         `json:"lastObservedTime"`
}
