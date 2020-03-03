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

	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
)

// ResourceCategory is used to identify a particular resource
type ResourceCategory string

const (
	// ResourceTypeCount points to a resource of category count
	ResourceTypeCount ResourceCategory = "count"

	// ResourceTypeUsedCapacity points to a resource of category used capacity
	ResourceTypeUsedCapacity ResourceCategory = "used-capacity"
)

// ResourceMap is a mapping of resource category to quantity
type ResourceMap map[ResourceCategory]resource.Quantity

// CStorClusterConfig is a kubernetes custom resource that defines
// the specifications to manage CStorPoolCluster (i.e. CSPC)
//
// NOTE:
// 	This is a user facing custom resource
type CStorClusterConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec   CStorClusterConfigSpec   `json:"spec"`
	Status CStorClusterConfigStatus `json:"status"`
}

// CStorClusterConfigSpec defines the configuration required
// to setup and manage cstor pool cluster
type CStorClusterConfigSpec struct {
	MinPoolCount resource.Quantity      `json:"minPoolCount"`
	MaxPoolCount resource.Quantity      `json:"maxPoolCount"`
	AllowedNodes metac.ResourceSelector `json:"allowedNodes"`
	DiskConfig   DiskConfig             `json:"diskConfig"`
	PoolConfig   PoolConfig             `json:"poolConfig"`
}

// DiskConfig has disk information related to
// one cstor pool instance
type DiskConfig struct {
	MinCount           resource.Quantity   `json:"minCount"`
	MinCapacity        resource.Quantity   `json:"minCapacity"`
	ExternalDiskConfig *ExternalDiskConfig `json:"external,omitempty"`
	LocalDiskConfig    *LocalDiskConfig    `json:"local,omitempty"`
}

// ExternalDiskConfig has the details required to provision
// a disk. This makes use of CSI based volume provisioning
// to realise a disk & subsequent disk attachment.
type ExternalDiskConfig struct {
	CSIAttacherName  string `json:"csiAttacherName"`
	StorageClassName string `json:"storageClassName"`
}

// LocalDiskConfig refers to local disks details that should be
// available & is eligible to participate in building cstor
// pool instace.
type LocalDiskConfig struct {
	BlockDeviceSelector metac.ResourceSelector `json:"blockDeviceSelector"`
}

// PoolConfig defines various options to configure a
// cstor pool cluster
type PoolConfig struct {
	PoolExpansion    PoolExpansion    `json:"poolExpansion"`
	ComputeResources ComputeResources `json:"computeResources"`
	RAIDType         PoolRAIDType     `json:"raidType"`
}

// PoolExpansion provides options to trigger expansion
// of any cstor pool instance
type PoolExpansion struct {
	Disable   *bool       `json:"disable"`
	Threshold ResourceMap `json:"capacityThreshold"`
}

// ComputeResources defines the resources required to run one
// cstor pool instance
type ComputeResources struct {
	Requests ResourceMap `json:"requests"`
	Limits   ResourceMap `json:"limits"`
}

// PoolRAIDType represents the supported pool type for all cstor
// pool instances
type PoolRAIDType string

const (
	// PoolRAIDTypeStripe represents a stripe pool type
	PoolRAIDTypeStripe PoolRAIDType = "stripe"

	// PoolRAIDTypeMirror represents a mirror pool type
	PoolRAIDTypeMirror PoolRAIDType = "mirror"

	// PoolRAIDTypeRAIDZ represents a raidz pool type
	PoolRAIDTypeRAIDZ PoolRAIDType = "raidz"

	// PoolRAIDTypeRAIDZ2 represents a raidz2 pool type
	PoolRAIDTypeRAIDZ2 PoolRAIDType = "raidz2"

	// PoolRAIDTypeDefault represents the default pool type
	PoolRAIDTypeDefault PoolRAIDType = PoolRAIDTypeMirror
)

// RAIDTypeToDefaultMinDiskCount maps pool instance's raid type
// to its default minimum disk count
var RAIDTypeToDefaultMinDiskCount = map[PoolRAIDType]int64{
	PoolRAIDTypeStripe: 1,
	PoolRAIDTypeMirror: 2,
	PoolRAIDTypeRAIDZ:  3,
	PoolRAIDTypeRAIDZ2: 6,
}

// CStorClusterConfigStatus represents the current state of
// CStorClusterConfig
type CStorClusterConfigStatus struct {
	Phase      CStorClusterConfigStatusPhase       `json:"phase"`
	Conditions []CStorClusterConfigStatusCondition `json:"conditions"`
}

// CStorClusterConfigStatusPhase reports the current phase of
// CStorClusterConfig
type CStorClusterConfigStatusPhase string

const (
	// CStorClusterConfigStatusPhaseError indicates error in
	// CStorClusterConfig
	CStorClusterConfigStatusPhaseError CStorClusterConfigStatusPhase = "Error"

	// CStorClusterConfigStatusPhaseOnline indicates
	// CStorClusterConfig in Online state i.e. no error or warning
	CStorClusterConfigStatusPhaseOnline CStorClusterConfigStatusPhase = "Online"
)

// CStorClusterConfigStatusCondition represents a condition
// that represents the current state of CStorClusterConfig
type CStorClusterConfigStatusCondition struct {
	Type             ConditionType  `json:"type"`
	Status           ConditionState `json:"status"`
	Reason           string         `json:"reason,omitempty"`
	LastObservedTime string         `json:"lastObservedTime"`
}
