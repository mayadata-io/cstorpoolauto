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

package lib

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// CSPCOps is a kubernetes custom resource that defines the
// specifications to manage a cstor pool cluster (i.e. CSPC)
//
// NOTE:
// 	This is a user facing custom resource
type CSPCOps struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec CSPCOpsSpec `json:"spec"`
}

// CSPCOpsSpec defines the configuration required to setup and
// manage cstor pool cluster
type CSPCOpsSpec struct {
	PoolClusterConfig PoolClusterConfig `json:"poolClusterConfig"`
}

// PoolClusterConfig defines the configuration required to setup
// and manage cstor pool cluster
type PoolClusterConfig struct {
	MinPools           ResourceMap        `json:"minPools"`
	MaxPools           ResourceMap        `json:"maxPools"`
	PoolType           *PoolType          `json:"poolType"`
	AllowedNodes       NodeSelector       `json:"allowedNodes"`
	DiskConfig         DiskConfig         `json:"diskConfig"`
	PoolClusterOptions PoolClusterOptions `json:"poolClusterOptions"`
}

// PoolClusterOptions defines various options to configure a
// cstor pool cluster
type PoolClusterOptions struct {
	PoolExpansion    PoolExpansion    `json:"poolExpansion"`
	ComputeResources ComputeResources `json:"computeResources"`
}

// PoolExpansion provides options to trigger expansion of any
// cstor pool instance
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

// CSPCPlan is a kubernetes custom resource that declares the
// specifications to manage cstor pool cluster (i.e. CSPC)
// resource. This acts as an helper to synchronise CSPCOps &
// CSPC resources.
type CSPCPlan struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec CSPCPlanSpec `json:"spec"`
}

// CSPCPlanSpec defines the node related plans required to
// provision & manage cstor pool instances of a cspc cluster
type CSPCPlanSpec struct {
	PoolClusterPlan PoolClusterPlan `json:"poolClusterPlan"`
}

// PoolType represents the supported pool type for all cstor
// pool instances
type PoolType string

const (
	// PoolTypeMirror represents a mirror pool type
	PoolTypeMirror PoolType = "mirror"

	// PoolTypeStripe represents a stripe pool type
	PoolTypeStripe PoolType = "stripe"

	// PoolTypeRaidz represents a raidz pool type
	PoolTypeRaidz PoolType = "raidz"
)

// PoolClusterPlan has the required planning to provision and manage
// all cstor pool instances for the given cluster
type PoolClusterPlan struct {
	PoolType      *PoolType            `json:"poolType"`
	DiskConfig    DiskConfig           `json:"diskConfig"`
	PoolInstances []PoolInstanceConfig `json:"poolInstances"`
}

// PoolInstanceConfig has the node & device information related to
// one cstor pool instance
type PoolInstanceConfig struct {
	NodeDetail    NodeDetail     `json:"nodeDetail"`
	DeviceDetails []DeviceDetail `json:"deviceDetails"`
}

// DiskConfig has disk information related to
// one cstor pool instance
type DiskConfig struct {
	DiskCount           resource.Quantity   `json:"diskCount"`
	DiskCapacity        resource.Quantity   `json:"diskCapacity"`
	ExternalProvisioner ExternalProvisioner `json:"externalProvisioner"`
}

// NodeDetail has node specific information
type NodeDetail struct {
	NodeName string `json:"nodeName"`
	NodeUID  string `json:"nodeUID"`
}

// DeviceDetail has device specific information
type DeviceDetail struct {
	BlockDeviceName string `json:"blockDeviceName"`
}

// ExternalProvisioner has the details required to provision
// a disk
type ExternalProvisioner struct {
	CSIAttacherName  string `json:"csiAttacherName"`
	StorageClassName string `json:"storageClassName"`
}
