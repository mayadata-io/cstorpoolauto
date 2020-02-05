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

// CStorPoolClusterRecommendation is a kubernetes custom
// resource that represents recommended configurations
// for one given input/requirement.
type CStorPoolClusterRecommendation struct {
	// Commented it out for now as we - don't introduce a variable
	// if you are planing to use it in future
	// Will bring it back when we will create CR(s)
	// metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	RequestSpec CStorPoolClusterRecommendationRequestSpec `json:"requestSpec"`

	Spec CStorPoolClusterRecommendationSpec `json:"spec"`
}

// CStorPoolClusterRecommendationSpec contains recommended
// pool instance config.
type CStorPoolClusterRecommendationSpec struct {
	PoolInstances []PoolInstanceConfig `json:"poolInstances"`
}

// PoolInstanceConfig contains node identity capacity
// and selected block devices for that.
type PoolInstanceConfig struct {
	Node Reference `json:"node"`
	// Capacity represents capacity for one pool instance
	Capacity resource.Quantity `json:"capacity"`
	// DataDevices contains list of data, read-cache and
	// write-cache block devices associated with the node.
	BlockDevices BlockDeviceTopology `json:"blockDevices"`
}

// BlockDeviceTopology contains block device lists which will be
// used for data read and write cache.
type BlockDeviceTopology struct {
	// DataDevices contains list of block devices associated
	// with the node which will be used for data device.
	DataDevices []Reference `json:"dataDevices"`
	// DataDevices contains list of block devices associated
	// with the node which will be used for write cache.
	WriteCacheDevices []Reference `json:"writeCacheDevices"`
	// Commented it out for now as we - don't introduce a variable
	// if you are planing to use it in future
	// DataDevices contains list of block devices associated
	// with the node which will be used for read cache.
	// ReadCacheDevices []Reference `json:"readCacheDevices"`
}
