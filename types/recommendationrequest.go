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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// CStorPoolClusterRecommendationRequest is a kubernetes custom
// resource that represents configuration input or requirement
// to create or scale one CStor pool.
type CStorPoolClusterRecommendationRequest struct {
	// Commented it out for now as we - don't introduce a variable
	// if you are planing to use it in future
	// Will bring it back when we will create CR(s)
	// metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`

	Spec CStorPoolClusterRecommendationRequestSpec `json:"spec"`
}

// CStorPoolClusterRecommendationRequestSpec contains input or requirement
// to create or scale one CStorpoolcluster.
type CStorPoolClusterRecommendationRequestSpec struct {
	// PoolCapacity represents requested capacity for one pool
	PoolCapacity resource.Quantity `json:"poolCapacity"`
	// BlockDeviceList represents list of block devices in all nodes.
	BlockDeviceList unstructured.UnstructuredList `json:"blockDeviceList"`
	// DataConfig represents raid configuration for data devices.
	DataConfig RaidGroupConfig `json:"dataConfig"`
	// WriteCacheConfig represents raid configuration for write cache devices.
	// If this field is nil then write cache is disabled.
	WriteCacheConfig *RaidGroupConfig `json:"writeCacheConfig"`
	// ReadCacheConfig represents raid configuration for read cache devices.
	// If this field is nil then read cache is disabled.
	// Commented it out for now as we - don't introduce a variable
	// if you are planing to use it in future
	// ReadCacheConfig *RaidGroupConfig `json:"readCacheConfig"`
}
