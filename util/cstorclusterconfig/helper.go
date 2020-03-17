/*
Copyright 2020 The MayaData Authors.

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
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"

	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
)

var nilselector = metac.ResourceSelector{}

// Helper provides all utility methods against a CStorClusterConfig
// unstructured instance
type Helper struct {
	ClusterConfig *unstructured.Unstructured

	raidType types.PoolRAIDType
	err      error
}

// NewHelper returns a new instance of CStorClusterConfig based helper
func NewHelper(obj *unstructured.Unstructured) *Helper {
	var h = &Helper{}
	if obj == nil || obj.Object == nil {
		h.err = errors.Errorf("Can't init helper: Nil object")
		return h
	}
	if obj.GetKind() != string(types.KindCStorClusterConfig) {
		h.err = errors.Errorf(
			"Can't init helper: Invalid kind: Want %q got %q",
			string(types.KindCStorClusterConfig), obj.GetKind(),
		)
		return h
	}
	h.ClusterConfig = obj
	return h
}

// IsLocalBlockDiskConfig returns true if provided CStorClusterConfig
// is set with local block disk configuration
func (h *Helper) IsLocalBlockDiskConfig() (bool, error) {
	if h.err != nil {
		return false, h.err
	}
	localDiskSelectTerms, found, err := unstructured.NestedSlice(
		h.ClusterConfig.Object,
		"spec",
		"diskConfig",
		"local",
		"blockDeviceSelector",
		"selectorTerms",
	)
	if err != nil {
		return false, err
	}
	if !found || len(localDiskSelectTerms) == 0 {
		return false, nil
	}
	return true, nil
}

// GetLocalBlockDeviceSelector returns block disk selector that has been
// configured to match against any block device(s)
func (h *Helper) GetLocalBlockDeviceSelector() (metac.ResourceSelector, error) {
	if h.err != nil {
		return nilselector, h.err
	}
	var cstorClusterConfigTyped = types.CStorClusterConfig{}
	err := unstruct.UnstructToTyped(
		h.ClusterConfig,
		&cstorClusterConfigTyped,
	)
	if err != nil {
		return nilselector, err
	}
	localDiskConf := cstorClusterConfigTyped.Spec.DiskConfig.LocalDiskConfig
	if localDiskConf == nil {
		return nilselector,
			errors.Errorf(
				"Can't get disk selector: Nil LocalDiskConfig",
			)
	}
	return localDiskConf.BlockDeviceSelector, nil
}

// IsDiskCountMatchRAIDType returns true if given count
// is supported by the RAIDType that is set against this
// CStorClusterConfig instance
func (h *Helper) IsDiskCountMatchRAIDType(count int64) (bool, error) {
	if h.err != nil {
		return false, h.err
	}
	if count <= 0 {
		return false,
			errors.Errorf(
				"Can't match: Invalid disk count %d",
				count,
			)
	}
	raid, err := h.GetRAIDTypeOrCached()
	if err != nil {
		return false, err
	}
	h.raidType = raid
	// check if remainder is zero
	if count%types.RAIDTypeToDefaultMinDiskCount[h.raidType] == 0 {
		return true, nil
	}
	return false, nil
}

func (h *Helper) validateRAIDType(raidType types.PoolRAIDType) error {
	if !types.SupportedRAIDTypes[raidType] {
		return errors.Errorf(
			"Invalid RAID type %q",
			raidType,
		)
	}
	return nil
}

// GetRAIDType returns the raid type of this CStorClusterConfig
// instance
func (h *Helper) GetRAIDType() (types.PoolRAIDType, error) {
	if h.err != nil {
		return "", h.err
	}
	raid, err := unstruct.GetStringOrError(
		h.ClusterConfig,
		"spec",
		"poolConfig",
		"raidType",
	)
	if err != nil {
		return "", err
	}
	// validate if configured raid type is valid
	err = h.validateRAIDType(types.PoolRAIDType(raid))
	if err != nil {
		return "", err
	}
	h.raidType = types.PoolRAIDType(raid)
	return h.raidType, nil
}

// GetRAIDTypeOrCached returns the raid type of this CStorClusterConfig
// instance
func (h *Helper) GetRAIDTypeOrCached() (types.PoolRAIDType, error) {
	if h.err != nil {
		return "", h.err
	}
	// check if cached value is available
	if h.raidType != "" {
		// return cached value
		return h.raidType, nil
	}
	return h.GetRAIDType()
}
