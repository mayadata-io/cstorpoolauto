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

package blockdevice

import (
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
)

// GetCapacityOrError returns capacity of a block device in resource.Quantity format
// If value if not found or for an invalid capacity then it returns an error
func GetCapacityOrError(obj unstructured.Unstructured) (resource.Quantity, error) {
	if obj.GetKind() != string(types.KindBlockDevice) {
		return resource.Quantity{},
			errors.Errorf("Can not get capacity: Expected kind %q got %q",
				types.KindBlockDevice, obj.GetKind())
	}
	return unstruct.GetQuantityOrError(&obj, "spec", "capacity", "storage")
}

// GetHostNameOrError returns kubernetes.io/hostname label value of a block device. If value
// is not found or value is empty it returns an error. Host name is the value of
// kubernetes.io/hostname label of a node. NOTE - It may not be same with node name.
func GetHostNameOrError(obj unstructured.Unstructured) (string, error) {
	if obj.GetKind() != string(types.KindBlockDevice) {
		return "", errors.Errorf("Can not get host name: Expected kind %q got %q",
			types.KindBlockDevice, obj.GetKind())
	}
	return unstruct.GetStringOrError(&obj, "metadata", "labels", "kubernetes.io/hostname")
}

// GetNodeNameOrError returns the node name in which given block device is attached. This is
// not kubernetes.io/hostname label value of a node. NOTE - name of node and label value
// may be different.
func GetNodeNameOrError(obj unstructured.Unstructured) (string, error) {
	if obj.GetKind() != string(types.KindBlockDevice) {
		return "",
			errors.Errorf("Can not get node name: Expected kind %q got %q",
				types.KindBlockDevice, obj.GetKind())
	}
	return unstruct.GetStringOrError(&obj, "spec", "nodeAttributes", "nodeName")
}

// IsActiveOrError checks the status of one blockdevice if it is Active then it returns true.
// Possible states are - Active, Inactive, Unknown
// Active - attached with one node.
// Inactive - detached from node.
// Unknown -  not able to get the status.
func IsActiveOrError(obj unstructured.Unstructured) (bool, error) {
	if obj.GetKind() != string(types.KindBlockDevice) {
		return false,
			errors.Errorf("Can not get status: Expected kind %q got %q",
				types.KindBlockDevice, obj.GetKind())
	}
	value, err := unstruct.GetStringOrError(&obj, "status", "state")
	if err != nil {
		return false, err
	}
	if value == string(types.BlockDeviceActive) {
		return true, nil
	}
	return false, nil
}

// IsUnclaimedOrError checks the claim status of one blockdevice if it is Unclaimed then
// it returns true. Possible states are - Unclaimed, Claimed, Released.
// - Unclaimed means no one is using this block device.
func IsUnclaimedOrError(obj unstructured.Unstructured) (bool, error) {
	if obj.GetKind() != string(types.KindBlockDevice) {
		return false,
			errors.Errorf("Can not get claim status: Expected kind %q got %q",
				types.KindBlockDevice, obj.GetKind())
	}
	value, err := unstruct.GetStringOrError(&obj, "status", "claimState")
	if err != nil {
		return false, err
	}
	if value == string(types.BlockDeviceUnclaimed) {
		return true, nil
	}
	return false, nil
}

// HasSystemPresentOrError checks if any file system is present in the block device or not.
// If file system is not present then it returns true.
func HasSystemPresentOrError(obj unstructured.Unstructured) (bool, error) {
	if obj.GetKind() != string(types.KindBlockDevice) {
		return false,
			errors.Errorf("Can not check file system: Expected kind %q got %q",
				types.KindBlockDevice, obj.GetKind())
	}
	_, found, err := unstructured.NestedString(obj.UnstructuredContent(), "spec", "filesystem", "fsType")
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	return true, nil
}
