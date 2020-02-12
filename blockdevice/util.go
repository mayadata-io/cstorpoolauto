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

package blockdevice

import (
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/unstruct"
)

const (
	// Kind represents Kind of Kubernetes custom resource block device
	Kind = "BlockDevice"
)

// GetCapacity returns capacity of a block device in resource.Quantity format
// If value if not found or for an invalid capacity then it returns an error
func GetCapacity(obj unstructured.Unstructured) (resource.Quantity, error) {
	if obj.GetKind() != Kind {
		return resource.Quantity{},
			errors.Errorf("Kind mismatch. Expected kind %s got %s", Kind, obj.GetKind())
	}
	return unstruct.GetQuantityOrError(&obj, "spec", "capacity", "storage")
}

// GetHostName returns kubernetes.io/hostname label value of a block device. If value
// is not found or value is empty it returns an error. Host name is the value of
// kubernetes.io/hostname label of a node. NOTE - It may not be same with node name.
func GetHostName(obj unstructured.Unstructured) (string, error) {
	if obj.GetKind() != Kind {
		return "", errors.Errorf("Kind mismatch. Expected kind %s got %s", Kind, obj.GetKind())
	}
	return unstruct.GetStringOrError(&obj, "metadata", "labels", "kubernetes.io/hostname")
}

// GetNodeName returns the node name in which given block device is attached. This is
// not kubernetes.io/hostname label value of a node. NOTE - name of node and lable value
// may be different.
func GetNodeName(obj unstructured.Unstructured) (string, error) {
	if obj.GetKind() != Kind {
		return "",
			errors.Errorf("Kind mismatch. Expected kind %s got %s", Kind, obj.GetKind())
	}
	return unstruct.GetStringOrError(&obj, "spec", "nodeAttributes", "NodeName")
}

// IsActive checks the status of one blockdevice if it is Active then it returns true.
// Possible states are - Active, Inactive, Unknown
// Active - attached with one node.
// Inactive - detached from node.
// Unknown -  not able to get the status.
func IsActive(obj unstructured.Unstructured) (bool, error) {
	if obj.GetKind() != Kind {
		return false,
			errors.Errorf("Kind mismatch. Expected kind %s got %s", Kind, obj.GetKind())
	}
	value, err := unstruct.GetStringOrError(&obj, "status", "state")
	if err != nil {
		return false, err
	}
	if value == "Active" {
		return true, nil
	}
	return false, nil
}

// IsUnclaimed checks the claim status of one blockdevice if it is Unclaimed then
// it returns true. Possible states are - Unclaimed, Claimed, Released.
// - Unclaimed means no one is using this block device.
func IsUnclaimed(obj unstructured.Unstructured) (bool, error) {
	if obj.GetKind() != Kind {
		return false,
			errors.Errorf("Kind mismatch. Expected kind %s got %s", Kind, obj.GetKind())
	}
	value, err := unstruct.GetStringOrError(&obj, "status", "claimState")
	if err != nil {
		return false, err
	}
	if value == "Unclaimed" {
		return true, nil
	}
	return false, nil
}

// IsFileSystemPresent checks if any file system is present in the block device or not.
// If file system is not present then it returns true.
func IsFileSystemPresent(obj unstructured.Unstructured) (bool, error) {
	if obj.GetKind() != Kind {
		return false,
			errors.Errorf("Kind mismatch. Expected kind %s got %s", Kind, obj.GetKind())
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
