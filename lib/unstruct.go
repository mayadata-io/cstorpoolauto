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
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

var (
	// KeyFunc checks for DeletedFinalStateUnknown objects
	// before calling MetaNamespaceKeyFunc.
	KeyFunc = cache.DeletionHandlingMetaNamespaceKeyFunc
)

// AnyUnstructRegistry is the register that holds various
// unstructured instances grouped by
//
// 1/ unstruct's apiVersion & kind, and then by
// 2/ unstruct's namespace & name
//
// This registry is useful to store arbitary unstructured
// instances in a way that is easy to find / filter later.
type AnyUnstructRegistry map[string]map[string]*unstructured.Unstructured

// String implements Stringer interface
func (m AnyUnstructRegistry) String() string {
	var message []string
	title := "Resource Instances:-"
	for vk, list := range m {
		for nsname, obj := range list {
			message = append(message, fmt.Sprintf("\t%s:%s %s", vk, nsname, obj.GetUID()))
		}
	}
	return fmt.Sprintf("%s\n%s\n", title, strings.Join(message, "\n"))
}

// IsEmpty returns true if this registry is empty
func (m AnyUnstructRegistry) IsEmpty() bool {
	for _, list := range m {
		for _, obj := range list {
			if obj != nil {
				return false
			}
		}
	}
	return true
}

// Len returns count of not nil items in this registry
func (m AnyUnstructRegistry) Len() int {
	var count int
	for _, list := range m {
		for _, obj := range list {
			if obj != nil {
				count++
			}
		}
	}
	return count
}

// FindResourceByGroupKindName finds the resource based on
// the given apigroup, kind and name
func (m AnyUnstructRegistry) FindResourceByGroupKindName(
	apiGroup, kind, name string,
) *unstructured.Unstructured {
	// The registry is keyed by apiVersion & kind, but we don't know
	// the version. So, check inside any GVK that matches the group and
	// kind, ignoring version.
	for key, resources := range m {
		if apiVer, k := ParseKeyToAPIVersionKind(key); k == kind {
			if g, _ := ParseAPIVersionToGroupVersion(apiVer); g == apiGroup {
				for n, res := range resources {
					if n == name {
						return res
					}
				}
			}
		}
	}
	return nil
}

// FilterResourceMapByGroupKind filters the registry for the
// resources based on the given apigroup and kind and returns
// them mapped by their namespace &/ name.
func (m AnyUnstructRegistry) FilterResourceMapByGroupKind(
	apiGroup, kind string,
) map[string]*unstructured.Unstructured {
	// The registry is keyed by apiVersion & kind, but we don't know
	// the version. So, check inside any GVK that matches the group and
	// kind, ignoring version.
	for key, resources := range m {
		if apiVer, k := ParseKeyToAPIVersionKind(key); k == kind {
			if g, _ := ParseAPIVersionToGroupVersion(apiVer); g == apiGroup {
				return resources
			}
		}
	}
	return nil
}

// FilterResourceListByGroupKind filters the registry for the
// resources based on the given apigroup and kind and
// returns them
func (m AnyUnstructRegistry) FilterResourceListByGroupKind(
	apiGroup, kind string,
) []*unstructured.Unstructured {
	rMap := m.FilterResourceMapByGroupKind(apiGroup, kind)
	if len(rMap) == 0 {
		return nil
	}
	var resources []*unstructured.Unstructured
	for _, res := range rMap {
		resources = append(resources, res)
	}
	return resources
}

// IsEmptyGroupKind returns true if resources with the provided
// apiGroup & kind are not available in this registry
func (m AnyUnstructRegistry) IsEmptyGroupKind(apiGroup, kind string) bool {
	return m.FilterResourceMapByGroupKind(apiGroup, kind) == nil
}

// relativeName returns the name of the attachment relative to the provided
// reference.
func relativeName(ref metav1.Object, obj *unstructured.Unstructured) string {
	if ref.GetNamespace() == "" && obj.GetNamespace() != "" {
		return fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	}
	return obj.GetName()
}

// namespaceNameOrName returns the name of the resource based on its
// scope
func namespaceNameOrName(obj *unstructured.Unstructured) string {
	if obj.GetNamespace() != "" {
		return fmt.Sprintf(
			"%s/%s", obj.GetNamespace(), obj.GetName(),
		)
	}
	return obj.GetName()
}

// describeObject returns a human-readable string to identify a
// given object.
func describeObject(obj *unstructured.Unstructured) string {
	if ns := obj.GetNamespace(); ns != "" {
		return fmt.Sprintf("%s/%s of kind %s", ns, obj.GetName(), obj.GetKind())
	}
	return fmt.Sprintf("%s of kind %s", obj.GetName(), obj.GetKind())
}

// sanitiseAPIVersion will make the apiVersion suitable to be used
// as value field in labels or annotations
func sanitiseAPIVersion(version string) string {
	return strings.ReplaceAll(version, "/", "-")
}

// DescObjAsSanitisedNSName will return the sanitised namespace name
// format corresponding to the given object
func DescObjAsSanitisedNSName(obj *unstructured.Unstructured) string {
	return strings.ReplaceAll(namespaceNameOrName(obj), "/", "-")
}

// DescObjectAsKey returns a machine readable string of the provided
// object. It can be used to identify the given object.
func DescObjectAsKey(obj *unstructured.Unstructured) string {
	ns := obj.GetNamespace()
	if ns != "" {
		return fmt.Sprintf("%s:%s:%s:%s",
			obj.GetAPIVersion(), obj.GetKind(), ns, obj.GetName(),
		)
	}

	return fmt.Sprintf("%s:%s:%s",
		obj.GetAPIVersion(), obj.GetKind(), obj.GetName(),
	)
}

// DescObjectAsSanitisedKey returns a sanitised name from the
// given object that can be used in annotation as key or value
func DescObjectAsSanitisedKey(obj *unstructured.Unstructured) string {
	ver := sanitiseAPIVersion(obj.GetAPIVersion())
	ns := obj.GetNamespace()
	if ns != "" {
		return fmt.Sprintf("%s-%s-%s-%s",
			ver, obj.GetKind(), ns, obj.GetName(),
		)
	}

	return fmt.Sprintf("%s-%s-%s",
		ver, obj.GetKind(), obj.GetName(),
	)
}

// List expands the registry map into a flat list of unstructured
// objects; in some random order.
func (m AnyUnstructRegistry) List() []*unstructured.Unstructured {
	var list []*unstructured.Unstructured
	for _, group := range m {
		for _, obj := range group {
			list = append(list, obj)
		}
	}
	return list
}

// ParseKeyToAPIVersionKind parses the given key into apiVersion
// and kind
func ParseKeyToAPIVersionKind(key string) (apiVersion, kind string) {
	parts := strings.SplitN(key, ".", 2)
	return parts[1], parts[0]
}

// ParseAPIVersionToGroupVersion parses the given version into
// respective group and version
func ParseAPIVersionToGroupVersion(apiVersion string) (group, version string) {
	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) == 1 {
		// It's a core version.
		return "", parts[0]
	}
	return parts[0], parts[1]
}
