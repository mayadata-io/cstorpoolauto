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

package k8s

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

// GetNestedSlice returns the slice found at given field path
// of the given object
func GetNestedSlice(obj *unstructured.Unstructured, fields ...string) ([]interface{}, error) {
	nestedSlice, found, err := unstructured.NestedSlice(obj.Object, fields...)
	if err != nil || !found {
		return nil,
			errors.Wrapf(
				err,
				"Failed to find slice at %s: Object %s %s",
				strings.Join(fields, "."), obj.GetNamespace(), obj.GetName(),
			)
	}
	if !found {
		return nil,
			errors.Errorf(
				"No slice is found at %s: Object %s %s",
				strings.Join(fields, "."), obj.GetNamespace(), obj.GetName(),
			)
	}
	return nestedSlice, nil
}

// MergeNestedSlice merges the given map against a
// slice of maps found at given field path & returns the
// updated slice.
//
// TODO (@amitkumardas): Unit Tests
func MergeNestedSlice(obj *unstructured.Unstructured, new map[string]interface{}, fields ...string) ([]interface{}, error) {
	nestedSlice, err := GetNestedSlice(obj, fields...)
	if err != nil {
		return nil, err
	}
	var indexKey string
	var indexValue string
	for k, v := range new {
		// One of these keys can be used to merge
		//
		// Note:
		//	One can't rely on ordering amongst these keys.
		//
		// Note:
		//	It is expected that the provided map to have only one
		// of these keys that can uniquely identify this map amongst
		// the slice of maps.
		if k == "uid" || k == "id" || k == "name" || k == "type" {
			indexKey = k
			indexValue = fmt.Sprintf("%s", v)
			break
		}
	}
	var found bool
	var foundAt int
	for i, item := range nestedSlice {
		itemMap, ok := item.(map[string]interface{})
		if !ok {
			return nil,
				errors.Errorf(
					"Invalid nested slice: Want map[string]interface{}: Got %T", item,
				)
		}
		for k, v := range itemMap {
			val := fmt.Sprintf("%s", v)
			if k == indexKey && val == indexValue {
				found = true
				foundAt = i
				break
			}
		}
		if found {
			break
		}
	}
	if found {
		// replace with new item
		nestedSlice[foundAt] = new
	} else {
		// add the new item
		nestedSlice = append(nestedSlice, new)
	}
	return nestedSlice, nil
}

// MergeAndSetNestedSlice merges the provided map against a slice
// of maps at given field path. It then sets the updated slice against
// the provided object.
func MergeAndSetNestedSlice(obj *unstructured.Unstructured, new map[string]interface{}, fields ...string) ([]interface{}, error) {
	updatedSlice, err := MergeNestedSlice(obj, new, fields...)
	if err != nil {
		return nil, err
	}
	err = unstructured.SetNestedSlice(obj.Object, updatedSlice, fields...)
	if err != nil {
		return nil, err
	}
	return updatedSlice, nil
}

// IsStatus returns true if status set against the
// provided instance if not nil
func IsStatus(obj *unstructured.Unstructured) (bool, error) {
	status, found, err := unstructured.NestedMap(obj.Object, "status")
	if status == nil {
		found = false
	}
	// No need to check if err != nil, since errors.Wrapf takes
	// care of it
	return found, errors.Wrapf(
		err,
		"Failed to get .status: Object %s %s", obj.GetNamespace(), obj.GetName(),
	)
}

// SetStatusToEmptyConditions sets the given object's status
// conditions to an empty slice
func SetStatusToEmptyConditions(obj *unstructured.Unstructured) error {
	statusConds := map[string]interface{}{
		"conditions": []interface{}{},
	}
	err := unstructured.SetNestedMap(obj.Object, statusConds, "status")
	return errors.Wrapf(
		err,
		"Failed to set empty status conditions: Object %s %s",
		obj.GetNamespace(), obj.GetName(),
	)
}

// MergeStatusConditions merges the provided conditions with existing
// ones if any & returns the updated conditions
//
// TODO (@amitkumardas): Unit Tests
func MergeStatusConditions(obj *unstructured.Unstructured, newCondition map[string]interface{}) ([]interface{}, error) {
	found, err := IsStatus(obj)
	if err != nil {
		return nil, err
	}
	if !found {
		err := SetStatusToEmptyConditions(obj)
		if err != nil {
			return nil, err
		}
	}
	return MergeNestedSlice(obj, newCondition, "status", "conditions")
}

// MergeAndSetStatusConditions merges the provided conditions with existing
// ones if any against the provided object
//
// TODO (@amitkumardas): Unit Tests
func MergeAndSetStatusConditions(obj *unstructured.Unstructured, newCondition map[string]interface{}) ([]interface{}, error) {
	return MergeAndSetNestedSlice(obj, newCondition, "status", "conditions")
}

// GetNestedMap returns the map found at given field path of the given
// object
func GetNestedMap(obj *unstructured.Unstructured, fields ...string) (map[string]interface{}, error) {
	nestedMap, found, err := unstructured.NestedMap(obj.Object, fields...)
	if err != nil || !found {
		return nil, err
	}
	return nestedMap, nil
}

// GetNestedMapOrEmpty returns the map found at given field path
// of the given object. It returns empty map in case of error or
// if this map was not found.
func GetNestedMapOrEmpty(obj *unstructured.Unstructured, fields ...string) (map[string]interface{}, error) {
	nestedMap, err := GetNestedMap(obj, fields...)
	if nestedMap == nil {
		nestedMap = map[string]interface{}{}
	}
	return nestedMap, err
}

// MergeToAnnotations merges the given key value pair against the
// provided annotations
func MergeToAnnotations(key, value string, given map[string]string) map[string]string {
	if given == nil {
		given = map[string]string{}
	}
	// this will add the key or update if key is already present
	given[key] = value
	return given
}

// GetAnnotationForKey fetches the annotation value from the given
// annotations & annotation key
func GetAnnotationForKey(given map[string]string, key string) (string, bool) {
	if len(given) == 0 {
		return "", false
	}
	val, found := given[key]
	return val, found
}

// UnstructToTyped transforms the provided unstruct instance
// to target type
func UnstructToTyped(src *unstructured.Unstructured, target interface{}) error {
	if src == nil || src.UnstructuredContent() == nil {
		return errors.Errorf(
			"Can't transform unstruct to typed: Nil unstruct content",
		)
	}
	if target == nil {
		return errors.Errorf(
			"Can't transform unstruct to typed: Nil target",
		)
	}
	return runtime.DefaultUnstructuredConverter.FromUnstructured(
		src.UnstructuredContent(), target,
	)
}
