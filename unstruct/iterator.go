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

package unstruct

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// OperationFn is a typed function that abstracts an operation
// against an unstructured instance
type OperationFn func(*unstructured.Unstructured) error

// SliceIteration provides iteration operations against its
// list of items. This provides a functional approach to
// iterate the list & execute callbacks during iteration.
type SliceIteration struct {
	Items []interface{}
}

// ItemToUnstruct transforms the provided instance into appropriate
// unstructured instance
func (i SliceIteration) ItemToUnstruct(idx int, item interface{}) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetUnstructuredContent(
		map[string]interface{}{
			//	The given item is mapped against spec field
			"spec": item,
		},
	)
	// NOTE:
	//
	// 	These values are set to avoid getting into any errors
	// in-case of any internal validation during parsing of
	// this unstructured instance
	u.SetKind("SliceItem")
	u.SetAPIVersion("v1")
	u.SetName(fmt.Sprintf("elem-%d", idx))
	return u
}

// SliceIterator is the constructor that returns a new instance of
// SliceIteration
func SliceIterator(items []interface{}) *SliceIteration {
	return &SliceIteration{Items: items}
}

// ForEach loops through this list and runs each item
// against the provided function(s) i.e. callback(s)
func (i *SliceIteration) ForEach(must OperationFn, others ...OperationFn) error {
	var unFns []OperationFn
	unFns = append(unFns, must)
	unFns = append(unFns, others...)
	for idx, item := range i.Items {
		// we must convert this item to an unstructured instance
		unItem := i.ItemToUnstruct(idx, item)
		// execute this item against all the callbacks
		for _, fn := range unFns {
			err := fn(unItem)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
