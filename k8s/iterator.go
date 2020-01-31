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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// UnstructOpsFn is a typed function that abstracts operations
// against an unstructured instance
type UnstructOpsFn func(*unstructured.Unstructured) error

// SliceIteration provides iteration operations against its
// list of items
type SliceIteration struct {
	Items []interface{}
}

// AsUnstruct transforms the provided instance into appropriate
// unstructured instance
func (i SliceIteration) AsUnstruct(idx int, given interface{}) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetUnstructuredContent(
		map[string]interface{}{
			"spec": given,
		},
	)
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

// ForEach loops through this instances list and runs each of the
// item against the provided functions
func (i *SliceIteration) ForEach(must UnstructOpsFn, others ...UnstructOpsFn) error {
	var unFns []UnstructOpsFn
	unFns = append(unFns, must)
	unFns = append(unFns, others...)
	for idx, elem := range i.Items {
		un := i.AsUnstruct(idx, elem)
		for _, fn := range unFns {
			err := fn(un)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
