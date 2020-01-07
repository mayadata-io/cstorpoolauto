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
	"cstorpoolauto/util/pointer"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Eval flags one or more checks against an unstructured
// instance as a boolean or error
type Eval struct {
	obj     *unstructured.Unstructured
	err     error
	success *bool
}

// Verify returns a new instance of Eval
func Verify(obj *unstructured.Unstructured) *Eval {
	if obj == nil {
		return &Eval{
			err: errors.Errorf("Nil object provided"),
		}
	}
	return &Eval{obj: obj}
}

func (e *Eval) skip() bool {
	if e.err != nil {
		return true
	}
	if e.success == nil {
		return false
	}
	return !*e.success
}

// IsOfKind evaluates if provided kind matches this
// object
func (e *Eval) IsOfKind(kind string) *Eval {
	if e.skip() {
		return e
	}
	if kind == "" {
		e.success = pointer.BoolPtr(false)
		return e
	}
	e.success = pointer.BoolPtr(e.obj.GetKind() == kind)
	return e
}

// IsOfAPIVersion evaluates if provided api version matches this
// object
func (e *Eval) IsOfAPIVersion(apiversion string) *Eval {
	if e.skip() {
		return e
	}
	if apiversion == "" {
		e.success = pointer.BoolPtr(false)
		return e
	}
	e.success = pointer.BoolPtr(e.obj.GetAPIVersion() == apiversion)
	return e
}

// hasPair evaluates if provided pair is available
// in the provided list of pairs
func (e *Eval) hasPair(pairs map[string]string, key, value string) bool {
	if e.skip() {
		return false
	}
	if key == "" || len(pairs) == 0 {
		e.success = pointer.BoolPtr(false)
		return false
	}
	got, _ := GetAnnotationForKey(pairs, key)
	return got == value
}

// HasAnn evaluates if provided annotation is available
// in this object
func (e *Eval) HasAnn(key, value string) *Eval {
	if e.skip() {
		return e
	}
	found := e.hasPair(e.obj.GetAnnotations(), key, value)
	e.success = pointer.BoolPtr(found)
	return e
}

// HasAnns evaluates if provided annotations are available
// in this object
func (e *Eval) HasAnns(given map[string]string) *Eval {
	if e.skip() {
		return e
	}
	anns := e.obj.GetAnnotations()
	if len(given) == 0 || len(anns) == 0 {
		e.success = pointer.BoolPtr(false)
		return e
	}
	var found bool
	for key, val := range given {
		found = e.hasPair(anns, key, val)
		if !found {
			break
		}
	}
	e.success = pointer.BoolPtr(found)
	return e
}

// HasLabel evaluates if provided label is available
// in this object
func (e *Eval) HasLabel(key, value string) *Eval {
	if e.skip() {
		return e
	}
	found := e.hasPair(e.obj.GetLabels(), key, value)
	e.success = pointer.BoolPtr(found)
	return e
}

// HasLabels evaluates if provided labels are available
// in this object
func (e *Eval) HasLabels(given map[string]string) *Eval {
	if e.skip() {
		return e
	}
	lbls := e.obj.GetLabels()
	if len(given) == 0 || len(lbls) == 0 {
		e.success = pointer.BoolPtr(false)
		return e
	}
	var found bool
	for key, val := range given {
		found = e.hasPair(lbls, key, val)
		if !found {
			break
		}
	}
	e.success = pointer.BoolPtr(found)
	return e
}

// Check returns the final evaluation
func (e *Eval) Check() (bool, error) {
	if e.err != nil {
		return false, e.err
	}
	if e.success == nil {
		return false, nil
	}
	return *e.success, nil
}

// MustCheck returns the final evaluation
//
// NOTE:
//	This panics if there were any errors
func (e *Eval) MustCheck() bool {
	if e.err != nil {
		panic(e.err)
	}
	if e.success == nil {
		return false
	}
	return *e.success
}
