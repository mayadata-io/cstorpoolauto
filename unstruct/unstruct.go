/*
Copyright 2020 The MayaData Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package unstruct

import (
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// UniqueName gets the unique name from the given instance
// This name can be considered as unique across multiple
// unstruct instance with different kinds & versions.
func UniqueName(obj *unstructured.Unstructured) string {
	gvk := obj.GetAPIVersion() + "/" + obj.GetKind()
	if obj.GetNamespace() == "" {
		return gvk + "/" + obj.GetName()
	}
	return gvk + "/" + obj.GetNamespace() + "/" + obj.GetName()
}

// Listing manages operations on one or more unstructured
// instances
//
// TODO (@amitkumardas):
//	This struct along with its methods might be deprecated
// in favour of structures & method found in selector.go
type Listing struct {
	objs []*unstructured.Unstructured

	// conditionNames is the registry to hold evaluation
	// names that are used during selection process
	conditionNames map[string]bool

	// conditions holds all the conditions to be run
	// against all the targeted unstruct instances
	conditions map[string]*LazyCondition

	// holds the successful evaluations
	successfulConds map[string][]*unstructured.Unstructured

	// holds the reason of un-successful evaluations
	failureReasons map[string][]error

	// holds the namespace/name of unstruct instances which
	// had one or more successful matches
	successfulInstances map[string]bool

	// flags if this selection instance has been executed
	//
	// 	This returns true if Run() method was executed
	isRun bool

	// runtime error if any
	err error
}

// AsListing returns a new instance of Listing
func AsListing(obj *unstructured.Unstructured) *Listing {
	return NewListing(
		[]*unstructured.Unstructured{obj},
	)
}

// NewListing returns a new instance of Listing from the
// list of provided unstructured instances
func NewListing(list []*unstructured.Unstructured) *Listing {
	s := &Listing{
		// init the maps
		conditionNames:      map[string]bool{},
		conditions:          map[string]*LazyCondition{},
		successfulConds:     map[string][]*unstructured.Unstructured{},
		successfulInstances: map[string]bool{},
		failureReasons:      map[string][]error{},
	}
	for _, obj := range list {
		if obj == nil {
			s.err = errors.Errorf("Invalid invocation: Nil object found")
			break
		}
	}
	if s.err != nil {
		return s
	}
	// set the provided objects if they are not nil
	s.objs = append(s.objs, list...)
	return s
}

// Contains returns true if provided name && uid is
// is available in this List.
func (s *Listing) Contains(target *unstructured.Unstructured) bool {
	if target == nil || s.err != nil {
		return false
	}
	for _, available := range s.objs {
		if available.GetName() == target.GetName() &&
			available.GetNamespace() == target.GetNamespace() &&
			available.GetKind() == target.GetKind() &&
			available.GetAPIVersion() == target.GetAPIVersion() {
			return true
		}
	}
	return false
}

// ContainsAll returns true if each item in the provided targets
// is available in this List.
func (s *Listing) ContainsAll(targets []*unstructured.Unstructured) bool {
	if s.err != nil || len(s.objs) != len(targets) {
		return false
	}
	if len(targets) == 0 && len(s.objs) == len(targets) {
		return true
	}
	for _, t := range targets {
		if !s.Contains(t) {
			// return false if any item does not match
			return false
		}
	}
	return true
}

// WithCondition adds a lazy condition
func (s *Listing) WithCondition(name string, condition *LazyCondition) *Listing {
	// store this condition against its name
	s.conditionNames[name] = true
	s.conditions[name] = condition
	return s
}

// EvalAllConditions evaluates all the conditions against each of
// Listing items
func (s *Listing) EvalAllConditions() (*Listing, error) {
	if s.err != nil {
		// return as there were runtime errors
		return nil, s.err
	}
	s.isRun = true
	for _, obj := range s.objs {
		for name, cond := range s.conditions {
			ok, err := cond.Check(obj)
			if err != nil {
				return s, err
			}
			if ok {
				s.successfulConds[name] = append(s.successfulConds[name], obj)
				s.successfulInstances[UniqueName(obj)] = true
			} else {
				uName := UniqueName(obj)
				s.failureReasons[uName] =
					append(s.failureReasons[uName], cond.FailureReasons()...)
			}
		}
	}
	return s, nil
}

// GetObjForCondition returns the instances that have passed the named
// evaluation
//
// NOTE:
//	This should be invoked after invoking Eval
func (s *Listing) GetObjForCondition(condName string) (*unstructured.Unstructured, bool, error) {
	if !s.isRun {
		return nil,
			false,
			errors.Errorf("Invalid get call: Eval must be invoked before this")
	}
	if !s.conditionNames[condName] {
		return nil, false, errors.Errorf("Invalid condition %q", condName)
	}
	matches := s.successfulConds[condName]
	if len(matches) == 0 {
		return nil, false, nil
	}
	return matches[0], true, nil
}

// ListObjsForCondition returns all the instances that have passed the
// named evaluation
//
// NOTE:
//	This should be invoked after invoking Eval
func (s *Listing) ListObjsForCondition(condName string) ([]*unstructured.Unstructured, bool, error) {
	if !s.isRun {
		return nil,
			false,
			errors.Errorf("Invalid list call: Eval must be invoked before this")
	}
	if !s.conditionNames[condName] {
		return nil, false, errors.Errorf("Invalid condition %q", condName)
	}
	matches := s.successfulConds[condName]
	if len(matches) == 0 {
		return nil, false, nil
	}
	return matches, true, nil
}

// ListConditionFailuresFor return the reasons of evaluation failure if
// any w.r.t the provided instance
//
// NOTE:
//	This should be invoked after invoking Eval
func (s *Listing) ListConditionFailuresFor(obj *unstructured.Unstructured) ([]error, bool, error) {
	if !s.isRun {
		return nil,
			false,
			errors.Errorf("Invalid failures call: Eval must be invoked before this")
	}
	failures := s.failureReasons[UniqueName(obj)]
	if len(failures) == 0 {
		return nil, false, nil
	}
	return failures, true, nil
}

// ListAllConditionFailures return the reasons of evaluation failure if
// any
//
// NOTE:
//	This should be invoked after invoking Eval
func (s *Listing) ListAllConditionFailures() ([]error, error) {
	if !s.isRun {
		return nil,
			errors.Errorf("Invalid failures call: Eval must be invoked before this")
	}
	var allFailures []error
	for _, failMsgs := range s.failureReasons {
		allFailures = append(allFailures, failMsgs...)
	}
	return allFailures, nil
}

// ListAllConditionRejects list all the instances that did not pass
// any of the conditions
func (s *Listing) ListAllConditionRejects() []*unstructured.Unstructured {
	var rejects []*unstructured.Unstructured
	for _, obj := range s.objs {
		if !s.successfulInstances[UniqueName(obj)] {
			rejects = append(rejects, obj)
		}
	}
	return rejects
}
