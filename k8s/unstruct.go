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

package k8s

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

// UnstructListing manages operations on one or more unstructured
// instances
type UnstructListing struct {
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

// Unstruct returns a new instance of UnstructListing
func Unstruct(obj *unstructured.Unstructured) *UnstructListing {
	return UnstructList(
		[]*unstructured.Unstructured{obj},
	)
}

// UnstructList returns a new instance of UnstructListing
func UnstructList(list []*unstructured.Unstructured) *UnstructListing {
	s := &UnstructListing{
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

// WithCondition adds a lazy condition
func (s *UnstructListing) WithCondition(name string, condition *LazyCondition) *UnstructListing {
	// store this condition against its name
	s.conditionNames[name] = true
	s.conditions[name] = condition
	return s
}

// Eval evaluates all the conditions against each of
// its objects
func (s *UnstructListing) Eval() (*UnstructListing, error) {
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

// GetForCondition returns the instances that have passed the named
// evaluation
//
// NOTE:
//	This should be invoked after invoking Eval
func (s *UnstructListing) GetForCondition(condName string) (*unstructured.Unstructured, bool, error) {
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

// ListForCondition returns all the instances that have passed the
// named evaluation
//
// NOTE:
//	This should be invoked after invoking Eval
func (s *UnstructListing) ListForCondition(condName string) ([]*unstructured.Unstructured, bool, error) {
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

// FailureReasonsForObj return the reasons of evaluation failure if
// any against the provided instance
//
// NOTE:
//	This should be invoked after invoking Run
func (s *UnstructListing) FailureReasonsForObj(obj *unstructured.Unstructured) ([]error, bool, error) {
	if !s.isRun {
		return nil,
			false,
			errors.Errorf("Invalid failures call: Run must be invoked before this")
	}
	failures := s.failureReasons[UniqueName(obj)]
	if len(failures) == 0 {
		return nil, false, nil
	}
	return failures, true, nil
}

// FailureReasons return the reasons of evaluation failure if
// any against the provided instance
//
// NOTE:
//	This should be invoked after invoking Run
func (s *UnstructListing) FailureReasons() ([]error, error) {
	if !s.isRun {
		return nil,
			errors.Errorf("Invalid failures call: Run must be invoked before this")
	}
	var allFailures []error
	for _, failMsgs := range s.failureReasons {
		allFailures = append(allFailures, failMsgs...)
	}
	return allFailures, nil
}

// Rejects list all the instances that did not pass
// any of the conditions
func (s *UnstructListing) Rejects() []*unstructured.Unstructured {
	var rejects []*unstructured.Unstructured
	for _, obj := range s.objs {
		if !s.successfulInstances[UniqueName(obj)] {
			rejects = append(rejects, obj)
		}
	}
	return rejects
}
