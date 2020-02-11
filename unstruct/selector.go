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
	"reflect"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
	"openebs.io/metac/controller/common/selector"
	"openebs.io/metac/dynamic/apply"
)

// Selection provides select & match capabilities to unstructured
type Selection struct {
	Object           *unstructured.Unstructured
	ResourceSelector metac.ResourceSelector
}

// Selector returns a new instance of Selection
func Selector(terms metac.ResourceSelector, obj *unstructured.Unstructured) *Selection {
	return &Selection{
		Object:           obj,
		ResourceSelector: terms,
	}
}

// IsMatchOrError returns true if selection terms matches with
// its object
func (s *Selection) IsMatchOrError() (bool, error) {
	eval := selector.Evaluation{
		Target: s.Object,
		Terms:  s.ResourceSelector.SelectorTerms,
	}
	return eval.RunMatch()
}

// IsMatch returns true if selection terms matches with
// its object
func (s *Selection) IsMatch() bool {
	match, _ := s.IsMatchOrError()
	return match
}

// ListSelection provides select & match capabilities to list of
// unstructured instances
type ListSelection struct {
	// list of instances against which selector terms get applied
	Objects []*unstructured.Unstructured

	// selector terms that are applied against the list of objects
	ResourceSelector metac.ResourceSelector

	// flag when set to true will not consider cached results
	DisableCache bool

	// matches & nomatches represent the cached copies of objects
	// that matched & did not match _(against the selector terms)_
	// correspondingly
	matches   []*unstructured.Unstructured
	nomatches []*unstructured.Unstructured
}

// ListSelector returns a new instance of ListSelection
func ListSelector(
	terms metac.ResourceSelector, objs ...*unstructured.Unstructured,
) *ListSelection {
	s := &ListSelection{
		ResourceSelector: terms,
	}
	for _, obj := range objs {
		if obj == nil || obj.UnstructuredContent() == nil {
			// accept only non nil instances
			continue
		}
		s.Objects = append(s.Objects, obj)
	}
	return s
}

// List returns the matches & no-matches
func (s *ListSelection) List() (matches, nomatches []*unstructured.Unstructured) {
	for _, obj := range s.Objects {
		sr := Selector(s.ResourceSelector, obj)
		if sr.IsMatch() {
			matches = append(matches, obj)
		} else {
			nomatches = append(nomatches, obj)
		}
	}
	return
}

// ListOrCached returns the cached matches & no-matches if present
// or runs the selector against its objects
func (s *ListSelection) ListOrCached() (matches, nomatches []*unstructured.Unstructured) {
	if !s.DisableCache && (len(s.matches) != 0 || len(s.nomatches) != 0) {
		// selector was run earlier, return the cached matches
		return s.matches, s.nomatches
	}
	s.matches, s.nomatches = s.List()
	return s.matches, s.nomatches
}

// MatchContains returns true if all the provided instance
// is a successful match
func (s *ListSelection) MatchContains(target *unstructured.Unstructured) bool {
	matches, _ := s.ListOrCached()
	l := List(matches)
	return l.Contains(target)
}

// NoMatchContains returns true if all the provided instance
// is not a successful match
func (s *ListSelection) NoMatchContains(target *unstructured.Unstructured) bool {
	_, nomatches := s.ListOrCached()
	l := List(nomatches)
	return l.Contains(target)
}

// MatchContainsAll returns true if all the provided instances
// are successful matches
func (s *ListSelection) MatchContainsAll(targets []*unstructured.Unstructured) bool {
	matches, _ := s.ListOrCached()
	l := List(matches)
	return l.ContainsAll(targets)
}

// NoMatchContainsAll returns true if all the provided instances
// are not successful matches
func (s *ListSelection) NoMatchContainsAll(targets []*unstructured.Unstructured) bool {
	_, nomatches := s.ListOrCached()
	l := List(nomatches)
	return l.ContainsAll(targets)
}

// MatchCount returns true if number of successful matches
// equals the provided count
func (s *ListSelection) MatchCount(target int) bool {
	matches, _ := s.ListOrCached()
	return len(matches) == target
}

// NoMatchCount returns true if number of un-successful matches
// equals the provided count
func (s *ListSelection) NoMatchCount(target int) bool {
	_, nomatches := s.ListOrCached()
	return len(nomatches) == target
}

// mergeByDesired executes a 3 way merge operation by
// merging the observed state with desired state. Last
// applied state is considered same as desired state.
func mergeByDesired(observed, desired *unstructured.Unstructured) (map[string]interface{}, error) {
	return apply.Merge(
		observed.UnstructuredContent(), // observed
		desired.UnstructuredContent(),  // last applied
		desired.UnstructuredContent(),  // desired
	)
}

// MatchDesiredOrError returns true if provided instance
// is contained & its desired state matches.
//
// NOTE:
//	Here desired state equality is performed. One just
// needs to provide the section from the entire structure
// that will be used to match.
func (s *ListSelection) MatchDesiredOrError(desired *unstructured.Unstructured) (bool, error) {
	if desired == nil || desired.UnstructuredContent() == nil {
		return false, errors.Errorf("Can't match target against list: Nil target")
	}
	matches, _ := s.ListOrCached()
	for _, observedMatch := range matches {
		if observedMatch.GetName() == desired.GetName() &&
			observedMatch.GetNamespace() == desired.GetNamespace() &&
			observedMatch.GetKind() == desired.GetKind() &&
			observedMatch.GetAPIVersion() == desired.GetAPIVersion() {
			merged, err := mergeByDesired(observedMatch, desired)
			if err != nil {
				return false, err
			}
			if !reflect.DeepEqual(merged, observedMatch.UnstructuredContent()) {
				return false, nil
			}
			return true, nil
		}
	}
	return false, nil
}

// MatchDesired returns true if provided instance match
// the selection & its desired state matches.
//
// NOTE:
//	Here desired state equality is performed. One just
// needs to provide the section from the entire structure
// that will be used to match.
func (s *ListSelection) MatchDesired(desired *unstructured.Unstructured) bool {
	ismatch, err := s.MatchDesiredOrError(desired)
	if err != nil {
		// we swallow the error & return false
		return false
	}
	return ismatch
}

// MatchDesiredAll returns true if all provided instances match
// the selection && are equal as well.
func (s *ListSelection) MatchDesiredAll(desired []*unstructured.Unstructured) bool {
	if len(desired) == 0 {
		// we dont match nil list
		return false
	}
	for _, desiredState := range desired {
		ismatch := s.MatchDesired(desiredState)
		if !ismatch {
			// a single nomatch fails this logic
			return false
		}
	}
	return true
}
