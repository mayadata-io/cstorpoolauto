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

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/validation"
)

func validateLabelKey(k string) error {
	if errs := validation.IsQualifiedName(k); len(errs) != 0 {
		return fmt.Errorf("invalid label key %q: %s", k, strings.Join(errs, "; "))
	}
	return nil
}

func validateLabelValue(k, v string) error {
	if errs := validation.IsValidLabelValue(v); len(errs) != 0 {
		return fmt.Errorf(
			"invalid label value: %q: at key: %q: %s", v, k, strings.Join(errs, "; "),
		)
	}
	return nil
}

// KeySlice is a map of key to slice of string
// values
type KeySlice map[string][]string

// Get returns the values corresponding to the
// given key. It returns an empty slice if key
// is not found.
func (s KeySlice) Get(key string) []string {
	for k, vals := range s {
		if k == key {
			return vals
		}
	}
	return []string{}
}

// Has returns true if given key is available
func (s KeySlice) Has(key string) bool {
	for k := range s {
		if k == key {
			return true
		}
	}
	return false
}

// SliceSelectorMatcher handles match operation of
// SliceSelectorRequirement.
//
// This should be initialized via NewSliceSelectorMatcher
// constructor for creating a valid matcher instance.
type SliceSelectorMatcher struct {
	// key is the label key that the selector applies to.
	Key string

	// operator represents a key's relationship to a set of values.
	Operator SliceSelectorOperator

	// Store values as a map of boolean for easy &
	// efficient filter based logic.
	//
	// NOTE:
	//	All the usecases involves verifying the existence
	// of a value.
	HasValueStore map[string]bool
}

func (r SliceSelectorMatcher) String() string {
	return "Slice selector matcher"
}

// NewSliceSelectorMatcher returns a new instance of SliceSelectorMatcher.
//
// If any of these rules is violated, an error is returned:
//
// (1) The operator can only be In, NotIn, Equals, or NotEquals.
// (2) For all the operators, the key & values that are set must be non-empty.
// (3) The key or value is invalid due to its length, or sequence of characters.
// 	See validateLabelKey & validateLabelValue for more details.
//
// NOTE:
// 	The empty string is a valid value in the input values set.
func NewSliceSelectorMatcher(
	key string, op SliceSelectorOperator, vals []string,
) (*SliceSelectorMatcher, error) {
	ssm := &SliceSelectorMatcher{}
	if key == "" {
		return nil, errors.Errorf("%s: Key can't be empty", ssm)
	}
	if len(vals) == 0 {
		return nil, errors.Errorf("%s: Values can't be empty", ssm)
	}
	if err := validateLabelKey(key); err != nil {
		return nil, err
	}
	switch op {
	case SliceSelectorOpIn, SliceSelectorOpNotIn,
		SliceSelectorOpEquals, SliceSelectorOpNotEquals:
		// are supported
	default:
		return nil, errors.Errorf("%s: Operator '%v' is not recognized", ssm, op)
	}

	for i := range vals {
		if err := validateLabelValue(key, vals[i]); err != nil {
			return nil, err
		}
	}
	// set evaluated values
	ssm.Key = key
	ssm.Operator = op
	for _, v := range vals {
		ssm.HasValueStore[v] = true
	}

	return ssm, nil
}

// hasValues does an exact match of the given slice of
// values with its own list of values
//
// NOTE:
//	Empty given list will return true
func (r *SliceSelectorMatcher) hasValues(given []string) bool {
	for _, v := range given {
		if !r.HasValueStore[v] {
			return false
		}
	}
	return true
}

// Match returns true if the Requirement matches the give KeySlice.
func (r *SliceSelectorMatcher) Match(kslice KeySlice) bool {
	switch r.Operator {
	case SliceSelectorOpIn, SliceSelectorOpEquals:
		return r.hasValues(kslice.Get(r.Key))
	case SliceSelectorOpNotIn, SliceSelectorOpNotEquals:
		if !kslice.Has(r.Key) {
			return true
		}
		return !r.hasValues(kslice.Get(r.Key))
	default:
		return false
	}
}

// SliceSelector exposes match operation against string slices
type SliceSelector struct {
	matchers []SliceSelectorMatcher

	matchFn func(KeySlice) bool
}

// SliceSelectorConfig helps in creating a new instance of
// SliceSelector
type SliceSelectorConfig struct {
	// MatchSlice is a map i.e. {key,value} pairs based slice selector
	// requirements. A single {key,value} in the MatchFields map is
	// equivalent to an element of matchFieldExpressions, whose key field
	// is "key", the operator is "In", and the values array contains only
	// "value".
	//
	// A key should represent the nested field path separated by dot(s)
	// i.e. '.'
	//
	// A MatchSlice is converted into a list of SliceSelectorRequirement
	// that are ANDed to determine if the selector matches its target or
	// not.
	MatchSlice map[string][]string

	// MatchSliceExpressions is a list of slice selector requirements.
	// These requirements are ANDed to determine if the selector matches
	// its target or not.
	MatchSliceExpressions []SliceSelectorRequirement
}

// NewSliceSelectorAlwaysTrue returns a new instance of SliceSelector
// that evaluates the match operation to true.
func NewSliceSelectorAlwaysTrue() *SliceSelector {
	return &SliceSelector{
		matchFn: func(k KeySlice) bool {
			return true
		},
	}
}

// NewSliceSelector returns a new instance of SliceSelector
func NewSliceSelector(config SliceSelectorConfig) (*SliceSelector, error) {
	if len(config.MatchSlice)+len(config.MatchSliceExpressions) == 0 {
		return NewSliceSelectorAlwaysTrue(), nil
	}
	selector := &SliceSelector{}
	for k, v := range config.MatchSlice {
		r, err := NewSliceSelectorMatcher(k, SliceSelectorOpEquals, v)
		if err != nil {
			return nil, err
		}
		selector = selector.Add(*r)
	}
	for _, expr := range config.MatchSliceExpressions {
		switch expr.Operator {
		case SliceSelectorOpIn, SliceSelectorOpNotIn,
			SliceSelectorOpEquals, SliceSelectorOpNotEquals:
		default:
			return nil, errors.Errorf("%q is not a valid slice selector operator", expr.Operator)
		}
		m, err := NewSliceSelectorMatcher(expr.Key, expr.Operator, expr.Values)
		if err != nil {
			return nil, err
		}
		selector = selector.Add(*m)
	}
	return selector, nil
}

// Add adds the given matcher to this selector instance
func (s *SliceSelector) Add(m SliceSelectorMatcher) *SliceSelector {
	s.matchers = append(s.matchers, m)
	return s
}

// Match returns true if selector matches the given target
func (s *SliceSelector) Match(target KeySlice) bool {
	if s.matchFn != nil {
		return s.matchFn(target)
	}
	for _, m := range s.matchers {
		if !m.Match(target) {
			return false
		}
	}
	return true
}
