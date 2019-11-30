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
	"strings"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

// AnySelector exposes match operation
// against any target instance based on its
// select terms
type AnySelector []*AnySelectorTerm

// Match does a AND operation against all the match
// fields present in AnySelectorTerm.
func (e AnySelector) Match(target *unstructured.Unstructured) (bool, error) {
	if len(e) == 0 {
		// no terms imply everything matches
		return true, nil
	}

	if target == nil {
		return false, errors.Errorf("Selector match failed: Nil target")
	}

	// A match function understand specific match expressions
	//
	// NOTE:
	// 	Match expressions are found in a term
	matchFns := []func(AnySelectorTerm, *unstructured.Unstructured) (bool, error){
		isFieldMatch,
		isAnnotationMatch,
		isLabelMatch,
		isSliceMatch,
	}

	// Matching terms are ORed against the target
	// Hence if a term is a match i.e. if any iteration has
	// a successful match, the overall match is a success &
	// returns true
	for _, term := range e {
		if term == nil {
			continue
		}

		// this is a counter which if equal to number of
		// successful match functions implies the term's
		// match was a success
		successfulTermMatchCount := 0

		// Each match specified in a term are ANDed
		//
		// One of more match(-es) declared in a term are executed
		// via match functions
		//
		// A match function deals with its own match expression(s)
		for _, match := range matchFns {
			isMatch, err := match(*term, target)
			if err != nil {
				return false, err
			}
			if !isMatch {
				// Since each match within a term is an AND operation,
				// a failed match function implies current term failed.
				// Hence ignore the current term & start with the
				// next term
				break
			}
			successfulTermMatchCount++
		}

		// check whether all match expressions in the current term
		// succeeded
		if successfulTermMatchCount == len(matchFns) {
			// no need to check for other terms since
			// terms are ORed
			return true, nil
		}
	}

	// at this point no terms would have succeeded
	return false, nil
}

// isAnnotationMatch runs the match operation against the
// target's annotations
func isAnnotationMatch(term AnySelectorTerm, target *unstructured.Unstructured) (bool, error) {
	if term.MatchAnnotations == nil && term.MatchAnnotationExpressions == nil {
		// match is true if there are no annotation based selectors
		return true, nil
	}

	if target == nil {
		return false, errors.Errorf("Annotation selector failed: Nil target")
	}

	sel := &metav1.LabelSelector{
		MatchLabels:      term.MatchAnnotations,
		MatchExpressions: term.MatchAnnotationExpressions,
	}
	annSel, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return false, errors.Wrapf(err, "Invalid annotation selector: %v", sel)
	}

	return annSel.Matches(labels.Set(target.GetAnnotations())), nil
}

// isLabelMatch runs the match operation against the
// target's labels
func isLabelMatch(term AnySelectorTerm, target *unstructured.Unstructured) (bool, error) {
	if term.MatchLabels == nil && term.MatchLabelExpressions == nil {
		// match is true if there are no label based selectors
		return true, nil
	}

	if target == nil {
		return false, errors.Errorf("Label selector failed: Nil target")
	}

	sel := &metav1.LabelSelector{
		MatchLabels:      term.MatchLabels,
		MatchExpressions: term.MatchLabelExpressions,
	}
	lblSel, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return false, errors.Wrapf(err, "Invalid label selector: %v", sel)
	}

	return lblSel.Matches(labels.Set(target.GetLabels())), nil
}

// isFieldMatch runs the match operation against the
// target's field path that holds a string
func isFieldMatch(term AnySelectorTerm, target *unstructured.Unstructured) (bool, error) {
	// match is true if there are no field based selectors
	if term.MatchFields == nil && term.MatchFieldExpressions == nil {
		return true, nil
	}

	if target == nil {
		return false, errors.Errorf("Field selector failed: Nil target")
	}

	sel := &metav1.LabelSelector{
		MatchLabels:      term.MatchFields,
		MatchExpressions: term.MatchFieldExpressions,
	}
	fieldSel, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return false, errors.Wrapf(err, "Invalid field selector: %v", sel)
	}

	var allKeys []string
	for kfield := range term.MatchFields {
		if kfield == "" {
			return false,
				errors.Wrapf(err, "Invalid field selector: Missing key: %v", term.MatchFields)
		}
		allKeys = append(allKeys, kfield)
	}
	for _, kexp := range term.MatchFieldExpressions {
		if kexp.Key == "" {
			return false,
				errors.Wrapf(err, "Invalid field expression selector: Missing key: %v", kexp)
		}
		allKeys = append(allKeys, kexp.Key)
	}

	// fill up given selector keys with actual values
	// from the target
	keyValues := make(map[string]string)
	for _, key := range allKeys {
		fields := strings.Split(key, ".")
		val, found, err := unstructured.NestedString(target.Object, fields...)
		if err != nil {
			return false, errors.Wrapf(err, "Field selector with key %s failed", key)
		}
		if found {
			// add key if and only if the key is found
			//
			// NOTE:
			// 	This is helpful for cases where match is being
			// made from 'Exists' or 'DoesNotExist' operator
			keyValues[key] = val
		}
	}

	return fieldSel.Matches(labels.Set(keyValues)), nil
}

// isSliceMatch runs the match operation against the
// target's field path that holds a string slice
func isSliceMatch(term AnySelectorTerm, target *unstructured.Unstructured) (bool, error) {
	// match is true if there are no match logic to be evaluated
	if len(term.MatchSlice)+len(term.MatchSliceExpressions) == 0 {
		return true, nil
	}

	if target == nil {
		return false, errors.Errorf("Slice match failed: Nil target")
	}

	selConfig := SliceSelectorConfig{
		MatchSlice:            term.MatchSlice,
		MatchSliceExpressions: term.MatchSliceExpressions,
	}
	sliceSel, err := NewSliceSelector(selConfig)
	if err != nil {
		return false, errors.Wrapf(err, "Invalid slice match: %v", selConfig)
	}

	// fill up specified selector keys with actual values
	// from the target
	targetSlice := make(map[string][]string)
	for _, kexp := range term.MatchSliceExpressions {
		if kexp.Key == "" {
			return false,
				errors.Errorf("Invalid slice match expression: Missing key: %v", kexp)
		}
		fields := strings.Split(kexp.Key, ".")

		// extract actual value(s) from target
		vals, _, err := unstructured.NestedStringSlice(target.Object, fields...)
		if err != nil {
			return false, errors.Wrapf(err, "Slice match with key %s failed", kexp.Key)
		}

		targetSlice[kexp.Key] = vals
	}

	return sliceSel.Match(targetSlice), nil
}
