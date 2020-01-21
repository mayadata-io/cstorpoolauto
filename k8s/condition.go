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

// ConditionOperator defines the kind of operation to be
// done against the condition criterias. One such example
// is OR-ing of criterias.
type ConditionOperator string

const (
	// ConditionOR implies condition criterias will be OR-ed
	ConditionOR ConditionOperator = "or"

	// ConditionAND implies condition criterias will be AND-ed
	//
	// NOTE: This is the default condition operator
	ConditionAND ConditionOperator = "and"
)

// Condition evaluates one or more criterias against an
// unstructured instance and reports if the target object
// passes the evaluation or not.
type Condition struct {
	// obj to be evaluated
	obj *unstructured.Unstructured

	// error if any during evaluation
	err error

	// if true indicates evaluation was successful
	success *bool

	// failureReasons will hold all the evaluation failureReasons
	failureReasons []error

	// annotations derived from above obj
	annotations map[string]string

	// labels derived from above obj
	labels map[string]string

	// operator to be used amongst all the criterias
	operator ConditionOperator
}

// init initializes the Condition instance with default
// values. This also enables a condition to be re-used
// against multiple objects without side effects.
func (e *Condition) init(obj *unstructured.Unstructured) *Condition {
	if obj == nil || obj.Object == nil {
		e.err = errors.Errorf("Nil object under evaluation")
		return e
	}
	// We do not use GetAnnotations() since it swallows error if any
	anns, _, err := unstructured.NestedStringMap(obj.Object, "metadata", "annotations")
	if err != nil {
		e.err = err
		return e
	}
	// We do not use GetLabels() since it swallows error if any
	lbls, _, err := unstructured.NestedStringMap(obj.Object, "metadata", "labels")
	if err != nil {
		e.err = err
		return e
	}
	// set necessary fields that gets evaluated or used during
	// evaluation at later point of time
	e.obj = obj
	e.annotations = anns
	e.labels = lbls

	// set the following to their initial states
	if e.operator == "" {
		e.operator = ConditionAND
	}
	e.failureReasons = nil
	e.success = nil
	e.err = nil
	return e
}

// NewCondition returns a new instance of Condition
func NewCondition(obj *unstructured.Unstructured) *Condition {
	e := &Condition{}
	return e.init(obj)
}

// NewORCondition returns a new instance of Condition
// that uses OR operator while evaluating the condition
// criterias
func NewORCondition(obj *unstructured.Unstructured) *Condition {
	e := &Condition{
		operator: ConditionOR,
	}
	return e.init(obj)
}

func (e *Condition) skip() bool {
	if e.err != nil {
		// always skip in case of any runtime error
		return true
	}
	if e.success == nil {
		// return false since no criteria has been evaluated
		return false
	}
	if e.operator == ConditionOR && *e.success {
		// OR operator implies any one success is overall success
		return true
	}
	if e.operator == ConditionOR {
		// return false since we don't want to fail
		// just because previous criteria check failed
		return false
	}
	// this follows the default AND operator i.e. if the previous
	// check criteria failed then skip the entire evaluation
	return !*e.success
}

// IsUID is a criteria that evaluates if provided UID
// matches the target object
func (e *Condition) IsUID(want string) *Condition {
	if e.skip() {
		return e
	}
	if want == "" {
		e.err = errors.Errorf("Invalid IsUID: Missing uid")
		return e
	}
	got := string(e.obj.GetUID())
	ok := got == want
	if !ok {
		e.failureReasons = append(
			e.failureReasons, errors.Errorf("IsUID failed: Want %q got %q", want, got),
		)
	}
	e.success = pointer.BoolPtr(ok)
	return e
}

// IsKind is a criteria that evaluates if provided kind
// matches the target object
func (e *Condition) IsKind(want string) *Condition {
	if e.skip() {
		return e
	}
	if want == "" {
		e.err = errors.Errorf("Invalid IsKind: Missing kind")
		return e
	}
	got := e.obj.GetKind()
	ok := got == want
	if !ok {
		e.failureReasons = append(
			e.failureReasons, errors.Errorf("IsKind failed: Want %q got %q", want, got),
		)
	}
	e.success = pointer.BoolPtr(ok)
	return e
}

// IsAPIVersion is a criteria that evaluates if provided
// api version matches the target object
func (e *Condition) IsAPIVersion(want string) *Condition {
	if e.skip() {
		return e
	}
	if want == "" {
		e.err = errors.Errorf("Invalid IsAPIVersion: Missing apiversion")
		return e
	}
	got := string(e.obj.GetAPIVersion())
	ok := got == want
	if !ok {
		e.failureReasons = append(
			e.failureReasons, errors.Errorf("IsAPIVersion failed: Want %q got %q", want, got),
		)
	}
	e.success = pointer.BoolPtr(ok)
	return e
}

// hasPair evaluates if provided pair is available
// in the provided list of pairs
func (e *Condition) hasPair(pairs map[string]string, key, value string) bool {
	if e.skip() {
		return false
	}
	if key == "" {
		e.err = errors.Errorf("Invalid hasPair: Missing key")
		return false
	}
	if len(pairs) == 0 {
		e.success = pointer.BoolPtr(false)
		e.failureReasons = append(
			e.failureReasons, errors.Errorf("hasPair failed: Nil pairs"),
		)
		return false
	}
	got, _ := GetAnnotationForKey(pairs, key)
	return got == value
}

// HasAnn is a criteria that evaluates if provided annotation
// is present in the target object
func (e *Condition) HasAnn(key, value string) *Condition {
	found := e.hasPair(e.annotations, key, value)
	// skip is called after hasPair invocation since latter may
	// result in setting skip to true
	if e.skip() {
		return e
	}
	if !found {
		e.failureReasons = append(
			e.failureReasons,
			errors.Errorf(
				"HasAnn failed: Can't find ann key %q with value %q",
				key, value,
			),
		)
	}
	e.success = pointer.BoolPtr(found)
	return e
}

// HasAnns is a criteria that evaluates if provided annotations
// are present in the target object
func (e *Condition) HasAnns(want map[string]string) *Condition {
	if e.skip() {
		return e
	}
	if len(want) == 0 {
		e.err = errors.Errorf("Invalid HasAnns: No annotations were provided")
		return e
	}
	if len(e.annotations) == 0 {
		e.failureReasons = append(
			e.failureReasons,
			errors.Errorf(
				"HasAnns failed: Want anns = %d: Got anns = 0", len(want),
			),
		)
		e.success = pointer.BoolPtr(false)
		return e
	}
	var found bool
	for key, val := range want {
		found = e.hasPair(e.annotations, key, val)
		if !found {
			e.failureReasons = append(
				e.failureReasons,
				errors.Errorf(
					"HasAnns failed: Can't find ann key %q with value %q",
					key, val,
				),
			)
			break
		}
	}
	e.success = pointer.BoolPtr(found)
	return e
}

// HasLabel is a criteria that evaluates if provided label
// is available in the target object
func (e *Condition) HasLabel(key, value string) *Condition {
	found := e.hasPair(e.labels, key, value)
	// skip is called after hasPair invocation since latter may
	// result in setting skip to true
	if e.skip() {
		return e
	}
	if !found {
		e.failureReasons = append(
			e.failureReasons,
			errors.Errorf(
				"HasLabel failed: Can't find label key %q with value %q",
				key, value,
			),
		)
	}
	e.success = pointer.BoolPtr(found)
	return e
}

// HasLabels is a criteria that evaluates if provided labels
// are present in the target object
func (e *Condition) HasLabels(want map[string]string) *Condition {
	if e.skip() {
		return e
	}
	if len(want) == 0 {
		e.err = errors.Errorf("Invalid HasLabels: No labels were provided")
		return e
	}
	if len(e.labels) == 0 {
		e.failureReasons = append(
			e.failureReasons,
			errors.Errorf(
				"HasLabels failed: Want lbls = %d: Got lbls = 0", len(want),
			),
		)
		e.success = pointer.BoolPtr(false)
		return e
	}
	var found bool
	for key, val := range want {
		found = e.hasPair(e.labels, key, val)
		if !found {
			e.failureReasons = append(
				e.failureReasons,
				errors.Errorf(
					"HasLabels failed: Can't find lbl key %q with value %q",
					key, val,
				),
			)
			break
		}
	}
	e.success = pointer.BoolPtr(found)
	return e
}

// Check returns the final evaluation
func (e *Condition) Check() (bool, error) {
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
func (e *Condition) MustCheck() bool {
	result, err := e.Check()
	if err != nil {
		panic(e.err)
	}
	return result
}

// FailureReasons returns all the failures during evaluation
// if any
func (e *Condition) FailureReasons() []error {
	return e.failureReasons
}

// IsFail returns true if there are any evaluation failures
func (e *Condition) IsFail() bool {
	return len(e.failureReasons) > 0
}

// LazyCondition helps executing one or more criterias
// lazily
type LazyCondition struct {
	*Condition

	// criterias i.e. functions that get executed lazily
	criteriaFns []func()
}

// NewLazyCondition returns a new instance of LazyCondition
func NewLazyCondition() *LazyCondition {
	return &LazyCondition{
		Condition: &Condition{},
	}
}

// NewLazyORCondition returns a new instance of LazyCondition
func NewLazyORCondition() *LazyCondition {
	return &LazyCondition{
		Condition: &Condition{
			operator: ConditionOR,
		},
	}
}

// IsUID is a lazy evaluation of IsUID criteria
func (e *LazyCondition) IsUID(want string) *LazyCondition {
	e.criteriaFns = append(e.criteriaFns, func() {
		e.Condition.IsUID(want)
	})
	return e
}

// IsKind is a lazy evaluation of IsKind criteria
func (e *LazyCondition) IsKind(want string) *LazyCondition {
	e.criteriaFns = append(e.criteriaFns, func() {
		e.Condition.IsKind(want)
	})
	return e
}

// IsAPIVersion is a lazy evaluation of IsAPIVersion criteria
func (e *LazyCondition) IsAPIVersion(want string) *LazyCondition {
	e.criteriaFns = append(e.criteriaFns, func() {
		e.Condition.IsAPIVersion(want)
	})
	return e
}

// HasAnn is a lazy evaluation of HasAnn criteria
func (e *LazyCondition) HasAnn(key, value string) *LazyCondition {
	e.criteriaFns = append(e.criteriaFns, func() {
		e.Condition.HasAnn(key, value)
	})
	return e
}

// HasAnns is a lazy evaluation of HasAnns criteria
func (e *LazyCondition) HasAnns(want map[string]string) *LazyCondition {
	e.criteriaFns = append(e.criteriaFns, func() {
		e.Condition.HasAnns(want)
	})
	return e
}

// HasLabel is a lazy evaluation of HasLabel criteria
func (e *LazyCondition) HasLabel(key, value string) *LazyCondition {
	e.criteriaFns = append(e.criteriaFns, func() {
		e.Condition.HasLabel(key, value)
	})
	return e
}

// HasLabels is a lazy evaluation of HasLabels criteria
func (e *LazyCondition) HasLabels(want map[string]string) *LazyCondition {
	e.criteriaFns = append(e.criteriaFns, func() {
		e.Condition.HasLabels(want)
	})
	return e
}

// Check evaluates the provided instance with criterias
// that must be set earlier
func (e *LazyCondition) Check(obj *unstructured.Unstructured) (bool, error) {
	if obj == nil || obj.Object == nil {
		return false,
			errors.Errorf("Invalid invocation: Nil object")
	}
	if len(e.criteriaFns) == 0 {
		return false,
			errors.Errorf("Invalid invocation: No condition criterias")
	}
	// initialize with provided object before running the evaluations
	e.init(obj)
	// run all criterias
	for _, fn := range e.criteriaFns {
		if fn == nil {
			return false, errors.Errorf("Invalid invocation: Nil criteria function")
		}
		// actual condition criteria is invoked
		fn()
		if e.err != nil {
			return false, e.err
		}
		if e.success != nil && *e.success && e.operator == ConditionOR {
			// fast return in case of OR operator & if the just
			// evaluated criteria passed
			return true, nil
		}
	}
	if e.success == nil {
		return false, nil
	}
	return *e.success, nil
}

// MustCheck evaluates the provided instance with criterias
// that must be set earlier
func (e *LazyCondition) MustCheck(obj *unstructured.Unstructured) bool {
	result, err := e.Check(obj)
	if err != nil {
		panic(err)
	}
	return result
}
