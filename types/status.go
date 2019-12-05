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

package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionType is a custom datatype that
// refers to various conditions supported in this operator
type ConditionType string

const (
	// CStorClusterConfigConditionReconcileError is used to
	// indicate presence or absence of error while reconciling
	// CStorClusterConfig
	CStorClusterConfigConditionReconcileError ConditionType = "CStorClusterConfigReconcileError"

	// CStorClusterPlanConditionReconcileError is used to
	// indicate presence or absence of error while reconciling
	// CStorClusterConfigPlan
	CStorClusterPlanConditionReconcileError ConditionType = "CStorClusterPlanReconcileError"

	// CStorClusterStorageSetConditionReconcileError is used to
	// indicate presence or absence of error while reconciling
	// CStorClusterStorageSet
	CStorClusterStorageSetConditionReconcileError ConditionType = "CStorClusterStorageSetError"
)

// ConditionStatus is a custom datatype that
// refers to presence or absence of any condition
type ConditionStatus string

const (
	// ConditionIsPresent refers to presence of any condition
	ConditionIsPresent ConditionStatus = "True"

	// ConditionIsAbsent refers to absence of any condition
	ConditionIsAbsent ConditionStatus = "False"
)

// MakeCStorClusterConfigReconcileErrCond builds a new
// CStorClusterConfigConditionReconcileError condition
// suitable to be used in API status.conditions
func MakeCStorClusterConfigReconcileErrCond(err error) map[string]string {
	return map[string]string{
		"type":             string(CStorClusterConfigConditionReconcileError),
		"status":           string(ConditionIsPresent),
		"reason":           err.Error(),
		"lastObservedTime": metav1.Now().String(),
	}
}

// MakeCStorClusterPlanReconcileErrCond builds a new
// CStorClusterPlanConditionReconcileError condition
// suitable to be used in API status.conditions
func MakeCStorClusterPlanReconcileErrCond(err error) map[string]string {
	return map[string]string{
		"type":             string(CStorClusterPlanConditionReconcileError),
		"status":           string(ConditionIsPresent),
		"reason":           err.Error(),
		"lastObservedTime": metav1.Now().String(),
	}
}

// MakeCStorClusterStorageSetReconcileErrCond builds a new
// CStorClusterStorageSetConditionReconcileError condition
// suitable to be used in API status.conditions
func MakeCStorClusterStorageSetReconcileErrCond(err error) map[string]string {
	return map[string]string{
		"type":             string(CStorClusterStorageSetConditionReconcileError),
		"status":           string(ConditionIsPresent),
		"reason":           err.Error(),
		"lastObservedTime": metav1.Now().String(),
	}
}

// MakeNoCStorClusterConfigReconcileErrCond builds a new no
// CStorClusterConfigConditionReconcileError condition. This
// should be used in such a way that it voids previous occurrence of
// this error if any.
func MakeNoCStorClusterConfigReconcileErrCond() map[string]string {
	return map[string]string{
		"type":             string(CStorClusterConfigConditionReconcileError),
		"status":           string(ConditionIsAbsent),
		"lastObservedTime": metav1.Now().String(),
	}
}

// MergeNoReconcileErrorOnCStorClusterConfig sets
// CStorClusterConfigConditionReconcileError condition to false.
func MergeNoReconcileErrorOnCStorClusterConfig(obj *CStorClusterConfig) {
	noErrCond := CStorClusterConfigStatusCondition{
		Type:             CStorClusterConfigConditionReconcileError,
		Status:           ConditionIsAbsent,
		LastObservedTime: metav1.Now(),
	}
	var newConds []CStorClusterConfigStatusCondition
	for _, old := range obj.Status.Conditions {
		if old.Type == CStorClusterConfigConditionReconcileError {
			// ignore previous occurrence of ReconcileError
			continue
		}
		newConds = append(newConds, old)
	}
	newConds = append(newConds, noErrCond)
	obj.Status.Conditions = newConds
}

// MergeNoReconcileErrorOnCStorClusterPlan sets
// CStorClusterPlanConditionReconcileError condition to false.
func MergeNoReconcileErrorOnCStorClusterPlan(obj *CStorClusterPlan) {
	noErrCond := CStorClusterPlanStatusCondition{
		Type:             CStorClusterPlanConditionReconcileError,
		Status:           ConditionIsAbsent,
		LastObservedTime: metav1.Now(),
	}
	var newConds []CStorClusterPlanStatusCondition
	for _, old := range obj.Status.Conditions {
		if old.Type == CStorClusterPlanConditionReconcileError {
			// ignore previous occurrence of ReconcileError
			continue
		}
		newConds = append(newConds, old)
	}
	newConds = append(newConds, noErrCond)
	obj.Status.Conditions = newConds
}

// MergeNoReconcileErrorOnCStorClusterStorageSet sets
// CStorClusterStorageSetConditionReconcileError condition to false.
func MergeNoReconcileErrorOnCStorClusterStorageSet(obj *CStorClusterStorageSet) {
	noErrCond := CStorClusterStorageSetStatusCondition{
		Type:             CStorClusterStorageSetConditionReconcileError,
		Status:           ConditionIsAbsent,
		LastObservedTime: metav1.Now(),
	}
	var newConds []CStorClusterStorageSetStatusCondition
	for _, old := range obj.Status.Conditions {
		if old.Type == CStorClusterStorageSetConditionReconcileError {
			// ignore previous occurrence of ReconcileError
			continue
		}
		newConds = append(newConds, old)
	}
	newConds = append(newConds, noErrCond)
	obj.Status.Conditions = newConds
}

// MakeCStorClusterPlanStatusToOnline sets the given CStorClusterPlan
// status to online and returns this newly formed status object.
func MakeCStorClusterPlanStatusToOnline(obj *CStorClusterPlan) map[string]interface{} {
	MergeNoReconcileErrorOnCStorClusterPlan(obj)
	obj.Status.Phase = CStorClusterPlanStatusPhaseOnline
	return map[string]interface{}{
		"phase":      CStorClusterPlanStatusPhaseOnline,
		"conditions": obj.Status.Conditions,
	}
}

// MakeCStorClusterStorageSetStatusToOnline sets the given
// CStorClusterStorageSet status to online and
// returns this newly formed status object.
func MakeCStorClusterStorageSetStatusToOnline(obj *CStorClusterStorageSet) map[string]interface{} {
	MergeNoReconcileErrorOnCStorClusterStorageSet(obj)
	obj.Status.Phase = CStorClusterStorageSetStatusPhaseOnline
	return map[string]interface{}{
		"phase":      CStorClusterStorageSetStatusPhaseOnline,
		"conditions": obj.Status.Conditions,
	}
}
