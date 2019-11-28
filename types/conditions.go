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

import "time"

// CStorClusterConfigConditionType is a custom datatype that
// refers to various conditions supported in this operator
type CStorClusterConfigConditionType string

const (
	// CStorClusterConfigConditionErrorSettingDefault is used to
	// indicate presence or absence of error while setting
	// defaults against CStorClusterConfig
	CStorClusterConfigConditionErrorSettingDefault CStorClusterConfigConditionType = "ErrorSettingDefault"
)

// CStorClusterConfigConditionStatus is a custom datatype that
// refers to presence or absence of any condition
type CStorClusterConfigConditionStatus string

const (
	// CStorClusterConfigConditionIsPresent refers to presence
	// of any CStorClusterConfig condition
	CStorClusterConfigConditionIsPresent CStorClusterConfigConditionStatus = "True"

	// CStorClusterConfigConditionIsAbsent refers to absence
	// of any CStorClusterConfig condition
	CStorClusterConfigConditionIsAbsent CStorClusterConfigConditionStatus = "False"
)

// MakeErrorSettingDefaultCondition builds a new
// CStorClusterConfigConditionErrorSettingDefault condition
// suitable to be used in API status.conditions
//
// NOTE:
// 	SetDefaultError points to cases when there is some error
// while setting defaults against CStorClusterConfig
func MakeErrorSettingDefaultCondition(err error) map[string]string {
	return map[string]string{
		"type":             string(CStorClusterConfigConditionErrorSettingDefault),
		"status":           string(CStorClusterConfigConditionIsPresent),
		"reason":           err.Error(),
		"lastObservedTime": time.Now().String(),
	}
}

// MakeNoErrorSettingDefaultCondition builds a new no
// CStorClusterConfigConditionErrorSettingDefault condition. This
// should be used in such a way that it voids previous errors
// if any during setting of defaults against CStorClusterConfig.
func MakeNoErrorSettingDefaultCondition() map[string]string {
	return map[string]string{
		"type":             string(CStorClusterConfigConditionErrorSettingDefault),
		"status":           string(CStorClusterConfigConditionIsAbsent),
		"lastObservedTime": time.Now().String(),
	}
}
