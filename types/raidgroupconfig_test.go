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
	"testing"
)

func TestGetDefaultRaidGroupConfig(t *testing.T) {
	var tests = map[string]struct {
		expectedType             PoolRAIDType
		expectedGroupDeviceCount int64
	}{
		"chheck default raid group config": {
			expectedType:             PoolRAIDTypeDefault,
			expectedGroupDeviceCount: RAIDTypeToDefaultMinDiskCount[PoolRAIDTypeMirror],
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := GetDefaultRaidGroupConfig()
			if got.Type != mock.expectedType {
				t.Fatalf("expected type %s but got %s",
					mock.expectedType, got.Type)
			}
			if got.GroupDeviceCount != mock.expectedGroupDeviceCount {
				t.Fatalf("expected device count %d but got %d",
					mock.expectedGroupDeviceCount, got.GroupDeviceCount)
			}
		})
	}
}

func TestPopulateDefaultGroupDeviceCount(t *testing.T) {
	var tests = map[string]struct {
		src                      RaidGroupConfig
		expectedGroupDeviceCount int64
		isErr                    bool
	}{
		"stripe pool and device count not set": {
			src: RaidGroupConfig{
				Type: PoolRAIDTypeStripe,
			},
			expectedGroupDeviceCount: RAIDTypeToDefaultMinDiskCount[PoolRAIDTypeStripe],
			isErr:                    false,
		},
		"stripe pool and device count set": {
			src: RaidGroupConfig{
				Type:             PoolRAIDTypeStripe,
				GroupDeviceCount: 7,
			},
			expectedGroupDeviceCount: 7,
			isErr:                    false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			got := GetDefaultRaidGroupConfig()
			if got.GroupDeviceCount != mock.expectedGroupDeviceCount {
				t.Fatalf("expected device count %d but got %d",
					mock.expectedGroupDeviceCount, got.GroupDeviceCount)
			}
		})
	}
}
