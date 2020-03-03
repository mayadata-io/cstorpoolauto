/*
Copyright 2020 The MayaData Authors.

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
		src                      *RaidGroupConfig
		expectedGroupDeviceCount int64
		isErr                    bool
	}{
		"stripe pool and device count not set": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeStripe,
			},
			expectedGroupDeviceCount: RAIDTypeToDefaultMinDiskCount[PoolRAIDTypeStripe],
			isErr:                    false,
		},
		"stripe pool and device count set": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeStripe,
				GroupDeviceCount: 7,
			},
			expectedGroupDeviceCount: 7,
			isErr:                    false,
		},
		"mirror pool and device count not set": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeMirror,
			},
			expectedGroupDeviceCount: RAIDTypeToDefaultMinDiskCount[PoolRAIDTypeMirror],
			isErr:                    false,
		},
		"mirror pool and device count set": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeMirror,
				GroupDeviceCount: 7,
			},
			expectedGroupDeviceCount: 7,
			isErr:                    false,
		},
		"raidz pool and device count not set": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeRAIDZ,
			},
			expectedGroupDeviceCount: RAIDTypeToDefaultMinDiskCount[PoolRAIDTypeRAIDZ],
			isErr:                    false,
		},
		"raidz pool and device count set": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ,
				GroupDeviceCount: 7,
			},
			expectedGroupDeviceCount: 7,
			isErr:                    false,
		},
		"raidz2 pool and device count not set": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeRAIDZ2,
			},
			expectedGroupDeviceCount: RAIDTypeToDefaultMinDiskCount[PoolRAIDTypeRAIDZ2],
			isErr:                    false,
		},
		"raidz2 pool and device count set": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ2,
				GroupDeviceCount: 7,
			},
			expectedGroupDeviceCount: 7,
			isErr:                    false,
		},
		"invalid raid type and device count is not set": {
			src: &RaidGroupConfig{
				Type: PoolRAIDType(""),
			},
			isErr: true,
		},
		"invalid raid type and device count is set": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDType(""),
				GroupDeviceCount: 1,
			},
			expectedGroupDeviceCount: 1,
			isErr:                    false,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			err := mock.src.PopulateDefaultGroupDeviceCountIfNotPresent()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
			if !mock.isErr && mock.src.GroupDeviceCount != mock.expectedGroupDeviceCount {
				t.Fatalf("expected device count %d but got %d",
					mock.expectedGroupDeviceCount, mock.src.GroupDeviceCount)
			}
		})
	}
}

func TestGetDataDeviceCount(t *testing.T) {
	var tests = map[string]struct {
		src             *RaidGroupConfig
		devicecount     []int64
		datadevicecount []int64
	}{
		"stripe pool": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeStripe,
			},
			devicecount:     []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			datadevicecount: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		"mirror pool": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeMirror,
			},
			devicecount:     []int64{2, 4, 6, 8, 10, 12, 14, 16, 18},
			datadevicecount: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		"raidz pool": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeRAIDZ,
			},
			devicecount:     []int64{3, 5, 9, 17, 33, 65},
			datadevicecount: []int64{2, 4, 8, 16, 32, 64},
		},
		"raidz2 pool": {
			src: &RaidGroupConfig{
				Type: PoolRAIDTypeRAIDZ2,
			},
			devicecount:     []int64{4, 6, 10, 18, 34, 66},
			datadevicecount: []int64{2, 4, 8, 16, 32, 64},
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			for i, c := range mock.devicecount {
				if i < len(mock.datadevicecount) {
					rc := mock.src
					rc.GroupDeviceCount = c
					if rc.GetDataDeviceCount() != mock.datadevicecount[i] {
						t.Fatalf("expected data device count %d but got %d",
							mock.datadevicecount[i], rc.GetDataDeviceCount())
					}
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	var tests = map[string]struct {
		src   *RaidGroupConfig
		isErr bool
	}{
		"stripe pool and device count is -ve": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeStripe,
				GroupDeviceCount: -3,
			},
			isErr: true,
		},
		"mirror pool and device count is -ve": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeMirror,
				GroupDeviceCount: -3,
			},
			isErr: true,
		},
		"raidz pool and device count is -ve": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ,
				GroupDeviceCount: -3,
			},
			isErr: true,
		},
		"raidz2 pool and device count is -ve": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ2,
				GroupDeviceCount: -3,
			},
			isErr: true,
		},
		"stripe pool and device count is less than min count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeStripe,
				GroupDeviceCount: 0,
			},
			isErr: true,
		},
		"stripe pool and valid device count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeStripe,
				GroupDeviceCount: 2,
			},
			isErr: false,
		},
		"mirror pool and invalid device count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeMirror,
				GroupDeviceCount: 3,
			},
			isErr: true,
		},
		"mirror pool and valid device count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeMirror,
				GroupDeviceCount: 2,
			},
			isErr: false,
		},
		"mirror pool and device count is less than min count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeMirror,
				GroupDeviceCount: 0,
			},
			isErr: true,
		},
		"raidz pool and invalid device count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ,
				GroupDeviceCount: 4,
			},
			isErr: true,
		},
		"raidz pool and device count is less than min count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ,
				GroupDeviceCount: 1,
			},
			isErr: true,
		},
		"raidz pool and valid device count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ,
				GroupDeviceCount: 3,
			},
			isErr: false,
		},
		"raidz2 pool and invalid device count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ2,
				GroupDeviceCount: 5,
			},
			isErr: true,
		},
		"raidz2 pool and valid device count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ2,
				GroupDeviceCount: 6,
			},
			isErr: false,
		},
		"raidz2 pool and device count is less than min count": {
			src: &RaidGroupConfig{
				Type:             PoolRAIDTypeRAIDZ2,
				GroupDeviceCount: 2,
			},
			isErr: true,
		},
		"invalid raid type and device count is not set": {
			src: &RaidGroupConfig{
				Type: PoolRAIDType(""),
			},
			isErr: true,
		},
	}
	for name, mock := range tests {
		name := name
		mock := mock
		t.Run(name, func(t *testing.T) {
			err := mock.src.Validate()
			if mock.isErr && err == nil {
				t.Fatalf("Expected error got none")
			}
			if !mock.isErr && err != nil {
				t.Fatalf("Expected no error got [%+v]", err)
			}
		})
	}
}
