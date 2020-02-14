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

import "github.com/pkg/errors"

// RaidGroupConfig contains raid type and device(s)
// count for a raid group
type RaidGroupConfig struct {
	// Type is the raid group type
	// Supported values are : stripe, mirror, raidz and raidz2
	Type PoolRAIDType `json:"type"`
	// GroupDeviceCount contains device count in a raid group
	// -- for stripe DeviceCount = 1
	// -- for mirror DeviceCount = 2
	// -- for raidz DeviceCount = (2^n + 1) default is (2 + 1)
	// -- for raidz2 DeviceCount = (2^n + 2) default is (4 + 2)
	GroupDeviceCount int64 `json:"groupDeviceCount"`
}

// GetDefaultRaidGroupConfig returns an object of RaidGroupConfig
// with default configuration. Default raid group config is
// type: mirror groupDeviceCount: 2
func GetDefaultRaidGroupConfig() *RaidGroupConfig {
	return &RaidGroupConfig{
		Type:             PoolRAIDTypeDefault,
		GroupDeviceCount: RAIDTypeToDefaultMinDiskCount[PoolRAIDTypeMirror],
	}
}

// PopulateDefaultGroupDeviceCount populate default device count for
// a given raid group. If device count is not set then only it sets
// device count for a raid group else it will skip.
func (rgc *RaidGroupConfig) PopulateDefaultGroupDeviceCount() error {
	if rgc.GroupDeviceCount != 0 {
		return nil
	}
	dc, ok := RAIDTypeToDefaultMinDiskCount[rgc.Type]
	if !ok {
		return errors.Errorf("Invalid RAID type %q: Supports %q, %q, %q or %q.",
			rgc.Type, PoolRAIDTypeStripe, PoolRAIDTypeMirror, PoolRAIDTypeRAIDZ, PoolRAIDTypeRAIDZ2)
	}
	rgc.GroupDeviceCount = dc
	return nil
}

// Validate validates RaidGroupConfig
func (rgc *RaidGroupConfig) Validate() error {
	// If we got any -ve number or 0 then it an invalid device count.
	if rgc.GroupDeviceCount <= 0 {
		return errors.Errorf("Invalid device count %d for RAID type %q.",
			rgc.GroupDeviceCount, rgc.Type)
	}
	switch rgc.Type {
	// For mirror pool device count in one vdev is 2
	case PoolRAIDTypeMirror:
		{
			if rgc.GroupDeviceCount != 2 {
				return errors.Errorf("Invalid device count %d for RAID type %q: Want 2.",
					rgc.GroupDeviceCount, rgc.Type)
			}
		}
	// For stripe pool device count in one vdev is n. Where n > 0
	case PoolRAIDTypeStripe:
		{
			return nil
		}
	// For raidz raid group device count in one vdev is (2^n +1)
	case PoolRAIDTypeRAIDZ:
		{
			count := rgc.GroupDeviceCount - 1
			for count != 1 {
				r := count % 2
				if r != 0 {
					return errors.Errorf("Invalid device count %d for RAID type %q: Want 2^n + 1.",
						rgc.GroupDeviceCount, rgc.Type)
				}
				count = count / 2
			}
		}
	// For raidz2 raid group device count in one vdev is (2^n +1)
	case PoolRAIDTypeRAIDZ2:
		{
			count := rgc.GroupDeviceCount - 2
			for count != 1 {
				r := count % 2
				if r != 0 {
					return errors.Errorf("Invalid device count %d for RAID type %q: Want 2^n + 2.",
						rgc.GroupDeviceCount, rgc.Type)
				}
				count = count / 2
			}
		}
	default:
		{
			return errors.Errorf("Invalid RAID type %q: Supports %q, %q, %q or %q.",
				rgc.Type, PoolRAIDTypeStripe, PoolRAIDTypeMirror, PoolRAIDTypeRAIDZ, PoolRAIDTypeRAIDZ2)
		}
	}
	return nil
}
