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

package blockdevice

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
)

// ListHelper exposes utility methods for a list of
// BlockDevice unstructured instances
type ListHelper struct {
	BlockDevices []*unstructured.Unstructured

	err error
}

// NewListHelper returns a new instance of ListHelper
func NewListHelper(devices []*unstructured.Unstructured) *ListHelper {
	var err error
	var errmsgs []string
	for idx, device := range devices {
		if device == nil || device.Object == nil {
			msg := fmt.Sprintf(
				"Can't init device list helper: Nil device at %d", idx,
			)
			errmsgs = append(errmsgs, msg)
		} else if device.GetKind() != string(types.KindBlockDevice) {
			msg := fmt.Sprintf(
				"Can't init device list helper: Invalid kind: Want %q got %q at %d",
				types.KindBlockDevice, device.GetKind(), idx,
			)
			errmsgs = append(errmsgs, msg)
		}
	}
	if len(errmsgs) != 0 {
		err = errors.Errorf(
			"%d errors found: [%s]",
			len(errmsgs), strings.Join(errmsgs, ", "),
		)
	}
	if err != nil {
		return &ListHelper{err: err}
	}
	return &ListHelper{
		BlockDevices: devices,
	}
}

// GroupDeviceNamesByHostName returns a list of devicenames
// grouped by their hostname
func (l *ListHelper) GroupDeviceNamesByHostName() (map[string][]string, error) {
	if l.err != nil {
		return nil, l.err
	}
	hostNameToDeviceNames := map[string][]string{}
	for _, device := range l.BlockDevices {
		h := NewHelper(device)
		host, err := h.GetHostName()
		if err != nil {
			return nil, err
		}
		hostNameToDeviceNames[host] =
			append(hostNameToDeviceNames[host], device.GetName())
	}
	return hostNameToDeviceNames, nil
}
