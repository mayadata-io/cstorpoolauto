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
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
)

// Helper exposes utility methods w.r.t BlockDevice unstructured
// instance
type Helper struct {
	BlockDevice *unstructured.Unstructured

	err error
}

// NewHelper returns a new instance of Helper
func NewHelper(device *unstructured.Unstructured) *Helper {
	var err error
	if device == nil || device.Object == nil {
		err = errors.Errorf(
			"Can't init device helper: Nil object",
		)
	} else if device.GetKind() != string(types.KindBlockDevice) {
		err = errors.Errorf(
			"Can't init device helper: Invalid kind: Want %q got %q",
			types.KindBlockDevice, device.GetKind(),
		)
	}
	if err != nil {
		return &Helper{
			err: err,
		}
	}
	return &Helper{
		BlockDevice: device,
	}
}

// GetHostName returns the hostname associated with the blockdevice
func (h *Helper) GetHostName() (string, error) {
	if h.err != nil {
		return "", h.err
	}
	return unstruct.GetStringOrError(
		h.BlockDevice, "metadata", "labels", "kubernetes.io/hostname",
	)
}
