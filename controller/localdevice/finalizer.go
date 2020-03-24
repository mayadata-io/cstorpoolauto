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

package localdevice

import (
	"fmt"

	"github.com/golang/glog"
	"openebs.io/metac/controller/generic"

	ccc "mayadata.io/cstorpoolauto/util/cstorclusterconfig"
	metacutil "mayadata.io/cstorpoolauto/util/metac"
)

type finalizer struct {
	request  *generic.SyncHookRequest
	response *generic.SyncHookResponse

	isDiskLocal bool
	fatal       error
	err         error
	warns       []string
}

func (f *finalizer) validateArgs() {
	// validation failure of request &/ response is a fatal error
	f.fatal = metacutil.ValidateGenericControllerArgs(f.request, f.response)
}

func (f *finalizer) skipIfNotLocalDisk() {
	f.isDiskLocal, f.err =
		ccc.NewHelper(f.request.Watch).IsLocalBlockDiskConfig()
	if f.err != nil {
		return
	}
	if !f.isDiskLocal {
		var msg = fmt.Sprintf(
			"Will skip LocalDevice finalize: DiskConfig is not local: Watch %q - %q / %q",
			f.request.Watch.GetKind(),
			f.request.Watch.GetNamespace(),
			f.request.Watch.GetName(),
		)
		glog.V(3).Infof(msg)
		// we want to skip reconciling this since this CstorClusterConfig
		// is not meant for local devices
		f.response.SkipReconcile = true
	}
}

func (f *finalizer) logFinalizeStart() {
	glog.V(3).Infof(
		"Started LocalDevice finalize: Watch %q - %q / %q",
		f.request.Watch.GetKind(),
		f.request.Watch.GetNamespace(),
		f.request.Watch.GetName(),
	)
}

func (f *finalizer) markFinalizedIfNoAttachments() {
	// Finalize is completed if there are no attachments in request
	//
	// NOTE:
	// 	It is expected to have CStorPoolCluster as attachments
	// during the process of finalizing
	if f.request.Attachments == nil || f.request.Attachments.IsEmpty() {
		var msg = fmt.Sprintf(
			"Finalize LocalDevice completed: Watch %q - %q / %q",
			f.request.Watch.GetKind(),
			f.request.Watch.GetNamespace(),
			f.request.Watch.GetName(),
		)
		glog.V(3).Infof(msg)
		// setting finalized to true will indicate metac to remove
		// its annotations from the watch i.e. CStorClusterConfig
		f.response.Finalized = true
	}
}

func (f *finalizer) logFinalizeFinish() {
	glog.V(2).Infof(
		"Completed LocalDevice finalize: Watch %q - %q / %q: %s",
		f.request.Watch.GetKind(),
		f.request.Watch.GetNamespace(),
		f.request.Watch.GetName(),
		metacutil.GetDetailsFromResponse(f.response),
	)
}

// handleError logs the error if any
func (f *finalizer) handleError() {
	if f.err == nil {
		// nothing to do if there was no error
		return
	}
	// log this error with context
	glog.Errorf(
		"Failed to finalize LocalDevice: Watch %q - %q / %q: %+v",
		f.request.Watch.GetKind(),
		f.request.Watch.GetNamespace(),
		f.request.Watch.GetName(),
		f.err,
	)
	// stop further reconciliation at metac since there was an error
	f.response.SkipReconcile = true
}

func (f *finalizer) finalize() error {
	fns := []func(){
		f.validateArgs,
		f.skipIfNotLocalDisk,
		f.logFinalizeStart,
		f.markFinalizedIfNoAttachments,
		f.logFinalizeFinish,
	}
	for _, fn := range fns {
		fn()
		// post operation checks
		if f.fatal != nil {
			// this panics
			return f.fatal
		}
		if f.err != nil {
			// this logs the error thus avoiding panic in the
			// controller
			f.handleError()
		}
		if f.response.SkipReconcile {
			return nil
		}
	}
	return nil
}

// Finalize implements the idempotent logic to finalize
// CStorClusterConfig. This gets triggered only when
// CStorClusterConfig is being deleted.
//
// NOTE:
// 	Finalize hook automatically sets a finalizer against the watch.
// This finalizer us removed when hookresponse's Finalized field
// is set to true.
//
// NOTE:
// 	SyncHookRequest is the payload received as part of finalize
// request. Similarly, SyncHookResponse is the payload sent as a
// response as part of finalize request.
//
// NOTE:
//	SyncHookRequest uses CStorClusterConfig as the watched resource.
// SyncHookResponse uses CStorPoolCluster to form the desired state
// w.r.t the watched resource. We don't return CStorPoolCluster
// in the response since that would imply deleting the same in the
// cluster.
//
// NOTE:
//	Returning error will panic this process. We would rather want this
// controller to run continuously. Hence, the errors are handled.
func Finalize(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	f := &finalizer{
		request:  request,
		response: response,
	}
	return f.finalize()
}
