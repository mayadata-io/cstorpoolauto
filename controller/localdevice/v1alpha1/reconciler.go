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
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metac "openebs.io/metac/apis/metacontroller/v1alpha1"
	"openebs.io/metac/controller/generic"

	bd "mayadata.io/cstorpoolauto/common/blockdevice"
	ccc "mayadata.io/cstorpoolauto/common/cstorclusterconfig"
	cspc "mayadata.io/cstorpoolauto/common/cstorpoolcluster/v1alpha1"
	metaccommon "mayadata.io/cstorpoolauto/common/metac"
	"mayadata.io/cstorpoolauto/types"
	"mayadata.io/cstorpoolauto/unstruct"
)

type syncer struct {
	request  *generic.SyncHookRequest
	response *generic.SyncHookResponse

	blockDevices     []*unstructured.Unstructured
	cstorPoolCluster *unstructured.Unstructured

	reconcileResponse ReconcileResponse
	isDiskLocal       bool
	fatal             error
	err               error
	warns             []string
}

func (s *syncer) validateArgs() {
	// validation failure of request &/ response is a fatal error
	s.fatal = metaccommon.ValidateGenericControllerArgs(s.request, s.response)
}

func (s *syncer) skipIfNotLocalDisk() {
	s.isDiskLocal, s.err =
		ccc.NewHelper(s.request.Watch).IsLocalBlockDiskConfig()
	if s.err != nil {
		return
	}
	if !s.isDiskLocal {
		var msg = fmt.Sprintf(
			"Will skip LocalDevice sync: DiskConfig is not local: Watch %q - %q / %q",
			s.request.Watch.GetKind(),
			s.request.Watch.GetNamespace(),
			s.request.Watch.GetName(),
		)
		glog.V(3).Infof(msg)
		s.response.SkipReconcile = true
	}
}

func (s *syncer) skipIfEmptyAttachments() {
	// Nothing needs to be done if there are no attachments in request
	//
	// NOTE:
	// 	It is expected to have at-least BlockDevices as attachments
	if s.request.Attachments == nil || s.request.Attachments.IsEmpty() {
		var msg = fmt.Sprintf(
			"Will skip LocalDevice sync: Nil attachments: Watch %q - %q / %q",
			s.request.Watch.GetKind(),
			s.request.Watch.GetNamespace(),
			s.request.Watch.GetName(),
		)
		glog.V(3).Infof(msg)
		s.response.SkipReconcile = true
	}
}

func (s *syncer) logSyncStart() {
	glog.V(3).Infof(
		"Started LocalDevice sync: Watch %q - %q / %q",
		s.request.Watch.GetKind(),
		s.request.Watch.GetNamespace(),
		s.request.Watch.GetName(),
	)
}

func (s *syncer) registerAttachments() {
	// TODO (@amitkumardas):
	// Make use of unstruct list selector
	// Introduce callback feature to grab blockdevices
	// & cspc separately
	for _, attachment := range s.request.Attachments.List() {
		// we are interested for BlockDevices attachments only
		if attachment.GetKind() == string(types.KindBlockDevice) {
			s.blockDevices = append(s.blockDevices, attachment)
		}
		if attachment.GetKind() == string(types.KindCStorPoolCluster) {
			uid, _ := unstruct.GetValueForKey(
				attachment.GetAnnotations(), types.AnnKeyCStorClusterConfigUID,
			)
			if string(s.request.Watch.GetUID()) == uid {
				s.cstorPoolCluster = attachment
				// don't add cspc to response now
				//
				// NOTE:
				// 	cspc is added to reponse after completing reconciliation
				continue
			}
		}
		s.response.Attachments = append(s.response.Attachments, attachment)
	}
}

func (s *syncer) reconcile() {
	// reconciler performs reconciliation of CStorClusterConfig
	reconciler := &Reconciler{
		ObservedCStorClusterConfig: s.request.Watch,
		ObservedBlockDevices:       s.blockDevices,
		ObservedCStorPoolCluster:   s.cstorPoolCluster,
	}
	s.reconcileResponse, s.err = reconciler.Reconcile()
	if s.err != nil {
		return
	}
	if s.reconcileResponse.SkipReconcile {
		// skip reconciliation at metac
		s.response.SkipReconcile = true
		glog.V(3).Infof(
			"Will skip LocalDevice sync: Reason %s: Watch %q - %q / %q",
			s.reconcileResponse.SkipReason,
			s.request.Watch.GetKind(),
			s.request.Watch.GetNamespace(),
			s.request.Watch.GetName(),
		)
		return
	}
	// add desired CStorPoolCluster to response
	s.response.Attachments = append(
		s.response.Attachments, s.reconcileResponse.CStorPoolCluster,
	)
}

func (s *syncer) logSyncFinish() {
	glog.V(2).Infof(
		"Finished LocalDevice sync: Watch %q - %q / %q: %s",
		s.request.Watch.GetKind(),
		s.request.Watch.GetNamespace(),
		s.request.Watch.GetName(),
		metaccommon.GetDetailsFromResponse(s.response),
	)
}

// handleError logs the error if any
func (s *syncer) handleError() {
	if s.err == nil {
		// nothing to do if there was no error
		return
	}
	// log this error with context
	glog.Errorf(
		"Failed to sync LocalDevice: Watch %q - %q / %q: %+v",
		s.request.Watch.GetKind(),
		s.request.Watch.GetNamespace(),
		s.request.Watch.GetName(),
		s.err,
	)
	// stop further reconciliation at metac since there was an error
	s.response.SkipReconcile = true
}

func (s *syncer) sync() error {
	fns := []func(){
		s.validateArgs,
		s.skipIfNotLocalDisk,
		s.skipIfEmptyAttachments,
		s.logSyncStart,
		s.registerAttachments,
		s.reconcile,
		s.logSyncFinish,
	}
	for _, fn := range fns {
		fn()
		// post operation checks
		if s.fatal != nil {
			return s.fatal
		}
		if s.err != nil {
			// this logs the error thus avoiding panic in the
			// controller
			s.handleError()
		}
		if s.response.SkipReconcile {
			return nil
		}
	}
	return nil
}

// Sync implements the idempotent logic to sync CStorClusterConfig
//
// NOTE:
// 	SyncHookRequest is the payload received as part of reconcile
// request. Similarly, SyncHookResponse is the payload sent as a
// response as part of reconcile request.
//
// NOTE:
//	SyncHookRequest uses CStorClusterConfig as the watched resource.
// SyncHookResponse has the resources that forms the desired state
// w.r.t the watched resource.
//
// NOTE:
//	Returning error will panic this process. We would rather want this
// controller to run continuously. Hence, the errors are handled.
func Sync(request *generic.SyncHookRequest, response *generic.SyncHookResponse) error {
	s := &syncer{
		request:  request,
		response: response,
	}
	return s.sync()
}

// Reconciler enables reconciliation of CStorClusterConfig instance
type Reconciler struct {
	ObservedCStorClusterConfig *unstructured.Unstructured
	ObservedBlockDevices       []*unstructured.Unstructured
	ObservedCStorPoolCluster   *unstructured.Unstructured

	cccHelper *ccc.Helper

	selectedBlockDevices               []*unstructured.Unstructured
	hostNameToSelectedBlockDeviceNames map[string][]string
	hostNameToObservedCSPCDeviceNames  map[string][]string
	observedHostNamesInCSPC            []string

	deviceSelector             metac.ResourceSelector
	desiredCStorPoolCluster    *unstructured.Unstructured
	isDeviceCountMatchRAIDType bool
	skipReconcile              bool
	skipReconcileReason        string
	raidType                   types.PoolRAIDType
	err                        error
}

// ReconcileResponse is a helper struct used to form the response
// of a successful reconciliation
type ReconcileResponse struct {
	CStorPoolCluster *unstructured.Unstructured
	SkipReconcile    bool
	SkipReason       string
}

// NilReconcileResponse is used to represent a nil
// ReconcileResponse value
var NilReconcileResponse = ReconcileResponse{}

func (r *Reconciler) init() {
	r.cccHelper = ccc.NewHelper(r.ObservedCStorClusterConfig)
}

func (r *Reconciler) setRAIDType() {
	// RAID type is used from CStorClusterConfig specs
	r.raidType, r.err = r.cccHelper.GetRAIDTypeOrCached()
}

// selectFromObservedBlockDevices filters the
// observed blockdevices based on local disk selector terms
func (r *Reconciler) selectFromObservedBlockDevices() {
	r.deviceSelector, r.err = r.cccHelper.GetLocalBlockDeviceSelector()
	if r.err != nil {
		return
	}
	if len(r.deviceSelector.SelectorTerms) == 0 {
		r.err = errors.Errorf(
			"Invalid CStorClusterConfig: No block device selector found",
		)
		return
	}
	l := unstruct.ListSelector(r.deviceSelector, r.ObservedBlockDevices...)
	r.selectedBlockDevices, _ = l.List()
	if len(r.selectedBlockDevices) == 0 {
		r.err = errors.Errorf(
			"0 of %d block devices selected", len(r.ObservedBlockDevices),
		)
	}
}

// mapHostNameToSelectedBlockDevices traverses through all the block devices
// and sets a mapping of hostname to corresponding block device names
func (r *Reconciler) mapHostNameToSelectedBlockDevices() {
	l := bd.NewListHelper(r.selectedBlockDevices)
	r.hostNameToSelectedBlockDeviceNames, r.err = l.GroupDeviceNamesByHostName()
}

func (r *Reconciler) isSelectedBlockDeviceCountMatchRAIDType() {
	// match device count on a per node basis
	for observedNode, selectedBlockDevices := range r.hostNameToSelectedBlockDeviceNames {
		// does device count match the RAID type
		r.isDeviceCountMatchRAIDType, r.err =
			r.cccHelper.IsDiskCountMatchRAIDType(int64(len(selectedBlockDevices)))
		if r.err != nil {
			// this was a runtime error
			return
		}
		if !r.isDeviceCountMatchRAIDType {
			r.err =
				errors.Errorf(
					"Can't reconcile: Invalid block device count %d: RAID %q: Node %q",
					len(selectedBlockDevices), r.raidType, observedNode,
				)
			// unsupported device count on any node makes the entire
			// operation invalid
			return
		}
	}
}

// walkObservedCStorPoolCluster traverses the observed CStorPoolCluster
// and set a mapping of hostname to corresponding block device names
func (r *Reconciler) walkObservedCStorPoolCluster() {
	// CStorPoolCluster can be nil if it was never created
	if r.ObservedCStorPoolCluster == nil {
		// Nothing needs to be done if CSPC is nil
		return
	}
	h := cspc.NewHelper(r.ObservedCStorPoolCluster)
	// set host names in the **order** they are found at CSPC specs
	r.observedHostNamesInCSPC, r.err = h.GetOrderedHostNamesOrCached()
	if r.err != nil {
		return
	}
	// set host name to device names mapping based on what is
	// observed in CSPC specs
	r.hostNameToObservedCSPCDeviceNames, r.err = h.GroupBlockDeviceNamesByHostName()
}

// buildDesiredCStorPoolCluster returns the desired CStorPoolCluster state
//
// NOTE:
//	This logic is idempotent. In other words, it returns same structure
// for every reconcile action i.e. add, update, even no change in state.
func (r *Reconciler) buildDesiredCStorPoolCluster() {
	b := &cspc.Builder{
		Name:                          r.ObservedCStorClusterConfig.GetName(),
		Namespace:                     r.ObservedCStorClusterConfig.GetNamespace(),
		OrderedHostNames:              r.observedHostNamesInCSPC,
		HostNameToObservedDeviceNames: r.hostNameToObservedCSPCDeviceNames,
		HostNameToDesiredDeviceNames:  r.hostNameToSelectedBlockDeviceNames,
		DesiredAnnotations: map[string]string{
			types.AnnKeyCStorClusterConfigUID:       string(r.ObservedCStorClusterConfig.GetUID()),
			types.AnnKeyCStorClusterConfigLocalDisk: "true",
		},
		DesiredRAIDType: r.raidType,
	}
	r.desiredCStorPoolCluster, r.err = b.BuildDesiredState()
}

// Reconcile runs through the reconciliation logic
//
// NOTE:
//	This logic be idempotent. In other words, it behaves the same
// for every reconcile action i.e. add, update, even no change in
// state.
func (r *Reconciler) Reconcile() (ReconcileResponse, error) {
	if r.ObservedCStorClusterConfig == nil {
		return NilReconcileResponse,
			errors.Errorf("Can't reconcile: Nil CStorClusterConfig")
	}
	if len(r.ObservedBlockDevices) == 0 {
		return NilReconcileResponse,
			errors.Errorf("Can't reconcile: Missing block devices")
	}
	r.init()
	fns := []func(){
		r.setRAIDType,
		r.selectFromObservedBlockDevices,
		r.mapHostNameToSelectedBlockDevices,
		r.isSelectedBlockDeviceCountMatchRAIDType,
		r.walkObservedCStorPoolCluster,
		r.buildDesiredCStorPoolCluster,
	}
	for _, fn := range fns {
		fn()
		// post operation checks
		if r.err != nil {
			return NilReconcileResponse, r.err
		}
		if r.skipReconcile {
			return ReconcileResponse{
				SkipReconcile: true,
				SkipReason:    r.skipReconcileReason,
			}, nil
		}
	}
	return ReconcileResponse{
		CStorPoolCluster: r.desiredCStorPoolCluster,
	}, nil
}
