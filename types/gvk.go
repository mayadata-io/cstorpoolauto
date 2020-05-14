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

const (
	// GroupDAOMayaDataIO refers to the group for all
	// custom resources defined in this project
	GroupDAOMayaDataIO string = "dao.mayadata.io"

	// GroupOpenEBSIO refers to the group for all
	// custom resources defined in openebs
	GroupOpenEBSIO string = "openebs.io"

	// GroupCStorOpenEBSIO refers to the group for all
	// custom resources defined for cStor
	GroupCStorOpenEBSIO string = "cstor.openebs.io"

	// VersionV1Alpha1 refers to v1alpha1 version of the
	// custom resources used here
	VersionV1Alpha1 string = "v1alpha1"

	// VersionV1 refers to v1version of the custom resources
	// used here
	VersionV1 string = "v1"

	// APIVersionDAOMayaDataV1Alpha1 refers to v1alpha1 api
	// version of DAO based custom resources
	APIVersionDAOMayaDataV1Alpha1 string = GroupDAOMayaDataIO + "/" + VersionV1Alpha1

	// APIVersionOpenEBSV1Alpha1 refers to v1alpha1 api
	// version of openebs based custom resources
	APIVersionOpenEBSV1Alpha1 string = GroupOpenEBSIO + "/" + VersionV1Alpha1

	// APIVersionCStorOpenEBSV1 refers to v1 api version of cStor
	// based custom resources present in OpenEBS project
	APIVersionCStorOpenEBSV1 string = GroupCStorOpenEBSIO + "/" + VersionV1
)

// Kind is a custom datatype to refer to kubernetes native
// resource kind value
type Kind string

const (
	// KindNode refers to kubernetes node (a native resource)
	// kind value
	KindNode Kind = "Node"

	// KindCStorClusterPlan refers to custom resource with
	// kind CStorClusterPlan
	KindCStorClusterPlan Kind = "CStorClusterPlan"

	// KindCStorClusterStorageSet refers to custom resource with
	// kind CStorClusterStorageSet
	KindCStorClusterStorageSet Kind = "CStorClusterStorageSet"

	// KindCStorClusterConfig refers to custom resource with
	// kind CStorClusterConfig
	KindCStorClusterConfig Kind = "CStorClusterConfig"

	// KindStorage refers to custom resource with kind Storage
	KindStorage Kind = "Storage"

	// KindBlockDevice refers to custom resource with kind BlockDevice
	KindBlockDevice Kind = "BlockDevice"

	// KindPersistentVolumeClaim refers to custom resource with kind
	// PersistentVolumeClaim
	KindPersistentVolumeClaim Kind = "PersistentVolumeClaim"

	// KindCStorPoolCluster refers to custom resource with kind
	// CStorPoolCluster
	KindCStorPoolCluster Kind = "CStorPoolCluster"
)
