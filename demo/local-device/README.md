## Introduction
CSPC operator which is a component of OpenEBS. Its a alpha feature, and new version of existing [SPC](https://docs.openebs.io/docs/next/ugcstor.html#creating-cStor-storage-pools).
To provision volumes on CSPC, CSI operators from OpenEBS need to be used. More details about CSPC and CSI which are in alpha version can be found [here](https://docs.openebs.io/docs/next/alphafeatures.html).

Performing operations (be it creation, monitoring, pool expansions, disk replacements etc)  on CSPC (or) SPC yamls involves good amount of admin involvement.

This CStorPoolAuto Data Agility Operator (DAO) makes ease of above mentiond operators on CSPC.
All the steps that admin need to perform are:
- set label on Block Device CRs
- Create `CStorClusterConfig` CR by mentioning above label


### Prerequisite

### Install cspc-operator
Steps to install  [cspc-operator](https://docs.openebs.io/docs/next/alphafeatures.html#install-openebs-cspc-operator) are here.

### Install cstorpoolauto DAO operator
cstorpoolauto DAO operator yaml is given below.

NOTE:- you can install it in any namespace.
```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: maya-system
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cstorpoolauto
  namespace: maya-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: cstorpoolauto
rules:
- apiGroups:
  - "*"
  resources:
  - cstorclusterconfigs
  - blockdevices
  - cstorpoolclusters
  - nodes
  verbs:
  - get
  - list
  - watch
  - create
  - update
  - delete
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: cstorpoolauto
subjects:
- kind: ServiceAccount
  name: cstorpoolauto
  namespace: maya-system
roleRef:
  kind: ClusterRole
  name: cstorpoolauto
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: apiextensions.k8s.io/v1beta1
kind: CustomResourceDefinition
metadata:
  name: cstorclusterconfigs.dao.mayadata.io
  labels:
    name: cstorclusterconfig
spec:
  group: dao.mayadata.io
  version: v1alpha1
  scope: Namespaced
  names:
    plural: cstorclusterconfigs
    singular: cstorclusterconfig
    kind: CStorClusterConfig
    shortNames:
    - cscconfig
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  labels:
    app.mayadata.io/name: cstorpoolauto
    name: cstorpoolauto
  name: cstorpoolauto
  namespace: maya-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app.mayadata.io/name: cstorpoolauto
      name: cstorpoolauto
  serviceName: ""
  template:
    metadata:
      labels:
        app.mayadata.io/name: cstorpoolauto
        name: cstorpoolauto
    spec:
      serviceAccountName: cstorpoolauto
      containers:
      - name: cstorpoolauto
        image: mayadataio/cstorpoolauto:v1.11.2
        command: ["/usr/bin/cstorpoolauto"]
        args:
        - --logtostderr
        - --run-as-local
        - -v=5
        - --discovery-interval=40s
        - --cache-flush-interval=240s
        - --metac-config-path=/etc/config/metac/localdevice/
 ```
 ### Steps to create cspc in auto way
1. Label the selected block devices on which you want to create CStor pools. Block devices that you labeled can be across nodes also.
 
 NOTE:- each selected block device should be in `Active` and `Unclaimed` state and there should not be any file system.
 
2. Apply the `CStorClusterConfig` CR. Mention the label used on step 1 in this CR. One sample `CStorClusterConfig` CR is given below, where label is `mirror-pool: mysql`.
 
 NOTE:- You need to create `CStorClusterConfig` in the same namespace in which `OpenEBS` is installed.
 ```yaml
 apiVersion: dao.mayadata.io/v1alpha1
kind: CStorClusterConfig
metadata:
  name: mirror-mysql-pool
  namespace: openebs
spec:
  diskConfig:
    local:
      # Selected block devices should labeled with `mirror-pool=mysql`
      # If you lobal your block devices with different key and value
      # then update matchLabels accordingly.
      blockDeviceSelector:
        selectorTerms:
        - matchLabels:
            mirror-pool: mysql
  poolConfig:
    raidType: mirror
```

With this, DAO operator creates CSPC with required disk details and defaults.
