## CStorCluster Data Agility Operator
- CStorCluster data agility operator will manage cstor storage lifecycle
- It will handle the following tasks:
    - Identify availabe storages
    - Provision desired storage(s)
    - Build & apply CStorPoolCluster
    - Manage cstor pool cluster's day 2 operations
- Following are various custom resources to implement the same

```yaml
# This manages the lifecycle of cstor storage
kind: CStorClusterConfig
metadata:
    name:
    namespace:
    labels:
    annotations:
spec:
    # Minimum number of cstor pool instances
    #
    # Defaults to the minimum value amongst the below:
    # - number of allowed nodes or 
    # - number of available kubernetes nodes or 
    # - 3
    minPoolCount:

    # Maximum number of cstor pool instances
    #
    # Default: minPools + 2
    # Note: can be same as minimum count of pools
    maxPoolCount:
    
    # Eligible nodes that can hold cstor pool instances
    #
    # Note: User needs to fill these
    allowedNodes:
        nodeSelectorTerms:
        -   matchLabels:
            matchAnnotations:

    # Disk details used to build cstor pool instances
    diskConfig:
        # Minimum number of disks to be used per cstor pool instance
        #
        # dependent on defaultRAIDType
        # - defaults to 2 if mirror or stripe
        # - default to 3 if raidz
        minCount:
        
        # capacity of each disk that participates in cstor pool instance
        #
        # defaults to 100Gi
        minCapacity:
        
        # provisioner controller used to provision cloud disks
        # 
        # Note: User needs to fill these
        externalProvisioner:
            csiAttacherName:
            storageClassName:

    poolConfig:
        # Write cache determines if all pool instances should have a write cache
        writeCache: # auto detect !!
            csiAttacherName:
            storageClassName:
        
        # Read cache determines if all pool instances should have a read cache
        readCache: # auto detect !! 
            csiAttacherName:
            storageClassName:
        
        # If all pool instances should have a spare
        spare:
        
        # Specify the RAID type i.e. stripe, mirror, raidz
        # for each pool instance
        #
        # Defaults to stripe
        raidType:

        poolExpansion:
            disable:
            threshold:
                used-capacity: 0.80
        
        computeResources:
            requests:
                memory:
                cpu: 
            limits:
                memory:
                cpu:
status:
    phase: 
    conditions: 
```

```yaml
# CStorClusterPlan gets created based on CStorClusterConfig and is
# used to plan the resources especially nodes to form CStorPoolCluster
#
# NOTE:
#   This resource & corresponding controller is expected to manage
# the cluster disruption in a way that lets one to scale up or down
# cstor cluster without impacting the application consuming cstor
# storage.
kind: CStorClusterPlan
metadata:
    # NOTE: Name will be deterministic
    name:  # same name as that of CStorClusterConfig
    namespace:
    annotations:
        # UID of CStorClusterConfig that triggered this resource
        dao.mayadata.io/cstorclusterconfig-uid:
spec:
    nodes:
    - name:  # Name of the node to participate in CStorPoolCluster
      uid:   # UID of the node to participate in CStorPoolCluster
```

```yaml
# CStorClusterStorageSet is used to provision storage for
# one node of cstor cluster
#
# NOTE:
#   This resource & its corresponding controller is expected to
# manage resizing the capacity w.r.t one node within the cstor
# cluster.
kind: CStorClusterStorageSet
metadata:
    # NOTE: Name will be non-deterministic
    generateName: ccplan-
    namespace: # same as CStorClusterPlan
    annotations:
        # UID of CStorClusterPlan that triggered this resource
        dao.mayadata.io/cstorclusterplan-uid:
spec:
    node:
        name:
        uid:
    disk:
        capacity:
        count:
    externalProvisioner:
        csiAttacherName:
        storageClassName:
```

```yaml
# Storage is used to provision storage using PVC PV
# workflow and then attach this storage against the
# declared node.
#
# NOTE:
#   A StorageSet will result in creation of one or
# more Storage instances
kind: Storage
metadata:
    # NOTE: Name will be non deterministic
    generateName: # ccsset-
    namespace: # same as CStorClusterStorageSet
    annotations:
        # UID of CStorClusterStorageSet that triggered this resource
        dao.mayadata.io/cstorclusterstorageset-uid:
spec:
    capacity:
    nodeName:
```

## High Level Design & Workflows
### Workflow for creation of pool cluster
- provide basic inputs to CStorClusterConfig
    - e.g. RAID type, Disk.ExternalProvisioner details

### Workflow for deletion of pool cluster
- minPool &/or maxPool can be decreased

### Workflow for modifying the max pools
- maxPool can be increased


### Workflow when node selectors get changed at runtime

### Workflow when one of nodes running the cstor pool is taken out of cluster
- allowedNodes.nodeSelectorTerms will take care of this scenario. A new node will take the place of the old node. Disks will be detached from old node & 
attached to new node.

## Known Issues
### Correlate block device with volume attachment. 
Block devices may be created via multiple ways. One of ways to have a BlockDevice created is via VolumeAttachment. We do not have a concrete way to map a block device with volume attachment even if the block device was created due to the attachment.

>> let storage code watch for BDC and with annotation, and NDM will not watch for it __ @vitta


## Old Design(s)
### Design 1
This design was the first attempt to get around the difficulties faced to create a CSPC yaml. In addition, it took care of creating & attaching un-available disks 

```yaml
kind: CSPCAuto
metadata:
    name:
    annotations:
        # These annotations that determine the storage provider.
        # They are required to provision storage if required.
        dao.mayadata.io/storage-class-name: 
        dao.mayadata.io/csi-driver-name:
spec:
    # desired state for each pool instance
    cspiList:
        # pool type e.g. either stripe, mirror, or raidz to 
        # configure each cstor pool instance
        poolType:

        # a list of cstor pool instances that can be declared
        # with desired disk count & disk capacity
        items:
        -   # label to uniquely identify a single node in the
            # cluster. In other words this cstor pool instance
            # must get scheduled on this node.
            nodeLabel:
          
            # Desired number of disks that need to participate
            # to build this cstor pool instance
            diskCount:

            # Desired capacity of each disk that participates 
            # in this cstor pool instance
            diskCapacity:
```