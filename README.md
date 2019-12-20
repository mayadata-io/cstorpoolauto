# cstorpoolauto
Auto operations for cstor pool

## One YAML to rule it all
```yaml
apiVersion: dao.mayadata.io/v1alpha1
kind: CStorClusterConfig
metadata:
    name: my-cstor-cluster
    namespace: openebs
spec:
    diskConfig:
        externalProvisioner:
            csiAttacherName: pd.csi.storage.gke.io
            storageClassName: csi-gce-pd
    poolConfig:
        raidType: mirror
```