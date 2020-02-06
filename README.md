# cstorpoolauto
Automated day 2 operations for OpenEBS cstor pool

## One YAML to rule it all
```yaml
apiVersion: dao.mayadata.io/v1alpha1
kind: CStorClusterConfig
metadata:
    name: my-cstor-cluster
    namespace: openebs
spec:
    diskConfig:
        external:
            csiAttacherName: pd.csi.storage.gke.io
            storageClassName: csi-gce-pd
    poolConfig:
        raidType: mirror
```

## What is this all about?
This project is a Kubernetes operator that eases managing OpenEBS cstor pools. This operator handles day 2 operations some of which are listed below:
- create pools by understanding the Kubernetes cluster this operator is deployed on,
- handle scale up & down of pool instances
- manage resize of pool instances
- dynamic provisioning of cloud disks that participate in pool formation
- dynamic attach & detach of these cloud disks

## How to run this?
We shall follow these steps to run a sample demo of this operator.

```
----------------------
Step 0/
----------------------
```

- This demo uses GKE, Google PD CSI driver.
```bash
git clone https://github.com/kubernetes-sigs/gcp-compute-persistent-disk-csi-driver.git
cd gcp-compute-persistent-disk-csi-driver/

# NOTE:
# Use the latest stable version
# 0.6 was the stable as on Dec 2019
git checkout release-0.6

cd gcp-compute-persistent-disk-csi-driver/deploy
```

```
------------------------
Step 1/
------------------------
```

- This demo uses a GKE setup
```bash
# FYI... Use your **own** GKE setup !!!
project name - K8s-Demo
project id - strong-eon-111101

GCE_PD_SA_DIR="./my-secret" GCE_PD_SA_NAME="smart-dao" PROJECT="strong-eon-111101" ./setup-project.sh

# a sample successful output
created key [aadsesdxxxxxxxxxxxxa34342d1] of type [json] as [./my-secret/cloud-sa.json] for [smart-dao@strong-eon-111101.iam.gserviceaccount.com]
```

```
-------------------
Step 2/
-------------------
```

- Verify your Kubernetes setup
```bash
> kubectl version

Client Version: version.Info{Major:"1", Minor:"14", GitVersion:"v1.14.1", GitCommit:"b7394102d6ef778017f2ca4046abbaa23b88c290", GitTreeState:"clean", BuildDate:"2019-04-08T17:11:31Z", GoVersion:"go1.12.1", Compiler:"gc", Platform:"linux/amd64"}

Server Version: version.Info{Major:"1", Minor:"13+", GitVersion:"v1.13.11-gke.14", GitCommit:"56d89863d1033f9668ddd6e1c1aea81cd846ef88", GitTreeState:"clean", BuildDate:"2019-11-07T19:12:22Z", GoVersion:"go1.12.11b4", Compiler:"gc", Platform:"linux/amd64"}
```

```bash
> kubectl get nodes
> kubectl get pods --all-namespaces
> kubectl get sts --all-namespaces
> kubectl get deploy --all-namespaces
```

```
--------------------------
Step 3/
--------------------------
```

- Deploy the RBAC & operator required to run Google PD CSI driver
```bash
GCE_PD_SA_DIR="./my-secret" GCE_PD_DRIVER_VERSION="stable" ./kubernetes/deploy-driver.sh

# sample successful output
+ kubectl apply -v=2 -f /tmp/gcp-compute-persistent-disk-csi-driver-specs-generated.yaml
serviceaccount/csi-controller-sa created
serviceaccount/csi-node-sa created
clusterrole.rbac.authorization.k8s.io/driver-registrar-role created
clusterrole.rbac.authorization.k8s.io/external-attacher-role created
clusterrole.rbac.authorization.k8s.io/external-provisioner-role created
clusterrole.rbac.authorization.k8s.io/external-snapshotter-role created
clusterrolebinding.rbac.authorization.k8s.io/csi-controller-attacher-binding created
clusterrolebinding.rbac.authorization.k8s.io/csi-controller-provisioner-binding created
clusterrolebinding.rbac.authorization.k8s.io/csi-controller-snapshotter-binding created
clusterrolebinding.rbac.authorization.k8s.io/driver-registrar-binding created
daemonset.apps/csi-gce-pd-node created
statefulset.apps/csi-gce-pd-controller created

> echo $?
0
```

```
----------------
Step 4/
----------------
```

- Verify if Google PD CSI driver is running successfully
```bash
> kubectl get pods --all-namespaces
NAMESPACE     NAME                                                    READY   STATUS      RESTARTS   AGE
default       csi-gce-pd-controller-0                                 0/3     Pending     0          99s
default       csi-gce-pd-node-kp4gp                                   2/2     Running     0          101s
default       csi-gce-pd-node-wwllq                                   0/2     Pending     0          101s
default       csi-gce-pd-node-zf4mw                                   2/2     Running     0          101s

> kubectl get sts --all-namespaces
NAMESPACE   NAME                    READY   AGE
default     csi-gce-pd-controller   0/1     2m17s

> kubectl get daemonset --all-namespaces
NAMESPACE     NAME                       DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR                                  AGE
default       csi-gce-pd-node            3         3         2       3            2           <none>                                         2m48s
```

```
-------------
Step 5/
----------------
```

- Deploy the secret to Kubernetes cluster
- This secret was generated in the very first step
- This secret has grants to manage Google PD(s)
```bash
> kubectl create secret generic cloud-sa --from-file=./my-secret/cloud-sa.json

> kubectl get secret
NAME                            TYPE                                  DATA   AGE
cloud-sa                        Opaque                                1      49s
csi-controller-sa-token-5nw8r   kubernetes.io/service-account-token   3      2d6h
csi-node-sa-token-t9q7d         kubernetes.io/service-account-token   3      2d6h
default-token-mc44p             kubernetes.io/service-account-token   3      8d
```

```
-------------
Step 6/
----------------
```

- Steps to run the demo starts here
```bash
git clone https://github.com/mayadata-io/cstorpoolauto.git

# move to below folder
> cd cstorpoolauto/demo/basic

> ll
total 16
drwxr-xr-x 2 amit amit 4096 Dec 15 12:07 ./
drwxr-xr-x 3 amit amit 4096 Dec 15 08:09 ../
-rw-r--r-- 1 amit amit  268 Dec 15 08:09 basic.yaml
-rw-r--r-- 1 amit amit 2233 Dec 15 12:07 test.sh

> chmod 755 test.sh 
 
> ll
total 16
drwxr-xr-x 2 amit amit 4096 Dec 15 12:07 ./
drwxr-xr-x 3 amit amit 4096 Dec 15 08:09 ../
-rw-r--r-- 1 amit amit  268 Dec 15 08:09 basic.yaml
-rwxr-xr-x 1 amit amit 2233 Dec 15 12:07 test.sh*
```

- Run the demo
```
./test.sh
```