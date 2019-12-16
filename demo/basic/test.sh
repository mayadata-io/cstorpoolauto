#!/bin/bash

cleanup() {
  set +e
  
  echo ""

  echo "--------------------------"
  echo "++ Clean up started"
  echo "--------------------------"

  kubectl delete -f ../../deploy/operator.yaml || true
  kubectl delete -f ../../deploy/rbac.yaml || true
  kubectl delete -f ../../deploy/crd.yaml || true

  kubectl delete -f storage_crd.yaml || true
  kubectl delete -f storage_rbac.yaml || true
  kubectl delete -f storage_deployment.yaml || true

  kubectl delete -f ../../deploy/namespace.yaml || true
  
  kubectl delete -f openebs-operator-1.5.0.yaml || true
  kubectl delete -f cspc-operator.yaml || true
  
  echo "--------------------------"
  echo "++ Clean up completed"
  echo "--------------------------"
}
#trap cleanup EXIT

# Uncomment this if you want to run this script in debug mode
#set -ex

echo -e "\n++ Installing cstorpoolauto operator"
kubectl apply -f ../../deploy/namespace.yaml
kubectl apply -f ../../deploy/crd.yaml
kubectl apply -f ../../deploy/rbac.yaml
kubectl apply -f ../../deploy/operator.yaml
echo -e "\n++ Installed cstorpoolauto operator successfully"

echo -e "\n++ Installing storage-provisioner operator"
curl https://raw.githubusercontent.com/mayadata-io/storage-provisioner/master/deploy/kubernetes/storage_crd.yaml > storage_crd.yaml
curl https://raw.githubusercontent.com/mayadata-io/storage-provisioner/master/deploy/kubernetes/rbac.yaml > storage_rbac.yaml
curl https://raw.githubusercontent.com/mayadata-io/storage-provisioner/master/deploy/kubernetes/deployment.yaml > storage_deployment.yaml
kubectl apply -f storage_crd.yaml
kubectl apply -f storage_rbac.yaml
kubectl apply -f storage_deployment.yaml
echo -e "\n++ Installed storage-provisioner operator successfully"

echo -e "\n++ Applying openebs"
curl https://openebs.github.io/charts/openebs-operator-1.5.0.yaml > openebs-operator-1.5.0.yaml
kubectl apply -f openebs-operator-1.5.0.yaml
echo -e "\n++ Applied openebs successfully"

echo -e "\n++ Applying cspc operator"
curl https://raw.githubusercontent.com/openebs/openebs/master/k8s/cspc-operator.yaml > cspc-operator.yaml
kubectl apply -f cspc-operator.yaml
echo -e "\n++ Applied cspc operator successfully"

echo -e "\n++ Applying a sample cstorpoolauto"
kubectl apply -f basic.yaml
echo -e "\n++ Applied sample cstorpoolauto successfully"
