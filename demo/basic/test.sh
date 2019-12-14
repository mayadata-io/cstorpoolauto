#!/bin/bash

cleanup() {
  set +e
  
  echo ""

  echo "--------------------------"
  echo "++ Clean up started"
  echo "--------------------------"

  kubectl delete -f openebs-operator-1.5.0.yaml || true
  kubectl delete -f cspc-operator.yaml || true

  kubectl delete -f ../../deploy/operator.yaml || true
  kubectl delete -f ../../deploy/rbac.yaml || true
  kubectl delete -f ../../deploy/crd.yaml || true
  kubectl delete -f ../../deploy/namespace.yaml || true
  
  echo "--------------------------"
  echo "++ Clean up completed"
  echo "--------------------------"
}
#trap cleanup EXIT

# Uncomment this if you want to run this script in debug mode
#set -ex

echo -e "\n++ Installing operator"
kubectl apply -f ../../deploy/crd.yaml
kubectl apply -f ../../deploy/namespace.yaml
kubectl apply -f ../../deploy/rbac.yaml
kubectl apply -f ../../deploy/operator.yaml
echo -e "\n++ Installed operator successfully"


echo -e "\n++ Applying openebs"
curl https://openebs.github.io/charts/openebs-operator-1.5.0.yaml > openebs-operator-1.5.0.yaml
kubectl apply -f openebs-operator-1.5.0.yaml
echo -e "\n++ Applied openebs successfully"

echo -e "\n++ Applying cspc operator"
curl https://raw.githubusercontent.com/openebs/openebs/master/k8s/cspc-operator.yaml 
kubectl apply -f cspc-operator.yaml
echo -e "\n++ Applied cspc operator successfully"

echo -e "\n++ Applying a sample cstorpoolauto"
kubectl apply -f basic.yaml
echo -e "\n++ Applied sample cstorpoolauto successfully"
