#!/bin/bash
set -E
if sudo minikube status > /dev/null; then 
    echo 'Minikube is already running. Use `sudo minikube stop` to stop and restart...'
    sudo minikube stop
fi

sudo minikube start --vm-driver=none
