Project: Sharded Raft
------------------

The code itself is in `server`. `client` contains a rather trivial client designed to test lab0. For testing you can use
Kubernetes, we have provided a script in `launch-tool/launch.py`. Please not that `launch.py` hardcodes a bunch of
assumptions about how pods are created, about the fact that we are running under minikube, and that the image itself is
named `local/raft-peer`. As such one can adopt this script for other purposes, but this will need some work.

To use Kubernetes with this project use `./create-docker-image.sh` to first create a Docker image. Then:

-   `./launch.py boot <num peers>` will boot a cluster with `num peers` participants. Each participant is given a list of
  all other participants (so you can connect to them).
-   `./launch.py list` lists all peers in the current cluster. You can use `kubectl logs <name>` to access the log for a
    particular pod.
-   `./launch.py kill <n>` can be used to kill the nth pod.
-   `./launch.py launch <n>` can be used to relaunch the nth pod (e.g., after it is killed).
-   `./launch.py shutdown` will kill all pods, shutting down the cluster.
-   `./launch.py client-url <n>` can be used to get the URL for the nth pod. One example use of this is `./client
    $(../launch-tool/launch.py client-url 1)` to get a client to connect to pod 1.

### To Build the code
`./build.sh` will automatically sourcing the file, go fmt it and build it. It will also call `./create-docker-image.sh` and `./launch.py boot 3`. When the script completes, there will be a Kubernetes clusters of 3 nodes running the raft implementation.

If the Kubernetes has some problem, can call `./boot.sh` to restart the Kubernetes.

### Testing
./client/raftkv_test.go: Simulate the client requests to the raft kv-store under different scenarios, e.g. Leader failure, 2f nodes failed, failed nodes rejoin etc.

./client/raftkv_linerizability_test.go: check the linerizability of client requests using porcupine.
