#!/bin/sh
set -E
docker build -t local/sharded-raft-peer -f Dockerfile_sharded_raft .
docker build -t local/shard-master-peer -f Dockerfile_shard_master .
