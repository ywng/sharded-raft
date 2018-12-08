#!/bin/sh
set -E
cd pb
go fmt
protoc --go_out=plugins=grpc:. kv.proto

cd ../shardmaster
go fmt
go get -v ./...
go build .

cd ../server
go fmt
go get -v ./...
go build .

cd ../sharded-raft-test-client
go fmt
go get -v ./...
go build .

cd ..
./create-docker-image.sh

launch-tool/launch.py shutdown
launch-tool/launch.py shutdown-sm
launch-tool/launch.py list
launch-tool/launch.py list-sm