#!/bin/sh
set -E
cd pb
go fmt
protoc --go_out=plugins=grpc:. kv.proto

cd ../server
go fmt
go get -v ./...
go build .

cd ../client
go fmt
go get -v ./...
go build .

cd ..
./create-docker-image.sh

launch-tool/launch.py shutdown
launch-tool/launch.py list
launch-tool/launch.py boot 3
launch-tool/launch.py list