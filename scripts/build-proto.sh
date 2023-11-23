#!/bin/bash
echo "building scheduler service proto definition"
protoc api/v1/scheduler.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --proto_path=.
