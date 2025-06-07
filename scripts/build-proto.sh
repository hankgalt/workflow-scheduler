#!/bin/bash
echo " building scheduler service proto definition"
echo "  ($(which protoc) - $(protoc --version))"
if [ $? -ne 0 ]; then
  echo "  !!! protoc not found, please install it first"
  exit 1
fi
protoc api/v1/scheduler.proto --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative --proto_path=.
