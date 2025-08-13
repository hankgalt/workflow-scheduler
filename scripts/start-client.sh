#!/bin/bash

if [ $# -gt 0 ]
    then
        TARGET="$1"
    else
        echo " client target is required!!!"
        exit 2
fi

cd cmd/clients/${TARGET} && go run client.go