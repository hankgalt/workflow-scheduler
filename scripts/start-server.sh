#!/bin/bash

if [ $# -gt 0 ]
    then
        TARGET="$1"
    else
        echo " server target is required!!!"
        exit 2
fi

ENV_FILE="./env/${TARGET}.env"

if [ -f "${ENV_FILE}" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
    echo " - starting ${TARGET} server with environment from ${ENV_FILE}"
    cd cmd/servers/${TARGET} && go run server.go
else 
    echo " - ${ENV_FILE} does not exist."
    exit 2
fi
