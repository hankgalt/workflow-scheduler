#!/bin/bash

if [ $# -gt 0 ]
    then
        TARGET="$1"
    else
        echo " - worker target is required"
        exit 2
fi

ENV_FILE="./env/${TARGET}.env"

if [ -f "${ENV_FILE}" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
    echo " - starting business ${TARGET} worker..."
    cd cmd/${TARGET}_worker && go run worker.go
else 
    echo " - ${ENV_FILE} does not exist."
    exit 2
fi
