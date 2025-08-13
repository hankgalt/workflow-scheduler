#!/bin/bash

ENV_FILE="./env/scheduler.env"

if [ -f "${ENV_FILE}" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
    echo " - starting scheduler server with environment from ${ENV_FILE}"
    cd cmd/servers/scheduler && go run scheduler.go
else 
    echo " - ${ENV_FILE} does not exist."
    exit 2
fi
