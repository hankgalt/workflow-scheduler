#!/bin/bash

ENV_FILE="./env/local.env"

if [ -f "${ENV_FILE}" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
    echo " - starting scheduler server - DB_HOST: $DB_HOST, DB_NAME: $DB_NAME"
    cd cmd/scheduler && go run scheduler.go
else 
    echo " - ${ENV_FILE} does not exist."
    exit 2
fi
