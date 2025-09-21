#!/bin/bash

# Neo4j environment variables
ENV_FILE="./env/neo4j.env"

# Check if the environment file exists and source it
if [ -f "${ENV_FILE}" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
else 
    echo " - ${ENV_FILE} does not exist."
    exit 2
fi

# move to root directory
BASEDIR=$(dirname "$0")
echo " - Base dir: ${BASEDIR}"
cd ${BASEDIR}/../

# Start Neo4j using docker-compose
docker-compose --verbose -f deploy/scheduler/docker-compose-neo4j.yml up -d