#!/bin/bash

if [ $# -gt 0 ]
    then
        START="$1"
    else
        echo " START is required"
        exit 2
fi

if [ "$START" != "bootstrap" ] && [ "$START" != "secure" ]; then
  echo " A valid start value is either 'bootstrap' or 'secure'"
  exit 2
fi
echo " Start is: ${START}"

# Mongo environment variables
ENV_FILE="./env/mongo3n2.env"

# Check if the environment file exists and source it
if [ -f "${ENV_FILE}" ]; then
    export $(grep -v '^#' $ENV_FILE | xargs)
else 
    echo " ${ENV_FILE} does not exist."
    exit 2
fi

# move to root directory
BASEDIR=$(dirname "$0")
echo " Running from dir: ${BASEDIR}"
cd ${BASEDIR}/../

if [ "$START" == "bootstrap" ]; then
    # Start MongoDB in bootstrap mode
    echo " Starting mongo in ${START} mode."
    docker compose -f deploy/scheduler/mongo/docker-compose-mongo-3-node-2-phase.yml --profile ${START} up -d
else
    # stop and remove bootstrap containers (volumes persist)
    echo " Stopping mongo bootstrap containers."
    docker compose -f deploy/scheduler/mongo/docker-compose-mongo-3-node-2-phase.yml --profile bootstrap down

    # start secure profile
    echo " Starting mongo in ${START} mode."
    docker compose -f deploy/scheduler/mongo/docker-compose-mongo-3-node-2-phase.yml --profile ${START} up -d
fi

# Start MongoDB using docker-compose
# docker compose -f deploy/scheduler/mongo/docker-compose-mongo-3-node-2-phase.yml --profile bootstrap up -d

# docker logs -f mongo-setup
# stop and remove bootstrap containers (volumes persist)
# docker compose -f deploy/scheduler/docker-compose-mongo.yml --profile bootstrap down

# start secure profile
# docker compose -f deploy/scheduler/docker-compose-mongo.yml --profile secure up -d

# mongosh "mongodb://adminuser:adminpass@mongo-rep1:27017,mongo-rep2:27017,mongo-rep3:27017/admin?replicaSet=mongo-rs&directConnection=false"
# mongosh "mongodb://appuser:apppass@mongo-rep1:27017,mongo-rep2:27017,mongo-rep3:27017/scheduler?replicaSet=mongo-rs&directConnection=false"
# mongosh "mongodb://testuser:testpass@mongo-rep1:27017,mongo-rep2:27017,mongo-rep3:27017/scheduler_test?replicaSet=mongo-rs&directConnection=false"

# docker run -it --rm --network schenet mongo:8.0 mongosh "mongodb://testuser:testpassword@mongo-rep1:27017,mongo-rep2:27017,mongo-rep3:27017/scheduler_test?replicaSet=mongo-rs&directConnection=false"

# mongosh "mongodb://testuser:testpass@localhost:27017/scheduler_test?authsource=scheduler&directConnection=true"
