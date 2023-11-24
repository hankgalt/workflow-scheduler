#!/bin/bash
export DATA_VOLUME="${GO_HANK_HOME}/workflow-scheduler/data/mysql"
export INIT_SCRIPT_PATH="${GO_HANK_HOME}/workflow-scheduler/data/scripts"

echo "data volume: $DATA_VOLUME, init script: $INIT_SCRIPT_PATH"

BASEDIR=$(dirname "$0")
cd ${BASEDIR}/../

docker-compose -f deploy/scheduler/docker-compose-db.yml down