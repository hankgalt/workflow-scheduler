#!/bin/bash
export DATA_VOLUME="${HOME}/go/src/github.com/hankgalt/workflow-scheduler/data/mysql"
export INIT_SCRIPT_PATH="${HOME}/go/src/github.com/hankgalt/workflow-scheduler/data/scripts"

echo "data volume: $DATA_VOLUME, init script: $INIT_SCRIPT_PATH"

BASEDIR=$(dirname "$0")
cd ${BASEDIR}/../

docker-compose -f deploy/scheduler/docker-compose-db.yml up -d
