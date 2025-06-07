#!/bin/bash

DIR_PATH=$(dirname $0)
echo "DIR_PATH: $DIR_PATH"

source $DIR_PATH/mongo.env
echo "HOST: $HOST, MONGO_USER: $MONGO_USER, MONGO_ROOT_PASSWORD: $MONGO_ROOT_PASSWORD, MONGO_PASSWORD: $MONGO_PASSWORD"

# check env 
if [ -z "$MONGO_USER" ]; then
  echo "MONGO_USER is not set. Exiting..."
  exit 1
fi

echo "sleeping for 5 seconds to give mongo containers time to initialize"
sleep 5

# echo mongo_setup.sh time now: `date +"%T" `

mongosh --host "$HOST" <<EOF
var cfg = {
  "_id": "rs0",
  "version": 1,
  "members": [
    {
      "_id": 0,
      "host": "mongodb-scheduler:27017",
      "priority": 2
    },
    {
      "_id": 1,
      "host": "mongodb-scheduler-rep-0:27017",
      "priority": 0
    },
    {
      "_id": 2,
      "host": "mongodb-scheduler-rep-1:27017",
      "priority": 0
    }
  ]
};
rs.initiate(cfg);
EOF

# echo "sleeping for 15 seconds to give mongo containers time to make replica set"
# sleep 15

# Create db users
echo "Creating mongo root user"
mongosh --host "$HOST" \
  --eval 'use admin' \
  --eval 'db.createUser({user: "root", pwd: "root", roles: ["readWrite", "dbAdmin"]})'
echo "Created mongo root user"

# echo "Creating mongo user"
# mongosh --host "$HOST" \
#   --eval 'use admin' \
#   --eval 'db.createUser({user: "scheduler", pwd: "scheduler", roles: ["readWrite", "dbAdmin"]})'

# echo "Created mongo user"





