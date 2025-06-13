#!/bin/bash
echo 'Waiting for MongoDB to be ready...'

sleep 5
until mongosh --quiet -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval 'db.adminCommand("ping")'; do
    echo 'MongoDB is not ready yet, retrying...'
    sleep 2
done

sleep 2
echo 'MongoDB ready, initializing replicaSet ...';
mongosh --quiet -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval '
    const replicaSet = process.env.MONGO_REPLICA_SET_NAME;
    try {
        rs.status();
    } catch (e) {
        print("Error getting ReplicaSet status: " + e);
        print("Initializing ReplicaSet now...");
        try {
            rs.initiate({
                _id: replicaSet,
                members: [
                    {_id: 0, host: "scheduler-mongo"},
                    {_id: 1, host: "scheduler-mongo-rep1"},
                    {_id: 2, host: "scheduler-mongo-rep2"}
                ]
            });
            print("ReplicaSet initialized successfully");
        } catch (e) {
            print("Error initializing ReplicaSet: " + e);
        }
    }
';

sleep 5
echo 'Waiting for primary...'
until mongosh --quiet -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval 'rs.isMaster().ismaster' | grep -q true; do
    echo 'Primary not elected yet, retrying...'
    sleep 2
done

echo 'Primary elected, creating admin user...'
mongosh -u ${MONGO_INITDB_ROOT_USERNAME} -p ${MONGO_INITDB_ROOT_PASSWORD} --host ${MONGO_HOST} --eval '
    db = db.getSiblingDB("admin");
    try {
        if (!db.getUser("${MONGO_ADMIN_USERNAME}")) {
            db.createUser({user: "${MONGO_ADMIN_USERNAME}", pwd: "${MONGO_ADMIN_PASSWORD}", roles: [ { role: "root", db: "admin" } ]});
            print("Admin user created successfully");
        }
    } catch (e) {
        print("Error creating admin user: " + e);
    }
';
echo 'MongoDB setup completed successfully.'
exit 0
