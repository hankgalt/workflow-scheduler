#!/usr/bin/env bash
set -euo pipefail

# ---- Config (env or defaults) -----------------------------------
MONGO_REPLICA_SET_NAME="${MONGO_REPLICA_SET_NAME:-scheduler-rs}"
MONGO_HOST="${MONGO_HOST:-scheduler-mongo:27017}"

RS_URI="mongodb://${MONGO_HOST_LIST}/?replicaSet=${MONGO_REPLICA_SET_NAME}&directConnection=false"

echo "===> Mongo host: ${MONGO_HOST}"
echo "===> Mongo host list: ${MONGO_HOST_LIST}"
echo "===> Replica set: ${MONGO_REPLICA_SET_NAME}"
echo "===> RS URI: ${RS_URI}"
echo "===> Mongo application DB: ${MONGO_APP_DB}"

# ---- Initiate RS (idempotent) -----------------------------------
mongosh "mongodb://${MONGO_HOST}/?directConnection=true" --quiet --eval "
  try {
    rs.status();
    print('Replica set already initiated.');
  } catch (e) {
    print('Initiating replica set...');
    rs.initiate({
      _id: '${MONGO_REPLICA_SET_NAME}',
      members: [
        { _id: 0, host: '${MONGO_HOST}', priority: 2 },
        { _id: 1, host: 'scheduler-mongo-rep1', priority: 1 },
        { _id: 2, host: 'scheduler-mongo-rep2', priority: 1 }
      ],
      settings: { electionTimeoutMillis: 10000 }
    });
    print('Initiated replica set...');
  }
"

# ---- Wait for any PRIMARY ---------------------------------------
mongosh "${RS_URI}" --quiet --eval '
  function waitForPrimary() {
    print("Waiting for primary...");
    for (;;) {
      try {
        const s = rs.status();
        const p = (s.members || []).find(m => m.stateStr === "PRIMARY");
        if (p) { print("PRIMARY is " + p.name); return; }
      } catch (e) {}
      print("Primary not elected yet, retrying...");
      sleep(1000);
    }
  }
  waitForPrimary();
'

# ---- Create users (idempotent) ----------------------------------
mongosh "${RS_URI}" --quiet --eval "
  const adminUser = process.env.MONGO_ADMIN_USER;
  const admin = db.getSiblingDB('admin');
  print('Checking for admin user...');
  if (!admin.getUser(adminUser)) {
    print('Creating admin user...');
    admin.createUser({ user: adminUser, pwd: process.env.MONGO_ADMIN_PASS, roles: [ { role: 'root', db: 'admin' } ] });
    print('Created admin user...');
  } else {
    print('Admin user exists.');
  }

  const appDb = db.getSiblingDB(process.env.MONGO_APP_DB);
  const appUser = process.env.MONGO_APP_USER;
  print('Checking for app user...');
  if (!appDb.getUser(appUser)) {
    print('Creating app user...');
    appDb.createUser({ user: appUser, pwd: process.env.MONGO_APP_PASS, roles: [ { role: 'readWrite', db: process.env.MONGO_APP_DB } ] });
    print('Created app user...');
  } else {
    print('App user exists.');
  }

  const apptestDb = db.getSiblingDB(process.env.MONGO_APP_DB + '_test');
  print('Checking for test user...');
  if (!apptestDb.getUser('testuser')) {
    print('Creating test user...');
    apptestDb.createUser({ user: 'testuser', pwd: 'testpass', roles: [ { role: 'readWrite', db: process.env.MONGO_APP_DB + '_test' } ] });
    print('Created test user...');
  } else {
    print('Test user exists.');
  }
"

echo "===> Bootstrap complete."
