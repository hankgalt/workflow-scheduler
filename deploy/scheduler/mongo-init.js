// mongo-init.js
print("⏳ Starting replica set and user setup...");

rs.initiate({
  _id: "rs0",
  version: 1,
  members: [
    { _id: 0, host: "scheduler-mongo:27017", priority: 2 },
    { _id: 1, host: "scheduler-mongo-rep-1:27017", priority: 0 },
    { _id: 2, host: "scheduler-mongo-rep-2:27017", priority: 0 }
  ]
});

print("✅ Replica set initiated. Waiting for primary...");
sleep(5000); // wait for election

const rootUser = "root";
const rootPass = "rootPassword";
const appDb = "scheduler";
const appUser = "schedulerAdmin";
const appPass = "adminPassword";

// Create root user
print("🔐 Creating root user...");
db = db.getSiblingDB("admin");
db.createUser({
  user: rootUser,
  pwd: rootPass,
  roles: ["root"]
});

// Create app user
print("👤 Creating application user...");
db = db.getSiblingDB(appDb);
db.createUser({
  user: appUser,
  pwd: appPass,
  roles: [{ role: "readWrite", db: appDb }]
});

print("✅ Setup complete.");
