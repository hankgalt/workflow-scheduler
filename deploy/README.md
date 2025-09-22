# Mongo setup

- Create a file ./mongo-keyfile with a random high-entropy string (ASCII), and restrict permissions:
    - `openssl rand -base64 756 > mongo-keyfile && chmod 400 mongo-keyfile`

- `docker run -it --rm --network schenet mongo:8.0 mongosh "mongodb://testuser:testpassword@mongo-rep1:27017,mongo-rep2:27017,mongo-rep3:27017/scheduler_test?replicaSet=mongo-rs&directConnection=false"`