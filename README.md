# Temporal.io based workflow scheduler

### Resources
- https://docs.temporal.io/workflows
- https://docs.temporal.io/dev-guide/go
- https://docs.temporal.io/concepts/
- https://protobuf.dev/getting-started/gotutorial/

### Local setup
- db
    - mongo:
        - setup `mongo3n2.env` in `env/` with following env vars:
            ```MONGO_HOST, MONGO_HOST_LIST, MONGO_REPLICA_SET_NAME, MONGO_APP_DB
            MONGO_ADMIN_USER, MONGO_ADMIN_PASS, MONGO_APP_USER, MONGO_APP_PASS```
        - generate `mongo-keyfile` in `deploy/scheduler/mongo`
            - `openssl rand -base64 756 > mongo-keyfile && chmod 400 mongo-keyfile`
        - For local setup, add localhost entries for mongo nodes(container names) in `/etc/hosts` file.
        - `make start-custom-mongo START=bootstrap`
        - Check logs for successful deployment: `docker logs <MONGO_HOST>-setup`
        - `make start-custom-mongo START=secure`
        - Check status
            - `mongosh "mongodb://<MONGO_ADMIN_USER>:<MONGO_ADMIN_PASS>@<MONGO_HOST>:27017,<MONGO_HOST>-rep1:27017,<MONGO_HOST>-rep2:27017/admin?replicaSet=<MONGO_REPLICA_SET_NAME>&directConnection=false"`
            - `mongosh "mongodb://<MONGO_APP_USER>:<MONGO_APP_PASS>@MONGO_HOST>:27017,<MONGO_HOST>-rep1:27017,<MONGO_HOST>-rep2:27017/<MONGO_APP_DB>?replicaSet=<MONGO_REPLICA_SET_NAME>&directConnection=false"`
            - `mongosh "mongodb://testuser:testpass@MONGO_HOST>:27017,<MONGO_HOST>-rep1:27017,<MONGO_HOST>-rep2:27017/<MONGO_APP_DB>_test?replicaSet=<MONGO_REPLICA_SET_NAME>&directConnection=false"`
            - `mongosh "mongodb://testuser:testpass@localhost:27017/scheduler_test?authsource=scheduler&directConnection=true"`
        - `make stop-custom-mongo`
    - mysql:
        - setup db scripts
            - setup data home env, update env name in `scripts/start-db.sh`
            - add `{DATA_HOME}/data/scripts/db-init.sql`
                ```CREATE USER IF NOT EXISTS '<root_user>'@'%' IDENTIFIED WITH mysql_native_password BY '<MYSQL_ROOT_PASSWORD>';
                    GRANT ALL PRIVILEGES ON *.* TO '<root_user>'@'%';
                    GRANT GRANT OPTION ON *.* TO '<root_user>'@'%';
                    FLUSH PRIVILEGES;

                    CREATE DATABASE IF NOT EXISTS <MYSQL_DATABASE>;
                    CREATE DATABASE IF NOT EXISTS <MYSQL_DATABASE_test>;

                    CREATE USER IF NOT EXISTS '<MYSQL_USER>'@'%' IDENTIFIED WITH mysql_native_password BY '<MYSQL_PASSWORD>';
                    GRANT ALL PRIVILEGES on <MYSQL_DATABASE>.* to '<MYSQL_USER>'@'%';
                    GRANT ALL PRIVILEGES on <MYSQL_DATABASE_test>.* to '<MYSQL_USER>'@'%';
                    FLUSH PRIVILEGES;```
        - setup `mysql.env` in `deploy/scheduler` with following env vars:
            ```MYSQL_ROOT_PASSWORD, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD```
        - `make start-mysql`
        - `make stop-mysql`
- observability
    - setup `obs.env` in `env/` with following env vars:
        ```METRICS_PORT, OTEL_ENDPOINT```
    - `make start-obs`
    - `make stop-obs`
- temporal
    - `make start-temporal`
    - `make register-domain`
    - `make stop-temporal`
- services
    - add env configs in `env/` folder
        - scheduler.env (service)
        ```SERVER_PORT, CERTS_PATH, POLICY_PATH, TEMPORAL_HOST, WORKFLOW_DOMAIN, MONGO_PROTOCOL, MONGO_DBNAME, MONGO_HOST_NAME, MONGO_HOST_LIST, MONGO_CLUS_CONN_PARAMS, MONGO_DIR_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DBNAME, MONGO_PROTOCOL, METRICS_PORT, OTEL_ENDPOINT, FILE_NAME, BUCKET```
        - business.env (service)
        ```SERVER_PORT, CERTS_PATH, POLICY_PATH, MONGO_PROTOCOL, MONGO_DBNAME, MONGO_HOST_NAME, MONGO_HOST_LIST, MONGO_CLUS_CONN_PARAMS, MONGO_DIR_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DBNAME, MONGO_PROTOCOL```
        - batch.env (worker)
        ```TEMPORAL_HOST, WORKFLOW_DOMAIN, METRICS_PORT, OTEL_ENDPOINT, GOOGLE_APPLICATION_CREDENTIALS```
    - `make start-batch-worker` to start batch worker
    - Scheduler
        - Setup server certs & policies (needs more details...)
        - Add `scheduler.env` in `env/` folder & add following env vars:
            ```SERVER_PORT, CERTS_PATH, POLICY_PATH, TEMPORAL_HOST, WORKFLOW_DOMAIN, MONGO_PROTOCOL, MONGO_DBNAME, MONGO_HOST_NAME, MONGO_HOST_LIST, MONGO_CLUS_CONN_PARAMS, MONGO_DIR_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DBNAME, MONGO_PROTOCOL, METRICS_PORT, OTEL_ENDPOINT, FILE_NAME, BUCKET```
        - `make start-scheduler` to start scheduler server from docker image
    - Business
        - Setup server certs & policies (needs more details...)
        - Add `business.env` in `env/` folder & add following env vars:
            ```SERVER_PORT, CERTS_PATH, POLICY_PATH, MONGO_PROTOCOL, MONGO_DBNAME, MONGO_HOST_NAME, MONGO_HOST_LIST, MONGO_CLUS_CONN_PARAMS, MONGO_DIR_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DBNAME, MONGO_PROTOCOL```
       - `make start-business` to start business server from docker image

### Testing
- Setup certs & policies (needs more details...)
- add `test.env` in `env/` with following env vars
     ```CERTS_PATH, POLICY_PATH, TEMPORAL_HOST, WORKFLOW_DOMAIN, MONGO_HOST_NAME, MONGO_HOST_LIST, MONGO_CLUS_CONN_PARAMS, MONGO_DIR_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DBNAME, MONGO_PROTOCOL, METRICS_PORT, OTEL_ENDPOINT, MONGO_PROTOCOL, MONGO_DBNAME, GOOGLE_APPLICATION_CREDENTIALS, MONGO_COLLECTION, FILE_NAME, BUCKET```
- VSCode -> open `_test` file, run tests
- add `client.scheduler.env/client.business.env` in `env/` with following env vars:
    ```MONGO_HOST_NAME, MONGO_HOST_LIST, MONGO_CLUS_CONN_PARAMS, MONGO_DIR_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, MONGO_DBNAME, MONGO_PROTOCOL, MONGO_PROTOCOL, MONGO_DBNAME, MONGO_COLLECTION, FILE_NAME, BUCKET```
- VSCode -> Run & Debug -> `Launch Scheduler Client` or
- `make run-client TARGET=[scheduler,business]`

### Maintenance
- protoc
    - `curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v28.3/protoc-28.3-osx-aarch_64.zip`, update version, os & arch.
    - `mv protoc-<version>-<os>-<arch>.zip $HOME/.local/bin/`
    - `unzip protoc-<version>-<os>-<arch>.zip`
    - `export PATH="$HOME/.local/bin:$PATH"`
    - `make setup-proto`
    - `make build-proto`
- module
    - `export GOPRIVATE=github.com/comfforts/comff-config`

### dev notes
- `openssl rand -base64 32`
- grpcurl
    - `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`
- ReadAt reads len(b) bytes from the File starting at byte offset off. It returns the number of bytes read and the error, if any. ReadAt always returns a non-nil error when n < len(b). At end of file, that error is io.EOF. n < len(b) when line ends earlier.
- `docker stats --no-stream --format "table {{.Name}}\t{{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | sort -k 4 -h -r`



