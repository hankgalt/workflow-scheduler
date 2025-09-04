# Temporal.io based workflow scheduler

### Resources
- https://docs.temporal.io/workflows
- https://docs.temporal.io/dev-guide/go
- https://docs.temporal.io/concepts/
- https://protobuf.dev/getting-started/gotutorial/

### Local setup
- db
    - mongo:
        - setup `mongo.env` in `env` with following env vars:
            ```MONGO_HOST, MONGO_REPLICA_SET_NAME,
            MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD,
            MONGO_ADMIN_USER, MONGO_ADMIN_PASS,
            MONGO_APP_DB, MONGO_APP_USER, MONGO_APP_PASS
        - generate `mongo-keyfile` in `deploy/scheduler`
            - `openssl rand -base64 756 > mongo-keyfile && chmod 400 mongo-keyfile`
        - `make start-mongo`
    - mysql:
        - setup db scripts
            - setup data home env, update env name in `scripts/start-db.sh`
            - add `{DATA_HOME}/data/scripts/db-init.sql`
                ```CREATE USER IF NOT EXISTS '<root_user>'@'%' IDENTIFIED WITH mysql_native_password BY '<root_password>';
                    GRANT ALL PRIVILEGES ON *.* TO '<root_user>'@'%';
                    GRANT GRANT OPTION ON *.* TO '<root_user>'@'%';
                    FLUSH PRIVILEGES;

                    CREATE DATABASE IF NOT EXISTS <db_name>;
                    CREATE DATABASE IF NOT EXISTS <db_name_test>;

                    CREATE USER IF NOT EXISTS '<db_user_name>'@'%' IDENTIFIED WITH mysql_native_password BY '<db_user_password>';
                    GRANT ALL PRIVILEGES on <db_name>.* to '<db_user_name>'@'%';
                    GRANT ALL PRIVILEGES on <db_name_test>.* to '<db_user_name>'@'%';
                    FLUSH PRIVILEGES;```
        - setup `mysql.env` in `deploy/scheduler` with following env vars:
            ```MYSQL_ROOT_PASSWORD, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD```
        - `make start-db`
- observability
    - `make start-obs`
- temporal
    - `make start-temporal`
    - `make register-domain`
- services
    - add env configs in `env/` folder
        - scheduler.env (service)
        ```SERVER_PORT, CERTS_PATH, POLICY_PATH, TEMPORAL_HOST, WORKFLOW_DOMAIN, MONGO_PROTOCOL, MONGO_DBNAME,MONGO_HOSTNAME, MONGO_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD, METRICS_PORT, OTEL_ENDPOINT, FILE_NAME, BUCKET```
        - business.env (service)
        ```SERVER_PORT, CERTS_PATH, POLICY_PATH, MONGO_PROTOCOL, MONGO_DBNAME,MONGO_HOSTNAME, MONGO_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD```
        - test.env
        ```CERTS_PATH, POLICY_PATH, TEMPORAL_HOST, WORKFLOW_DOMAIN, MONGO_HOSTNAME, MONGO_CONN_PARAMS, MONGO_USERNAME, MONGO_PASSWORD METRICS_PORT, OTEL_ENDPOINT, MONGO_PROTOCOL, MONGO_DBNAME, GOOGLE_APPLICATION_CREDENTIALS, MONGO_COLLECTION, FILE_NAME, BUCKET```
        - batch.env (worker)
        ```TEMPORAL_HOST, WORKFLOW_DOMAIN, METRICS_PORT, OTEL_ENDPOINT, GOOGLE_APPLICATION_CREDENTIALS```
        - obs.env (observability)
        ```METRICS_PORT, OTEL_ENDPOINT```
    - `make start-worker TARGET=[batch]`
    - `make start-server TARGET=[scheduler,business]` or `make start-service` to start scheduler server from docker image

### Testing
- `make run-client TARGET=[scheduler,business]` or `make test-server TARGET=[scheduler,business]`
- VSCode

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


