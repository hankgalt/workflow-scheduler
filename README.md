# Temporal.io based workflow scheduler

### Resources
- https://docs.temporal.io/workflows
- https://docs.temporal.io/dev-guide/go
- https://docs.temporal.io/concepts/
- https://protobuf.dev/getting-started/gotutorial/

### Local setup
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
- add `mysql.env` in `deploy/scheduler` folder. `MYSQL_ROOT_PASSWORD, MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD`
- `make start-db`
- `make start-temporal`
- `make register-domain`
- add env configs in `env/` folder
    - local
        `SERVER_PORT, DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, TEMPORAL_HOST, WORKFLOW_DOMAIN`
    - test
        `DB_NAME, DB_USER, DB_PASSWORD, CERTS_PATH, POLICY_PATH, TEMPORAL_HOST, WORKFLOW_DOMAIN, CREDS_PATH, BUCKET`
    - worker (business)
        `TEMPORAL_HOST, WORKFLOW_DOMAIN, SCHEDULER_SERVICE_HOST, CREDS_PATH, BUCKET, METRICS_PORT`
- `make start-worker TARGET=business`
- `make start-server`

### Testing
- `make run-client` or `make test-server`
- VSCode

### Maintenance
- build proto - `make build-proto`
- `export GOPRIVATE=github.com/comfforts/comff-config`
- `go mod tidy -go=1.16 && go mod tidy -go=1.17`

### dev notes

- ReadAt reads len(b) bytes from the File starting at byte offset off. It returns the number of bytes read and the error, if any. ReadAt always returns a non-nil error when n < len(b). At end of file, that error is io.EOF. n < len(b) when line ends earlier.
