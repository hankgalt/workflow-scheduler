# Cadence.io based workflow scheduler

- setup db scripts
- `make start-db`
- `make start-server`
- `make run-client` or `make test-server`
- `export GOPRIVATE=github.com/comfforts/comff-config`
- `go mod tidy -go=1.16 && go mod tidy -go=1.17`
- build proto - `make build-proto`
