# Cadence.io based workflow scheduler

- setup db scripts
- `make start-db`
- `make start-server`
- `make run-client` or `make test-server`
- `export GOPRIVATE=github.com/comfforts/comff-config`
- `go mod tidy -go=1.16 && go mod tidy -go=1.17`
- build proto - `make build-proto`



- https://github.com/uber-go/cadence-client/issues/1107, ringpop-go and tchannel-go depends on older version of thrift, yarpc brings up newer version https://github.com/uber/cadence/blob/d3d06825adcf11c20ec3fc58e329f1d9560bb729/go.mod#L92
`replace github.com/apache/thrift => github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7`

- https://github.com/uber/cadence-idl

- ReadAt reads len(b) bytes from the File starting at byte offset off. It returns the number of bytes read and the error, if any. ReadAt always returns a non-nil error when n < len(b). At end of file, that error is io.EOF. n < len(b) when line ends earlier.

- `export GOPRIVATE=github.com/comfforts/comff-config`