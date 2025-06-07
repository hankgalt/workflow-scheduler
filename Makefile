
# build scheduler service proto
build-proto:
	@echo "building latest scheduler proto for ${HEAD}"
	scripts/build-proto.sh

# start database
start-mysql:
	@echo "starting mysql db"
	scripts/start-mysql.sh

# stop database
stop-mysql:
	@echo "stopping mysql db"
	scripts/stop-db.sh

start-mongo:
	@echo "Creating MongoDB cluster..."
	@set -a; . deploy/scheduler/mongo.env; set +a; docker-compose -f deploy/scheduler/docker-compose-mongo.yml up -d

stop-mongo:
	@echo "Stopping MongoDB cluster..."
	@set -a; . deploy/scheduler/mongo.env; set +a; docker-compose -f deploy/scheduler/docker-compose-mongo.yml down

# start scheduler service from local repo
start-server:
	@echo "starting local business service with latest ${HEAD}"
	scripts/start-server.sh

test-server:
	@echo " - testing rev ${HEAD} business server"
	cd cmd/client && grpcurl -key certs/client-key.pem -cert certs/client.pem -cacert certs/ca.pem localhost:65051 list scheduler.v1.Scheduler

# start scheduler service client from local repo
run-client:
	@echo "starting local test client with latest ${HEAD}"
	scripts/start-client.sh

# start temporal server
start-temporal:
	@echo "starting temporal server"
	docker-compose -f deploy/scheduler/docker-compose-temporal.yml up -d

# stop temporal server
stop-temporal:
	@echo "stopping temporal server"
	docker-compose -f deploy/scheduler/docker-compose-temporal.yml down

register-domain:
	@echo "registering scheduler temporal domain"
	docker exec temporal-admin-tools tctl --namespace scheduler-domain namespace register
	docker exec temporal-admin-tools tctl namespace describe scheduler-domain

# start scheduler service with docker compose
start-service:
	@echo "starting scheduler service"
	docker-compose -f deploy/scheduler/docker-compose-sch.yml up -d

# stop docker composed scheduler service
stop-service:
	@echo "starting scheduler service"
	docker-compose -f deploy/scheduler/docker-compose-sch.yml down

# start worker from local repo
start-worker:
	scripts/start-worker.sh ${TARGET}

setup-proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

# gen-go:
# 	which protoc
# 	protoc -I . --go_out ./api --go_opt=paths=source_relative ./**/*.proto --experimental_allow_proto3_optional && \
# 	protoc -I . --go-grpc_out ./api --go-grpc_opt=paths=source_relative ./**/*.proto --experimental_allow_proto3_optional


