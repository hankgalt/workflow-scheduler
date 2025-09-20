HEAD := $(shell git rev-parse --short HEAD)
SCHED_VERSION := v0.0.12
######## - Proto - #######
# build scheduler service proto
build-proto:
	@echo "building latest scheduler proto"
	scripts/build-proto.sh

# setup proto tools
setup-proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

# gen-go:
# 	which protoc
# 	protoc -I . --go_out ./api --go_opt=paths=source_relative ./**/*.proto --experimental_allow_proto3_optional && \
# 	protoc -I . --go-grpc_out ./api --go-grpc_opt=paths=source_relative ./**/*.proto --experimental_allow_proto3_optional

######## - Network - #######
# setup docker network for local development
network:
	@echo "Creating schenet network if it doesn't exist..."
	@if ! docker network inspect schenet > /dev/null 2>&1; then \
		docker network create schenet; \
		echo "Network schenet created."; \
	else \
		echo "Network schenet already exists."; \
	fi

######## - Data stores - #######
# start mysql database
mysql:
	@echo "Starting mysql db"
	scripts/start-mysql.sh

# start mysql database with network check
start-mysql: network mysql

# stop mysql database
stop-mysql:
	@echo "stopping mysql db"
	scripts/stop-mysql.sh

# start mongo cluster
mongo:
	@echo "Starting MongoDB cluster..."
	scripts/start-mongo.sh

# start mongo cluster with network check
start-mongo: network mongo

# stop mongo cluster
stop-mongo:
	@echo "Stopping MongoDB cluster..."
	@set -a; . env/mongo.env; set +a; docker-compose -f deploy/scheduler/docker-compose-mongo.yml down

######## - Temporal - #######
# start Temporal server
temporal:
	@echo "starting temporal server"
	docker-compose -f deploy/scheduler/docker-compose-temporal.yml up -d

# start Temporal server with network check
start-temporal: network temporal

# stop Temporal server
stop-temporal:
	@echo "stopping temporal server"
	docker-compose -f deploy/scheduler/docker-compose-temporal.yml down

# register Temporal domain
register-domain:
	@echo "registering scheduler temporal domain"
	docker exec temporal-admin-tools tctl --namespace scheduler-domain namespace register
	docker exec temporal-admin-tools tctl namespace describe scheduler-domain

# start Temporal worker
start-worker:
	scripts/start-worker.sh ${TARGET}

# start batch worker with docker compose
start-batch-worker:
	@echo "starting batch worker"
	docker-compose -f deploy/scheduler/docker-compose-batch.yml up -d

# stop docker composed batch worker
stop-batch-worker:
	@echo "stopping batch worker"
	docker-compose -f deploy/scheduler/docker-compose-batch.yml down

# start Temporal dev server for local development & testing
# UI is accessible at http://localhost:8233
start-dev-server:
	@echo "starting temporal dev server for testing"
	docker run --rm -p 7233:7233 -p 8233:8233 temporalio/temporal server start-dev --ip 0.0.0.0

######## - Observability - #######
# start observability
obs:
	@echo "starting observability"
	docker-compose -f deploy/scheduler/docker-compose-observability.yml up -d --force-recreate --build

# start observability with network check
start-obs: network obs

# stop observability
stop-obs:
	@echo "stopping observability"
	docker-compose -f deploy/scheduler/docker-compose-observability.yml down

######## - Clients - #######
# start service client from local repo
run-client:
	@echo "starting local $(TARGET) client"
	scripts/start-client.sh TARGET=$(TARGET)

######## - Services - #######
# start service from local repo
start-server:
	@echo "starting local $(TARGET) service"
	scripts/start-server.sh $(TARGET)

test-server:
	@echo " - testing $(TARGET) server"
	cd cmd/scheduler/client && grpcurl -key certs/client-key.pem -cert certs/client.pem -cacert certs/ca.pem localhost:65051 list scheduler.v1.Scheduler

# start scheduler service with docker compose
start-scheduler:
	@echo "starting scheduler service"
	docker-compose -f deploy/scheduler/docker-compose-scheduler.yml up -d

# stop docker composed scheduler service
stop-scheduler:
	@echo "stopping scheduler service"
	docker-compose -f deploy/scheduler/docker-compose-scheduler.yml down

######## - Infra - #######
# start infrastructure components
start-infra: network obs temporal mongo wait-30 register-domain wait-10
	docker logs scheduler-mongo-setup

# stop infrastructure components
stop-infra: stop-temporal stop-mongo stop-obs

######## - Utilities - #######
# wait for 10 seconds
wait-10:
	@echo "Waiting 10 seconds..."
	sleep 10

wait-30:
	@echo "Waiting 30 seconds..."
	sleep 30
