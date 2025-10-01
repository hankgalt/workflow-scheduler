# Collect Git info
CURRENT_TAG   := $(shell git describe --tags --abbrev=0 2>/dev/null || echo "none")
CURRENT_DESC  := $(shell git describe --tags --always 2>/dev/null || echo "none")
HEAD_SHA      := $(shell git rev-parse HEAD)
SHORT_SHA     := $(shell git rev-parse --short HEAD)
BRANCH        := $(shell git rev-parse --abbrev-ref HEAD)

IMG_TAG := $(CURRENT_TAG)-$(SHORT_SHA)

# Export so that subcommands (like docker-compose) see them
export IMG_TAG

# current git repo info
git-info:
	@echo "CURRENT_TAG   = $(CURRENT_TAG)"
	@echo "CURRENT_DESC  = $(CURRENT_DESC)"
	@echo "HEAD_SHA      = $(HEAD_SHA)"
	@echo "SHORT_SHA     = $(SHORT_SHA)"
	@echo "BRANCH        = $(BRANCH)"
	@echo "IMG_TAG      = $(IMG_TAG)"

######## - Proto - #######
# build service protos
build-proto:
	@echo "building latest service protos"
	scripts/build-proto.sh

# setup proto tools
setup-proto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

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

# start custom mongo
start-custom-mongo: network
	@echo "Starting custom MongoDB..."
	scripts/start-mongo-custom.sh ${START}

# stop custom mongo
stop-custom-mongo:
	@echo "Stopping MongoDB cluster..."
	@set -a; . env/mongo3n2.env; set +a; docker-compose -f deploy/scheduler/mongo/docker-compose-mongo-3-node-2-phase.yml --profile secure down

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

# show mongo cluster status
mongo-stat:
	docker logs scheduler-mongo-setup

# start neo4j cluster
neo4j:
	@echo "Starting Neo4j cluster..."
	scripts/start-neo4j.sh

# start neo4j cluster with network check
start-neo4j: network neo4j

# stop neo4j cluster
stop-neo4j:
	@echo "Stopping Neo4j cluster..."
	@set -a; . env/neo4j.env; set +a; docker-compose -f deploy/scheduler/docker-compose-neo4j.yml down

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

# build batch worker with docker compose
build-batch-worker:
	@echo "building batch worker docker image with IMG_TAG=$(IMG_TAG) on BRANCH=$(BRANCH)"
	DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1 docker-compose -f deploy/scheduler/docker-compose-batch.yml build --no-cache

# start batch worker with docker compose
start-batch-worker:
	@echo "starting batch worker with IMG_TAG=$(IMG_TAG) on BRANCH=$(BRANCH)"
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

# build scheduler service docker image with docker compose
build-scheduler:
	@echo "building scheduler service docker image with IMG_TAG=$(IMG_TAG) on BRANCH=$(BRANCH)"
	DOCKER_BUILDKIT=1 COMPOSE_DOCKER_CLI_BUILD=1  docker-compose -f deploy/scheduler/docker-compose-scheduler.yml build --no-cache

# start scheduler service with docker compose
start-scheduler:
	@echo "starting scheduler service with IMG_TAG=$(IMG_TAG) on BRANCH=$(BRANCH)"
	docker-compose -f deploy/scheduler/docker-compose-scheduler.yml up -d

# stop docker composed scheduler service
stop-scheduler:
	@echo "stopping scheduler service"
	docker-compose -f deploy/scheduler/docker-compose-scheduler.yml down

######## - Infra - #######
# start infrastructure components
start-infra: network obs temporal wait-30 register-domain

# stop infrastructure components
stop-infra: stop-temporal stop-obs

######## - Utilities - #######
# wait for 10 seconds
wait-10:
	sleep 10

wait-30:
	sleep 30
