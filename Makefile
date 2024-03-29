
# build scheduler service proto
.PHONY: build-proto
build-proto:
	@echo "building latest scheduler proto for ${HEAD}"
	scripts/build-proto.sh

# start database
.PHONY: start-db
start-db:
	@echo "starting mysql db"
	scripts/start-db.sh

# stop database
.PHONY: stop-db
stop-db:
	@echo "stopping mysql db"
	scripts/stop-db.sh

# start scheduler service from local repo
.PHONY: start-server
start-server:
	@echo "starting local business service with latest ${HEAD}"
	scripts/start-server.sh

.PHONY: test-server
test-server:
	@echo " - testing rev ${HEAD} business server"
	cd cmd/client && grpcurl -key certs/client-key.pem -cert certs/client.pem -cacert certs/ca.pem localhost:65051 list scheduler.v1.Scheduler

# start scheduler service client from local repo
.PHONY: run-client
run-client:
	@echo "starting local test client with latest ${HEAD}"
	scripts/start-client.sh

# start temporal server
.PHONY: start-temporal
start-temporal:
	@echo "starting temporal server"
	docker-compose -f deploy/scheduler/docker-compose-temporal.yml up -d

# stop temporal server
.PHONY: stop-temporal
stop-temporal:
	@echo "stopping temporal server"
	docker-compose -f deploy/scheduler/docker-compose-temporal.yml down

.PHONY: register-domain
register-domain:
	@echo "registering scheduler temporal domain"
	docker exec temporal-admin-tools tctl --namespace scheduler-domain namespace register
	docker exec temporal-admin-tools tctl namespace describe scheduler-domain

# start scheduler service with docker compose
.PHONY: start-service
start-service:
	@echo "starting scheduler service"
	docker-compose -f deploy/scheduler/docker-compose-sch.yml up -d

# stop docker composed scheduler service
.PHONY: stop-service
stop-service:
	@echo "starting scheduler service"
	docker-compose -f deploy/scheduler/docker-compose-sch.yml down

# start worker from local repo
.PHONY: start-worker
start-worker:
	scripts/start-worker.sh ${TARGET}


