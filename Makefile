
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
