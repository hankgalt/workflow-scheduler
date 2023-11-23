
# build scheduler service proto
.PHONY: build-proto
build-proto:
	@echo "building latest scheduler proto for ${HEAD}"
	scripts/build-proto.sh
