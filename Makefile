# The commands here are the ones AGENTS.md's "## Checks" section describes and CI runs.
# Keeping them in one file means a developer and a reviewer invoke the same thing, and a
# change to how something is built or tested happens in one place rather than three.

BIN := bin
CMDS := remote-fs remote-fs-server

.PHONY: build test azurite azurite-down clean

## build: compile both binaries into bin/
build:
	@mkdir -p $(BIN)
	go build -o $(BIN)/ $(addprefix ./cmd/,$(CMDS))

## test: run every test in the module
#
# Two layers need something this file does not provide. The layers that mount a filesystem
# need /dev/fuse and skip themselves without it, so a pass here means "everything that
# could run did" rather than "everything ran". The object store layer needs the emulator
# `make azurite` starts, and fails rather than skipping when it is absent. CI makes the
# first distinction itself: it fails on a skip, because the run that proved nothing must
# not report the same green as the run that proved everything.
test:
	go test ./...

## azurite: start the Azure Blob emulator the object store tests run against
azurite:
	docker compose -f deployments/localhost/docker-compose.yml up -d --wait

## azurite-down: stop it; it keeps its state in memory, so nothing survives
azurite-down:
	docker compose -f deployments/localhost/docker-compose.yml down

## clean: remove the build output
clean:
	rm -rf $(BIN)
