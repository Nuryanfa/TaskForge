.PHONY: fmt format-check vet test test-integration test-race build check run-api run-worker migrate compose-up compose-down

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

format-check:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

test:
	go test ./...

test-integration:
	go test ./... -run Integration -count=1

test-race:
	go test -race ./...

build:
	mkdir -p bin
	go build -trimpath -o bin/taskforge-api ./cmd/taskforge-api
	go build -trimpath -o bin/taskforge-worker ./cmd/taskforge-worker
	go build -trimpath -o bin/taskforge-migrate ./cmd/taskforge-migrate

check: format-check vet test test-race build

run-api:
	go run ./cmd/taskforge-api

run-worker:
	go run ./cmd/taskforge-worker

migrate:
	go run ./cmd/taskforge-migrate

compose-up:
	docker compose up --build

compose-down:
	docker compose down
