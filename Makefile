.PHONY: fmt format-check vet test test-race build check run compose-up compose-down

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

format-check:
	test -z "$$(gofmt -l .)"

vet:
	go vet ./...

test:
	go test ./...

test-race:
	go test -race ./...

build:
	mkdir -p bin
	go build -trimpath -o bin/taskforge ./cmd/taskforge

check: format-check vet test test-race build

run:
	go run ./cmd/taskforge

compose-up:
	docker compose up --build

compose-down:
	docker compose down
