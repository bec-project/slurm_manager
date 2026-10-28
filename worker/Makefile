TARGET_ARCH ?= amd64

.PHONY: build test
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=$(TARGET_ARCH) go build -trimpath -ldflags='-s -w' -o ../dist/slurm-worker-linux-$(TARGET_ARCH) ./cmd/slurm-worker

test:
	go test -race -timeout 60s ./...
	go vet ./...
