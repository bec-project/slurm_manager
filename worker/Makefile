TARGET_ARCH ?= amd64
VERSION ?= 0.0.0-dev

.PHONY: build package test
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=$(TARGET_ARCH) go build -trimpath -ldflags='-s -w' -o ../dist/bec-slurm-worker-linux-$(TARGET_ARCH) ./cmd/bec-slurm-worker

package: export BUILD_VERSION = $(VERSION)
package:
	goreleaser release --snapshot --clean

test:
	go test -race -timeout 60s ./...
	go vet ./...
