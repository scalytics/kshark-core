VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.PHONY: all build test vet clean snapshot release docker help

all: vet build ## Run vet then build

build: ## Build the kshark binary
	CGO_ENABLED=0 go build -ldflags '$(LDFLAGS)' -o kshark ./cmd/kshark

test: ## Run tests
	go test ./... -v -race -timeout 120s

vet: ## Run go vet
	go vet ./...

clean: ## Remove build artifacts
	rm -f kshark
	rm -rf dist/

snapshot: ## Build a local snapshot release (no publish)
	goreleaser release --snapshot --clean

release: ## Run GoReleaser to publish a release (requires GITHUB_TOKEN and a git tag)
	goreleaser release --clean

docker: ## Build Docker image
	docker build -t kshark:$(VERSION) -t kshark:latest .

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
