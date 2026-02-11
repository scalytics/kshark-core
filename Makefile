VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE    ?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.PHONY: all build test vet clean snapshot release releaseminor docker scan help

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

release: ## Auto-bump patch version, commit, tag, and push to trigger CI release
	@LATEST=$$(git tag --list 'v*' --sort=-v:refname | head -1); \
	if [ -z "$$LATEST" ]; then \
		NEXT="v0.1.0"; \
	else \
		MAJOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f1); \
		MINOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f2); \
		PATCH=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f3 | cut -d- -f1); \
		PATCH=$$((PATCH + 1)); \
		NEXT="v$$MAJOR.$$MINOR.$$PATCH"; \
	fi; \
	echo "Latest tag: $${LATEST:-none}"; \
	echo "Next tag:   $$NEXT"; \
	echo ""; \
	read -p "Create and push tag $$NEXT? [y/N] " CONFIRM; \
	if [ "$$CONFIRM" = "y" ] || [ "$$CONFIRM" = "Y" ]; then \
		if ! git diff --quiet || ! git diff --cached --quiet; then \
			git add -A && \
			git commit -m "Release $$NEXT"; \
		fi; \
		git push origin HEAD && \
		git tag "$$NEXT" && \
		git push origin "$$NEXT" && \
		echo "Tag $$NEXT pushed — CI release will start automatically."; \
	else \
		echo "Aborted."; \
	fi

releaseminor: ## Auto-bump minor version, commit, tag, and push to trigger CI release
	@LATEST=$$(git tag --list 'v*' --sort=-v:refname | head -1); \
	if [ -z "$$LATEST" ]; then \
		NEXT="v0.1.0"; \
	else \
		MAJOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f1); \
		MINOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f2); \
		MINOR=$$((MINOR + 1)); \
		NEXT="v$$MAJOR.$$MINOR.0"; \
	fi; \
	echo "Latest tag: $${LATEST:-none}"; \
	echo "Next tag:   $$NEXT"; \
	echo ""; \
	read -p "Create and push tag $$NEXT? [y/N] " CONFIRM; \
	if [ "$$CONFIRM" = "y" ] || [ "$$CONFIRM" = "Y" ]; then \
		if ! git diff --quiet || ! git diff --cached --quiet; then \
			git add -A && \
			git commit -m "Release $$NEXT"; \
		fi; \
		git push origin HEAD && \
		git tag "$$NEXT" && \
		git push origin "$$NEXT" && \
		echo "Tag $$NEXT pushed — CI release will start automatically."; \
	else \
		echo "Aborted."; \
	fi

releaseminor: ## Auto-bump patch version, tag, and push to trigger CI release
	@LATEST=$$(git tag --list 'v*' --sort=-v:refname | head -1); \
	if [ -z "$$LATEST" ]; then \
		NEXT="v0.1.0"; \
	else \
		MAJOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f1); \
		MINOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f2); \
		PATCH=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f3 | cut -d- -f1); \
		PATCH=0; \
		MINOR=$$((MINOR + 1)); \
		NEXT="v$$MAJOR.$$MINOR.$$PATCH"; \
	fi; \
	echo "Latest tag: $${LATEST:-none}"; \
	echo "Next tag:   $$NEXT"; \
	echo ""; \
	read -p "Create and push tag $$NEXT? [y/N] " CONFIRM; \
	if [ "$$CONFIRM" = "y" ] || [ "$$CONFIRM" = "Y" ]; then \
		git tag "$$NEXT" && \
		git push origin "$$NEXT" && \
		echo "Tag $$NEXT pushed — CI release will start automatically."; \
	else \
		echo "Aborted."; \
	fi


docker: ## Build Docker image
	docker build -t kshark:$(VERSION) -t kshark:latest .

KSHARK_PROPS ?= client.properties
KSHARK_TIMEOUT ?= 120s
KSHARK_LOG ?= reports/kshark-make.log

scan: build ## Run kshark with a 120s global timeout (override with KSHARK_TIMEOUT)
	./kshark -props $(KSHARK_PROPS) -timeout $(KSHARK_TIMEOUT) -log $(KSHARK_LOG)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
