
.PHONY: help
help: ## Show this help
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: all build build-release check test clean deploy
all: check build test ## Perform all the checks builds and testing

check: ## Ensure that the crate meets the basic formatting and structure
	cargo fmt --check
	cargo clippy
	(cd deployment && terraform fmt -check)

build: ## Build the crate with each set of features
	./ci/build.sh

build-release: check test ## Build the release versions of Lambdas
	./ci/build-release.sh

deploy: check ## Deploy the examples
	(cd deployment && terraform apply)

test: ## Run the crate's tests with each set of features
	./ci/test.sh

clean: ## Clean up resources from build
	cargo clean

REGISTRY ?= asia-northeast1-docker.pkg.dev/data-dev-596660/oxbow

.PHONY: docker-build-file-loader docker-push-file-loader
docker-build-file-loader: ## Build file-loader Docker image for linux/amd64
	docker buildx build \
		--platform linux/amd64 \
		-t $(REGISTRY)/file-loader:latest \
		-f cloudrun/file-loader/Dockerfile \
		.

docker-push-file-loader: ## Build and push file-loader Docker image with registry cache
	docker buildx build \
		--platform linux/amd64 \
		-t $(REGISTRY)/file-loader:latest \
		-f cloudrun/file-loader/Dockerfile \
		--cache-from type=registry,ref=$(REGISTRY)/file-loader:cache \
		--cache-to type=registry,ref=$(REGISTRY)/file-loader:cache,mode=max \
		--push \
		.
