IMAGE_PREFIX ?= cria
IMAGE_TAG ?= dev
KIND_CLUSTER ?= cria

.PHONY: build-images
build-images:
	docker build -t $(IMAGE_PREFIX)/control-plane:$(IMAGE_TAG) -f control-plane/Dockerfile .
	docker build -t $(IMAGE_PREFIX)/worker-mock:$(IMAGE_TAG) -f workers/python/Dockerfile.mock .

.PHONY: build-llm-image
build-llm-image:
	docker build -t $(IMAGE_PREFIX)/worker-llm:$(IMAGE_TAG) -f workers/python/Dockerfile .

.PHONY: kind-load
kind-load: build-images
	kind load docker-image $(IMAGE_PREFIX)/control-plane:$(IMAGE_TAG) --name $(KIND_CLUSTER)
	kind load docker-image $(IMAGE_PREFIX)/worker-mock:$(IMAGE_TAG) --name $(KIND_CLUSTER)

.PHONY: kind-load-llm
kind-load-llm: build-llm-image
	kind load docker-image $(IMAGE_PREFIX)/worker-llm:$(IMAGE_TAG) --name $(KIND_CLUSTER)

.PHONY: kind-up
kind-up:
	KIND_CLUSTER=$(KIND_CLUSTER) ./scripts/kind-bootstrap.sh

.PHONY: kind-down
kind-down:
	KIND_CLUSTER=$(KIND_CLUSTER) ./scripts/kind-bootstrap.sh --delete

.PHONY: helm-lint
helm-lint:
	helm lint helm/cria
	helm template cria helm/cria --namespace inference-system >/dev/null

.PHONY: test
test:
	cd control-plane && cargo test
	PYTHONPATH=workers/python python -m pytest workers/python/tests

.PHONY: smoke
smoke:
	python scripts/smoke_test.py
