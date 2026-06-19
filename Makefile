IMAGE_PREFIX ?= cria
IMAGE_TAG ?= dev

.PHONY: build-images
build-images:
	docker build -t $(IMAGE_PREFIX)/control-plane:$(IMAGE_TAG) -f control-plane/Dockerfile .
	docker build -t $(IMAGE_PREFIX)/worker:$(IMAGE_TAG) -f workers/python/Dockerfile .

.PHONY: kind-load
kind-load: build-images
	kind load docker-image $(IMAGE_PREFIX)/control-plane:$(IMAGE_TAG)
	kind load docker-image $(IMAGE_PREFIX)/worker:$(IMAGE_TAG)

.PHONY: test
test:
	cd control-plane && cargo test
	PYTHONPATH=workers/python python -m pytest workers/python/tests

.PHONY: smoke
smoke:
	python scripts/smoke_test.py
