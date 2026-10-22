DEV := docker run --rm -v $(CURDIR):/app -w /app catalog-import:dev
NET := docker run --rm --network imports_default --network-alias tests -v $(CURDIR):/app -w /app \
	-e DATABASE_URL=postgresql+psycopg://imports:imports@postgres:5432/imports \
	-e BROKER_URL=amqp://imports:imports@rabbitmq:5672// -e RESULT_BACKEND=redis://redis:6379/0 \
	-e API_URL=http://api:8000 catalog-import:dev

.PHONY: demo up down image lint test test-integration bench chaos k8s-validate

demo:              ## Guided tour: 10 checked steps incl. killing workers mid-import (~2 min)
	./demo/run.sh

up:                ## API :8001, 2 import workers, webhook worker, beat, RabbitMQ (UI :15673), Postgres, Redis
	docker compose up -d --build --wait

down:
	docker compose --profile observability down -v

image:
	docker build -q --target dev -t catalog-import:dev .

lint: image        ## ruff + mypy --strict
	$(DEV) sh -c 'uv run --frozen ruff check . && uv run --frozen ruff format --check . && uv run --frozen mypy'

test: image        ## unit tests, no services needed
	$(DEV) uv run --frozen pytest -q tests/unit

test-integration: image up   ## against the running stack, with a webhook receiver in the test container
	$(NET) uv run --frozen pytest -q tests

bench:             ## rows/s with 1, 2 and 4 import workers, no faults (results in bench/results)
	./bench/run.sh

chaos:             ## imports keep arriving while workers and RabbitMQ are SIGKILLed
	./chaos/run.sh

k8s-validate:
	docker run --rm -v $(CURDIR)/deploy/k8s:/k registry.k8s.io/kubectl:v1.34.1 kustomize /k > rendered.yaml
	docker run --rm -v $(CURDIR)/rendered.yaml:/r.yaml ghcr.io/yannh/kubeconform:latest -strict -summary /r.yaml
