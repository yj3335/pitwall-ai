SHELL := /bin/bash
ROOT  := $(shell pwd)
ENV   := source $(ROOT)/.venv/env.sh &&
PY    := $(ROOT)/.venv/bin/python
SESSION ?= 9558
MODELS := qwen2.5-coder:14b llama3.2:3b bge-m3

.PHONY: venv ollama-pull services-up services-down status build data lakehouse index \
        run stop mcp-sim api processor replay web-dev test test-go test-py test-web test-web-live bench robustness smoke clean

## toolchain ---------------------------------------------------------------
venv:
	bash scripts/bootstrap-venv.sh

ollama-pull:
	@for m in $(MODELS); do \
	  if ollama list | awk 'NR>1{print $$1}' | grep -qx "$$m" || ollama list | awk 'NR>1{print $$1}' | grep -qx "$$m:latest"; then \
	    echo "present: $$m"; else ollama pull $$m; fi; done

services-up:
	bash scripts/services.sh up

services-down:
	bash scripts/services.sh down

status:
	bash scripts/run-stack.sh status

build:
	$(ENV) mkdir -p bin && go build -o bin/ ./cmd/...

## data --------------------------------------------------------------------
data:
	$(PY) scripts/fetch-openf1.py
	$(PY) scripts/build-fia-corpus.py   # add --include-2026 for the (new-formula) 2026 regulations

lakehouse: build
	rm -rf data/lakehouse/telemetry data/lakehouse/laps data/lakehouse/triggers
	$(ENV) for f in data/races/session_*_events.jsonl.gz; do ./bin/processor -from-file $$f -out data/lakehouse; done
	$(PY) scripts/measure-pit-loss.py

index:
	$(ENV) cd python && $(PY) -m pitwall.rag.indexer

## run ---------------------------------------------------------------------
mcp-sim: build
	$(ENV) ./bin/mcp-sim -http :8081

processor: build
	$(ENV) ./bin/processor -brokers localhost:9092 -out data/lakehouse_live

replay: build
	$(ENV) ./bin/replayer -brokers localhost:9092 -file data/races/session_$(SESSION)_events.jsonl.gz -speed 20

api:
	$(ENV) cd python && $(PY) -m uvicorn pitwall.api.app:app --host 127.0.0.1 --port 8080

run:
	bash scripts/run-stack.sh up

stop:
	bash scripts/run-stack.sh down

web-dev:
	$(ENV) cd web && npm run dev

## verification ------------------------------------------------------------
test: test-go test-py test-web

test-go:
	$(ENV) go vet ./... && go test -race -count=1 ./...

test-py:
	$(ENV) cd python && $(PY) -m pytest -q

test-web:
	$(ENV) cd web && npx tsc --noEmit && npm test -- --ci && npm run build && PW_NO_BUILD=1 npx playwright test

test-web-live:   # real stack + real models: needs `make run` and a replay
	$(ENV) cd web && npm run build && PW_NO_BUILD=1 PITWALL_E2E_LIVE=1 npx playwright test e2e/live.spec.ts

bench: build
	bash scripts/bench.sh

robustness:   # about 2 hours: fault seeds, temperature sweep, input sensitivity (after make bench)
	bash scripts/robustness.sh

smoke:
	bash scripts/smoke-test.sh

clean:
	rm -rf bin data/lakehouse/session=* .run
