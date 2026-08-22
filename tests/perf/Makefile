.PHONY: fix lint test run

PROFILE ?= managed-agent-turn.yaml

fix:
	uv run ruff format .
	uv run ruff check --fix .

lint:
	uv run ruff format --check .
	uv run ruff check .

test:
	uv run pytest -q

run:
	uv run python -m perf_harness.cli run $(PROFILE)
