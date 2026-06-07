.PHONY: test lint typecheck import-check check

test:
	python -m pytest tests/ -q

lint:
	ruff check src/ app/

typecheck:
	mypy --config-file pyproject.toml

import-check:
	lint-imports

check: lint typecheck import-check test
