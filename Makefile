# orbital-stack — canonical command interface.
# Rule of thumb: if it's not in this Makefile, it's not supported.

PYTHON := uv run python

.PHONY: setup test lint typecheck scrape pipeline dashboard-dev dashboard-build docs-serve clean

# Install runtime + dev dependencies and register pre-commit hooks.
setup:
	uv sync --all-extras
	uv run pre-commit install
	uv run pre-commit install --hook-type commit-msg

# Run the full test suite with coverage report.
test:
	uv run pytest

# Lint source and format-check with ruff (no auto-fix).
lint:
	uv run ruff check src tests pipelines scripts
	uv run ruff format --check src tests pipelines scripts

# Static type-check with mypy in strict mode (src/orbital only).
typecheck:
	uv run mypy src/orbital

# Run the UNOOSA scraper once and write a local snapshot (no DVC push).
scrape:
	$(PYTHON) -m orbital.ingest.unoosa

# Execute the full weekly ingest Prefect flow end-to-end locally.
pipeline:
	$(PYTHON) -m pipelines.flows.ingest_flow

# Launch the Evidence.dev dashboard in dev mode with hot reload.
dashboard-dev:
	cd dashboards/tratado_silencioso && npm run dev

# Build a production static bundle of the dashboard.
dashboard-build:
	cd dashboards/tratado_silencioso && npm run build

# Serve the mkdocs-material documentation site locally.
docs-serve:
	uv run mkdocs serve

# Remove build artifacts, caches, and coverage outputs.
clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage coverage.xml dist build
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
