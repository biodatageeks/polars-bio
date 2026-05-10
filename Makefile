
SHELL=/bin/bash

venv:  ## Set up virtual environment
	uv sync

install: venv
	unset CONDA_PREFIX && \
	uv run maturin develop

install-release: venv
	unset CONDA_PREFIX && \
	uv run maturin develop --release

pre-commit: venv
	cargo fmt --all && cargo clippy --all-features
	uv run ruff check polars_bio tests --fix --exit-non-zero-on-fix
	uv run ruff format polars_bio tests

test: venv
	uv run pytest tests/ \
			--ignore=tests/test_overlap_algorithms.py \
			--ignore=tests/test_streaming.py \
		&& uv run pytest tests/test_overlap_algorithms.py \
		&& uv run pytest tests/test_warnings.py \
		&& uv run pytest tests/test_streaming.py

run: install
	uv run python run.py

run-release: install-release
	uv run python run.py
