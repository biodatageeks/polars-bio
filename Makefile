
SHELL=/bin/bash

venv:  ## Set up virtual environment
	python3 -m venv .venv
	poetry lock --no-update
	poetry install

install: venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop

install-release: venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop --release

pre-commit: venv
	cargo fmt --all && cargo clippy --all-features
	.venv/bin/python -m ruff check polars_bio tests --fix --exit-non-zero-on-fix
	.venv/bin/python -m ruff format polars_bio tests

test: venv
	.venv/bin/python -m pytest tests

run: install
	source .venv/bin/activate && python run.py

run-release: install-release
	source .venv/bin/activate && python run.py
