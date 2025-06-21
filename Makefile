
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
	.venv/bin/python -m pytest tests/ --ignore=tests/test_overlap_algorithms.py --ignore=tests/test_streaming.py  && .venv/bin/python -m pytest tests/test_overlap_algorithms.py && .venv/bin/python -m pytest tests/test_warnings.py && .venv/bin/python -m pytest tests/test_streaming.py

run: install
	source .venv/bin/activate && python run.py

run-release: install-release
	source .venv/bin/activate && python run.py
