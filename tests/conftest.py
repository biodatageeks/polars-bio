"""
Test-time compatibility shims.

- Provide a lightweight `pytest.assume()` context manager so tests can
  soft-assert multiple conditions without stopping at the first failure.
- Ensure `pl.testing` attribute is available even if the parent package
  doesn't expose the submodule on import (older Polars behavior).
"""

from __future__ import annotations

import contextlib
import threading

import pytest

# --- pytest.assume shim ----------------------------------------------------
# Collect assumption failures per-test, then fail at teardown with a summary.
_local = threading.local()


@pytest.fixture(autouse=True)
def _assume_collector():
    _local.failures = []  # reset for each test function
    try:
        yield
    finally:
        failures = getattr(_local, "failures", [])
        if failures:
            # Summarize all assumption failures at the end of the test
            pytest.fail("Assumptions failed:\n- " + "\n- ".join(failures))


def _install_pytest_assume():
    if not hasattr(pytest, "assume"):

        @contextlib.contextmanager
        def assume():
            try:
                yield
            except AssertionError as e:  # record, don't stop the test
                failures = getattr(_local, "failures", None)
                if failures is not None:
                    failures.append(str(e))
                else:
                    # Outside of a test context: re-raise
                    raise

        # Attach to pytest namespace
        setattr(pytest, "assume", assume)


_install_pytest_assume()


# --- polars.testing exposure ----------------------------------------------
try:
    import polars as pl
    import polars.testing as _pl_testing  # type: ignore

    # Some versions don't expose `pl.testing` on the parent module by default.
    if not hasattr(pl, "testing"):
        pl.testing = _pl_testing  # type: ignore[attr-defined]
except Exception:
    # If Polars isn't importable here, let the tests surface the real error.
    pass
