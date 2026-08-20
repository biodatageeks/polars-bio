"""Regression tests for the pyBigWig parity-test dependency boundary."""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
PARITY_TEST = ROOT / "tests" / "test_io_bbi_parity.py"


def test_pybigwig_loader_error_fails_parity_test_collection(tmp_path):
    """An installed but unloadable pyBigWig must not turn into a test skip."""
    (tmp_path / "pyBigWig.py").write_text(
        'raise ImportError("synthetic pyBigWig loader failure")\n'
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(tmp_path), env.get("PYTHONPATH")))
    )

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", str(PARITY_TEST)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    output = result.stdout + result.stderr

    assert result.returncode == 2, output
    assert "ERROR collecting tests/test_io_bbi_parity.py" in output
    assert "synthetic pyBigWig loader failure" in output
