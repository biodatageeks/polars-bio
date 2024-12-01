from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING


from polars_bio._internal import __version__ as __version__

if TYPE_CHECKING:
    pass

LIB = Path(__file__).parent

