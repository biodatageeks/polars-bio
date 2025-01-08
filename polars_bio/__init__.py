import logging

from .range_op import FilterOp, ctx, nearest, overlap

logging.basicConfig()
logging.getLogger().setLevel(logging.WARN)
logger = logging.getLogger("polars_bio")
logger.setLevel(logging.INFO)

__version__ = "0.4.0"
__all__ = ["overlap", "nearest", "ctx", "FilterOp", "vizualize_intervals"]
