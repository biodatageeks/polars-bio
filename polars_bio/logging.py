import logging

logging.basicConfig()
logging.getLogger().setLevel(logging.WARN)
logger = logging.getLogger("polars_bio")
logger.setLevel(logging.INFO)
