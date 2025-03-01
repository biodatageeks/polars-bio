import datetime

import datafusion

from polars_bio.polars_bio import BioSessionContext
from polars_bio.range_op_helpers import tmp_cleanup

from .logging import logger


def singleton(cls):
    """Decorator to make a class a singleton."""
    instances = {}

    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return get_instance


@singleton
class Context:
    def __init__(self):
        logger.info("Creating BioSessionContext")
        self.ctx = BioSessionContext(seed=str(datetime.datetime.now().timestamp()))
        init_conf = {
            "datafusion.execution.target_partitions": "1",
            "datafusion.execution.parquet.schema_force_view_types": "true",
        }
        for k, v in init_conf.items():
            self.ctx.set_option(k, v)
        self.ctx.set_option("sequila.interval_join_algorithm", "coitrees")
        self.config = datafusion.context.SessionConfig(init_conf)

    def __del__(self):
        tmp_cleanup(self.ctx.seed)

    def set_option(self, key, value):
        self.ctx.set_option(key, value)
        self.config.set(key, value)


def set_option(key, value):
    Context().set_option(key, value)


ctx = Context().ctx
