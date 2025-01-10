from polars_bio.polars_bio import BioSessionContext


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
        self.ctx = BioSessionContext()
        self.ctx.set_option("datafusion.execution.target_partitions", "1")
        self.ctx.set_option("sequila.interval_join_algorithm", "coitrees")

    def set_option(self, key, value):
        self.ctx.set_option(key, value)


ctx = Context().ctx
