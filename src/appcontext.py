import json
from pyspark.sql import SparkSession


class AppContext:

    def __init__(self, cfg):
        self.config = self.load_config(cfg)
        self.spark = self.get_session()

    def load_config(self, config):
        with open(config, "r") as fh:
            cfg = json.load(fh)

        return cfg

    def get_session(self):
        ses = SparkSession \
                .builder \
                .config(map=self.config['spark']) \
                .getOrCreate()

        return ses