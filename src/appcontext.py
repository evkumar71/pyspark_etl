import json
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


class AppContext:

    def __init__(self, cfg):
        self.config = self.load_config(cfg)
        self.spark = self.get_session()

    def load_config(self, config):
        with open(config, "r") as fh:
            cfg = json.load(fh)

        return cfg

    def get_session(self):
        conf = SparkConf()
        for k, v in self.config['spark'].items():
            conf.set(k, v)

        ses = SparkSession \
                .builder \
                .config(conf=conf) \
                .getOrCreate()

        return ses