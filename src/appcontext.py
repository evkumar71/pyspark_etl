import json
from pyspark.sql import SparkSession


class app_context():
    def __init__(self):
        self.config = self.load_config()
        self.spark = self.get_session()

    def load_config(self):
        with open("config/config.json", "r") as fh:
            cfg = json.load(fh)

        return cfg

    def get_session(self):
        ses = SparkSession \
                .builder \
                .config(map=self.config['spark']) \
                .getOrCreate()

        return ses