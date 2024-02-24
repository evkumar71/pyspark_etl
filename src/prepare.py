from datastore import DataStore
from config import app_config
from metrics import *


class Prepare():
    def __init__(self, spark):
        self.spark = spark
        self.config = app_config['data']
        self.symbols = symbols = ['ZUO', 'ZVO', 'ZYME', 'ZYNE', 'ZYXI']
        self.obj_ds = DataStore(spark=self.spark, config=self.config)

    def process_sym(self):
        obj_ds = self.obj_ds
        df_meta = obj_ds.load_metadata()
        df_meta.show(5)

        for sym in self.symbols:
            df_csv = obj_ds.load_symbol(sym)
            obj_ds.write_target(df_csv, sym)
            df_parq = obj_ds.read_target(sym)
            df_parq.show(2)

    def calc_avg(self):
        obj_ds = self.obj_ds

        for sym in self.symbols:
            df = obj_ds.load_symbol(sym)
            find_max(df, sym)
            find_sma(df)
