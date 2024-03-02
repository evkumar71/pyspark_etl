from metrics import *


class Prepare:

    def __init__(self, spark, obj):
        self.spark = spark
        self.symbols = symbols = ['ZUO', 'ZVO', 'ZYME', 'ZYNE', 'ZYXI']
        self.obj_ds = obj

    def process_sym(self):
        df_meta = self.obj_ds.load_metadata()
        df_meta.show(5)

        for sym in self.symbols:
            obj_ds = self.obj_ds
            df_csv = obj_ds.load_symbol(sym)
            obj_ds.write_target(df_csv, sym)
            df_parq = obj_ds.read_target(sym)
            df_parq.show(2)

    def calc_avg(self):
        for sym in self.symbols:
            df = self.obj_ds.load_symbol(sym)
            find_max(df, sym)
            find_sma(df)
