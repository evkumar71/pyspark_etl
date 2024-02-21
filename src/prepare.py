from pyspark.sql import SparkSession
from datastore import DataStore
from config import app_config

"""
class PrepareData:
for each symbol
    load symbol (csv)
    enrich with long name (from meta)
    store to target layer (parquet)
df = load from target layer (symbol)
calc moving average (df)
calc max

def calc_sma(df)
"""

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("spark etl") \
        .master("local[*]") \
        .getOrCreate()

    print(app_config)
    cls = DataStore(ses=spark, config=app_config['data'])
    df_meta = cls.load_metadata()
    df_meta.show(5)

    symbols = ['ZUO', 'ZVO', 'ZYME', 'ZYNE', 'ZYXI']

    for sym in symbols:
        df_csv = cls.load_symbol_raw(sym)
        cls.write_target(df_csv, sym)
        df_parq = cls.read_target(sym)
        df_new = df_parq.join(df_meta, df_meta['nasdaqSymbol'] == sym) \
                        .select(df_parq['*'], df_meta['nasdaqSymbol'], df_meta['securityName'])
        df_new.show(2)

    spark.stop()
