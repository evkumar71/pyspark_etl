from pyspark.sql import SparkSession
from datastore import DataStore

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

    cls = DataStore(ses=spark)
    df_meta = cls.load_metadata()
    df_meta.show(5)

    symbols = ['ZUO', 'ZVO', 'ZYME', 'ZYNE', 'ZYXI']

    for sym in symbols:
        df_csv = cls.load_symbol_raw(sym)
        df_csv.show(2)

    spark.stop()
