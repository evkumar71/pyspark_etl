from pyspark.sql import DataFrame
from pyspark.sql.types import *


class DataStore:

    schema_csv = StructType([
        StructField('Date', DateType()),
        StructField('Open', DoubleType()),
        StructField('High', DoubleType()),
        StructField('Low', DoubleType()),
        StructField('Close', DoubleType()),
        StructField('Adj Close', DoubleType()),
        StructField('Volume', LongType())
    ])

    schema_meta = StructType([
        StructField("Nasdaq Traded", StringType()),
        StructField("Symbol", StringType()),
        StructField("Security Name", StringType()),
        StructField("listing Exchange", StringType()),
        StructField("Market Category", StringType()),
        StructField("ETF", StringType()),
        StructField("Round Lot Size", DoubleType()),
        StructField("Test Issue", StringType()),
        StructField("Financial Status", StringType()),
        StructField("CQS Symbol", StringType()),
        StructField("NASDAQ Symbol", StringType()),
        StructField("NextShares", StringType())
    ])

    def __init__(self, spark, config):

    def load_symbol(self, symbol) -> DataFrame:
        df_meta = self.load_metadata()
        df_symbol = self.load_symbol_raw()
        df_meta.join(df_symbol) # enrich symbol with long symbol name
        return symbol

    def load_metadata(self):
        return None


    def load_symbol_raw(self):
        return None


    def write_target(self, df, name):
        return None