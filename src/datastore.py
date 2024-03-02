from pyspark.sql import DataFrame, DataFrameWriter, DataFrameReader
from pyspark.sql.functions import lit
from schema import schema_csv, schema_meta
from utils import rename_columns


class DataStore:

    # def __init__(self, spark, config=None):
    #     self.spark = spark
    #     self.config = config
    def __init__(self, context):
        self.spark = context.spark
        self.config = context.config['data']

    def load_symbol(self, symbol) -> DataFrame:
        df_meta = self.load_metadata().select('symbol', 'securityName')
        df_symbol = self.load_symbol_raw(symbol).withColumn('sym', lit(symbol))

        return df_symbol.join(df_meta, df_symbol['sym'] == df_meta['symbol'])

        #return df_symbol

    def load_metadata(self):
        csv_path = f"{self.config['raw_layer']}/symbols_valid_meta.csv"
        df = self.spark.read.csv(csv_path, schema=schema_meta, header=True)

        return rename_columns(df)

    def load_symbol_raw(self, sym) -> DataFrame:
        csv_path = f"{self.config['raw_layer']}/{sym}.csv"
        df = self.spark.read.csv(csv_path, schema=schema_csv, header=True)

        return rename_columns(df)

    def write_target(self, df, sym):
        pq_path = f"{self.config['drv_layer']}/{sym}.parquet"
        df.write.parquet(path=pq_path, mode="overwrite")

    def read_target(self, sym) -> DataFrame:
        pq_path = f"{self.config['drv_layer']}/{sym}.parquet"
        df = self.spark.read.parquet(pq_path)
        
        return df
