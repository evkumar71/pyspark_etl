from pyspark.sql import DataFrame, DataFrameWriter, DataFrameReader
from pyspark.sql.functions import lit
from schema import schema_csv, schema_meta


class DataStore:

    def __init__(self, spark, config=None):
        self.spark = spark
        self.config = config

    def rename_columns(self, df: DataFrame):
        dic = {}
        for col in df.columns:
            lis = col.split()
            new_name = lis[0].lower()
            if len(lis) > 1:
                new_name = f"{lis[0].lower()}{lis[1].capitalize()}"
            dic[col] = new_name

        return df.withColumnsRenamed(dic)

    def load_symbol(self, symbol) -> DataFrame:
        df_meta = self.load_metadata().select('symbol', 'securityName')
        df_sym = self.load_symbol_raw(symbol).withColumn('sym', lit(symbol))
        df_symbol = df_sym.join(df_meta, df_sym['sym'] == df_meta['symbol'])

        return df_symbol

    def load_metadata(self):
        csv_path = f"{self.config['raw_layer']}/symbols_valid_meta.csv"
        df = self.spark.read.csv(csv_path, schema=schema_meta, header=True)

        return self.rename_columns(df)

    def load_symbol_raw(self, sym) -> DataFrame:
        csv_path = f"{self.config['raw_layer']}/{sym}.csv"
        df = self.spark.read.csv(csv_path, schema=schema_csv, header=True)

        return self.rename_columns(df)

    def write_target(self, df, sym):
        pq_path = f"{self.config['drv_layer']}/{sym}.parquet"
        # DataFrameWriter
        df_writer = DataFrameWriter(df)
        df_writer.parquet(path=pq_path, mode="overwrite")

    def read_target(self, sym) -> DataFrame:
        pq_path = f"{self.config['drv_layer']}/{sym}.parquet"

        df_reader = DataFrameReader(self.spark)
        df = df_reader.parquet(pq_path)
        return df
