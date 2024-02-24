from pyspark.sql import DataFrame, DataFrameWriter, DataFrameReader
from pyspark.sql.functions import lit
from schema import schema_csv, schema_meta


class DataStore:

    def __init__(self, spark, config=None):
        self.spark = spark
        self.config = config

    def load_symbol(self, symbol) -> DataFrame:
        df_meta = self.load_metadata().select('symbol', 'securityName')
        df_sym = self.load_symbol_raw(symbol).withColumn('sym', lit(symbol))
        df_symbol = df_sym.join(df_meta, df_sym['sym'] == df_meta['symbol'])

        return df_symbol

    def load_metadata(self):
        csv_path = f"{self.config['raw_layer']}/symbols_valid_meta.csv"
        df = self.spark.read.csv(csv_path, schema=schema_meta, header=True)

        df2 = df.select(df['Nasdaq Traded'].alias('nasdaqTraded'),
                        df['Symbol'].alias('symbol'),
                        df['Security Name'].alias('securityName'),
                        df['Listing Exchange'].alias('listingExchange'),
                        df['Market Category'].alias('marketCategory'),
                        df['ETF'].alias('etf'),
                        df['Round Lot Size'].alias('roundLotSize'),
                        df['Test Issue'].alias('testIssue'),
                        df['Financial Status'].alias('financialStatus'),
                        df['CQS Symbol'].alias('cqsSymbol'),
                        df['NASDAQ Symbol'].alias('nasdaqSymbol'),
                        df['NextShares'].alias('nextShares')
                        )
        return df2

    def load_symbol_raw(self, sym) -> DataFrame:
        csv_path = f"{self.config['raw_layer']}/{sym}.csv"
        df = self.spark.read.csv(csv_path, schema=schema_csv, header=True)

        return df

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
