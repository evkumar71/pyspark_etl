from pyspark.sql import DataFrame, DataFrameWriter, DataFrameReader
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

    def __init__(self, ses, config=None):
        self.spark = ses
        self.config = config

    def load_symbol(self, symbol) -> DataFrame:
        df_meta = self.load_metadata()
        df_symbol = self.load_symbol_raw(symbol)
        df_meta.join(df_symbol)  # enrich symbol with long symbol name

        return df_meta

    def load_metadata(self):
        csv_path = f"{self.config['raw_layer']}/symbols_valid_meta.csv"
        df = self.spark.read.csv(csv_path, schema=DataStore.schema_meta, header=True)

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

    def load_symbol_raw(self, sym):
        csv_path = f"{self.config['raw_layer']}/{sym}.csv"
        df = self.spark.read.csv(csv_path, schema=DataStore.schema_csv, header=True)

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

