from pyspark.sql.types import *


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