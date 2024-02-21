from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datastore import DataStore
from config import app_config


class Prepare():
    def __init__(self, ses):
        self.spark = ses
        self.config = app_config['data']
        self.symbols = symbols = ['ZUO', 'ZVO', 'ZYME', 'ZYNE', 'ZYXI']

    def find_max(self,df: DataFrame, sym):
        df2 = df.withColumn('sym', lit(sym))
        df2.groupby(df2['sym'], year(df2['Date']).alias('year')) \
            .agg(min('Close').alias('minClose'), max('Close').alias('maxClose')) \
            .orderBy(col('year')) \
            .show(5)

    # Simple Moving Average
    def find_sma(self, df: DataFrame):
        df.groupby(year(df['Date']).alias('year'), month(df['Date']).alias('month')) \
            .agg(avg('Close').alias('simple moving avg')) \
            .orderBy('year', 'month') \
            .show(5)

    def process_sym(self):
        cls = DataStore(ses=cls_pre.spark, config=cls_pre.config)
        df_meta = cls.load_metadata()
        df_meta.show(5)

        for sym in self.symbols:
            df_csv = cls.load_symbol_raw(sym)
            cls.write_target(df_csv, sym)
            df_parq = cls.read_target(sym)
            df_new = df_parq.join(df_meta, df_meta['nasdaqSymbol'] == sym) \
                .select(df_parq['*'], df_meta['nasdaqSymbol'], df_meta['securityName'])
            df_new.show(2)

    def calc_avg(self):
        cls = DataStore(ses=cls_pre.spark, config=cls_pre.config)

        for sym in self.symbols:
            df = cls.load_symbol_raw(sym)
            self.find_max(df, sym)
            self.find_sma(df)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("spark etl") \
        .master("local[*]") \
        .getOrCreate()

    cls_pre = Prepare(spark)
    cls_pre.process_sym()
    cls_pre.calc_avg()

    spark.stop()
