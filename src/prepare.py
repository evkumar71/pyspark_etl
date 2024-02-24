from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from datastore import DataStore
from config import app_config


class Prepare():
    def __init__(self, spark):
        self.spark = spark
        self.config = app_config['data']
        self.symbols = symbols = ['ZUO', 'ZVO', 'ZYME', 'ZYNE', 'ZYXI']
        self.obj_ds = DataStore(spark=self.spark, config=self.config)

    def find_max(self,df: DataFrame, sym):
        winSpec = Window.partitionBy(year(df['Date']).alias('year')) \
                        .orderBy(year(df['Date']))
        df2 = df.withColumn('maxClose', max(df['Close']).over(winSpec))
        df2.show(5)

        # df.groupby(df['sym'], year(df['Date']).alias('year')) \
        #     .agg(min('Close').alias('minClose'), max('Close').alias('maxClose')) \
        #     .orderBy(col('year')) \
        #     .show(5)

    # Simple Moving Average
    def find_sma(self, df: DataFrame):
        winSpec = Window.partitionBy(year(df['Date']), month(df['Date'])) \
                        .orderBy(df['Date']).rowsBetween(-4,Window.currentRow)
        df2 = df.withColumn('movingAvg', avg(df['close']).over(winSpec))
        df2.show(3)

    def process_sym(self):
        obj_ds = self.obj_ds
        df_meta = obj_ds.load_metadata()
        df_meta.show(5)

        for sym in self.symbols:
            df_csv = obj_ds.load_symbol(sym)
            obj_ds.write_target(df_csv, sym)
            df_parq = obj_ds.read_target(sym)
            df_parq.show(2)

    def calc_avg(self):
        obj_ds = self.obj_ds

        for sym in self.symbols:
            df = obj_ds.load_symbol(sym)
            self.find_max(df, sym)
            # self.find_sma(df)


if __name__ == '__main__':
    spark_ses = SparkSession \
        .builder \
        .appName("spark etl") \
        .master("local[*]") \
        .getOrCreate()

    obj_pre = Prepare(spark_ses)
    obj_pre.process_sym()
    obj_pre.calc_avg()

    spark_ses.stop()
