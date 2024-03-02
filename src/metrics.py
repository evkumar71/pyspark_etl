from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import year, month, max, avg
from appcontext import AppContext
from datastore import DataStore


class Metrics:
    def __init__(self, context):
        self.obj_ds = DataStore(context)

    def find_max(self, df: DataFrame):
        winSpec = Window.partitionBy(year(df['date']).alias('year')) \
            .orderBy(year(df['date']))
        df2 = df.withColumn('maxClose', max(df['close']).over(winSpec))
        df2.show(5)

    # Simple Moving Average
    def find_sma(self, df: DataFrame):
        winSpec = Window.partitionBy(year(df['date']), month(df['date'])) \
            .orderBy(df['date']).rowsBetween(-4, Window.currentRow)
        df2 = df.withColumn('movingAvg', avg(df['close']).over(winSpec))
        df2.show(3)


def main():
    context = AppContext("config/config.json")
    met = Metrics(context)
    df = met.obj_ds.load_symbol('ZUO')
    met.find_max(df)
    met.find_sma(df)

    context.spark.stop()


if __name__ == '__main__':
    main()
