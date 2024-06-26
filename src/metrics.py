from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import year, month, max, avg
from appcontext import AppContext
from datastore import DataStore
import sys


class Metrics:
    def __init__(self, context):
        self.obj_ds = DataStore(context)

    def highest_close(self, df: DataFrame):
        df = df.withColumn('year', year('date'))
        winSpec = Window.partitionBy('year').orderBy('year')
        df2 = df.withColumn('maxClose', max(df['close']).over(winSpec))
        df3 = df2.filter("close == maxClose")
        df4 = df3.limit(3).select('date', 'maxClose')
        df4.show()
        return df4

    # Simple Moving Average
    def find_sma(self, df: DataFrame):
        winSpec = Window.partitionBy(year(df['date']), month(df['date'])) \
            .orderBy(df['date']).rowsBetween(-4, Window.currentRow)
        df2 = df.withColumn('movingAvg', avg(df['close']).over(winSpec))
        df2.show(3)


def main():
    if len(sys.argv) > 1:
        context = AppContext(sys.argv[1])
    else:
        context = AppContext("config/config.json")

    met = Metrics(context)
    df = met.obj_ds.load_symbol('ZUO')
    met.highest_close(df)
    met.find_sma(df)

    context.spark.stop()


if __name__ == '__main__':
    main()
