from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import year, month, max, avg


def find_max(df: DataFrame, sym):
    winSpec = Window.partitionBy(year(df['Date']).alias('year')) \
        .orderBy(year(df['Date']))
    df2 = df.withColumn('maxClose', max(df['Close']).over(winSpec))
    df2.show(5)


# Simple Moving Average
def find_sma(df: DataFrame):
    winSpec = Window.partitionBy(year(df['Date']), month(df['Date'])) \
        .orderBy(df['Date']).rowsBetween(-4, Window.currentRow)
    df2 = df.withColumn('movingAvg', avg(df['close']).over(winSpec))
    df2.show(3)
