from pyspark.sql import SparkSession
from prepare import Prepare


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
