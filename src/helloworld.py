from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName('aws test') \
    .getOrCreate()

df = spark.read.csv('s3://vijay-pyspark-etl/data/raw/ZUO.csv')

df.show(10)

