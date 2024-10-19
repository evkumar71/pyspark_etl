from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro, to_avro

avro_schema = '{"type":"record","name":"namespace","fields":[{"name":"date","type":[{"type":"int","logicalType":"date"},"null"]},{"name":"open","type":["double","null"]},{"name":"close","type":["double","null"]}]}'

spark = SparkSession.builder \
    .appName("kafka-consumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
    .config(" spark.sql.streaming.forceDeleteTempCheckpointLocation", True) \
    .master("local[*]") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "avro-test-topic-2") \
    .option("checkpointLocation", "/tmp/spark/checkpoint") \
    .load() \
    .withColumn("value_from_avro", from_avro("value", avro_schema)) \
    .select("value_from_avro.*")

    # .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = df.writeStream.format("console").start()
query.awaitTermination()
