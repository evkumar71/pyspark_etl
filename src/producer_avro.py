from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.avro.functions import from_avro, to_avro


spark = SparkSession.builder \
    .appName("kafka-producer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0") \
    .master("local[*]") \
    .getOrCreate()

aapl_df = spark.read.parquet("data/drv/stocks/AAPL")
aapl = spark.readStream.schema(aapl_df.schema).parquet("data/drv/stocks/AAPL") \
    .select('date', 'open', 'close')
print(aapl.isStreaming)
print(spark._jvm.org.apache.spark.sql.avro.SchemaConverters.toAvroType(aapl._jdf.schema(), False, 'namespace', None))

query = aapl \
    .select(col("date").alias("key"), struct(col("date"), col("open"), col("close")).alias("value")) \
    .select(to_avro(col("key")).alias("key"), to_avro(col("value")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "avro-test-topic-2") \
    .option("checkpointLocation", "/tmp/spark/checkpoint") \
    .start()

query.awaitTermination()
