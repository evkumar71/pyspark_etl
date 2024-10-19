from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

# Run instruction
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/kafka_producer.py

# file path
pq_path = "data/derived/ZUO.parquet"

# create spark session
spark = SparkSession \
        .builder \
        .appName("kafka producer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

# derive file schema
schema = spark.read.parquet(pq_path).schema

# create read stream
df = spark.readStream \
        .schema(schema) \
        .parquet(pq_path) \

# create write stream using only 2 fields from file
# stream reads parquet file and writes to topic simulating producer
query = df \
        .select(col('date').cast(StringType()).alias("key"), col('close').cast(StringType()).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("checkPointLocation", "/tmp/spark/checkpoint") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "test-topic-1") \
        .start()

query.awaitTermination()
