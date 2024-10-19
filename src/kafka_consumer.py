from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import col, year

# Run instruction
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 src/kafka_consumer.py

# create spark session
spark = SparkSession \
        .builder \
        .appName("kafka producer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

# create stream-reader DF to read from topic
df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test-topic-1") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(col('key').cast(StringType()), col('value').cast(StringType()))

# create
query = df.writeStream \
        .format("console") \
        .start()

query.awaitTermination()