from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# create session
spark = SparkSession \
        .builder \
        .appName("streaming test")  \
        .getOrCreate()

# create streaming data-frame
df_lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

# read df
words = df_lines.select(
    explode(
        split(df_lines.value, " ")
    ).alias("word")
    )

wordCounts = words.groupBy("word").count()

query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

query.awaitTermination()