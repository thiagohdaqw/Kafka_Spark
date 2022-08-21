from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, lit, col, upper

SOCKET_HOST = "localhost"
SOCKET_PORT = "9999"

spark = SparkSession \
    .builder \
    .appName("P2 - PSPD - Socket") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines from connection to SOCKET_SERVER
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", SOCKET_HOST) \
    .option("port", SOCKET_PORT) \
    .load()

# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, "\s+")
    ).alias('word')
)
words = words.select(upper(words.word).alias('word'))

# Generate running word count
wordCounts = words.groupBy("word").count()

def foreach_batch_func(df, _):
    """Find the total number of words and write it and wordsCount in console"""
    total = df \
        .groupBy() \
        .sum() \
        .select(lit('TOTAL').alias('key'), col('sum(count)').alias('value'))

    df.write.format('console').save()
    total.write.format('console').save()

# Sink
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(foreach_batch_func) \
    .start()

query.awaitTermination()