from pyspark.sql import SparkSession
from pyspark.sql.functions import length, explode, split, substring, upper, window

INTERVAL = '10 seconds'
KAFKA_SERVER = 'localhost:9092'
WORDS_TOPIC = 'wc'
STATS_TOPIC = 'statistics'

spark = SparkSession \
    .builder \
    .appName("P2 - PSPD - Transformer") \
    .getOrCreate()

# Create DataFrame representing the stream of input lines and subscribe it in kafka topic
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", WORDS_TOPIC) \
    .option('includeTimestamp', 'true') \
    .load()

# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, r"\s+")).alias("word"),
        lines.timestamp
    )

# Group words
wordCounts = words.groupBy("word").count()

# Count the total of words readed
total = words \
    .groupBy() \
    .count() \
    .selectExpr("'TOTAL' as key", "CAST(count AS STRING) as value")

# Count the words that startswith S, P and R
letters = words \
    .filter(upper(substring(words.word, 0, 1)).isin(["S", "P", "R"])) \
    .withWatermark("timestamp", INTERVAL) \
    .groupBy(
        window(words.timestamp, INTERVAL, INTERVAL),
        upper(substring(words.word, 0, 1)).alias("key"),
    ) \
    .count() \
    .selectExpr("key", "CAST(count AS STRING) as value")

# Count the words that has length 6, 8 and 11
lengths = words \
    .filter(length(words.word).isin([6, 8, 11])) \
    .withWatermark("timestamp", INTERVAL) \
    .groupBy(
        window(words.timestamp, INTERVAL, INTERVAL),
        length(words.word).alias("key")
    ) \
    .count() \
    .selectExpr("CAST(key AS STRING)", "CAST(count AS STRING) as value")

# Sinks
qW = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

qT = total \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option('topic', STATS_TOPIC) \
    .option('checkpointLocation', '/tmp/spark/total-stats') \
    .trigger(processingTime=INTERVAL) \
    .start()

qLen = lengths \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option('topic', STATS_TOPIC) \
    .option('checkpointLocation', '/tmp/spark/len-stats') \
    .trigger(processingTime=INTERVAL) \
    .start()

qLet = letters \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option('topic', STATS_TOPIC) \
    .option('checkpointLocation', '/tmp/spark/let-stats') \
    .trigger(processingTime=INTERVAL) \
    .start()