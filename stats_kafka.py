from pyspark.sql import SparkSession

KAFKA_SERVER = 'localhost:9092'
STATS_TOPIC = 'statistics'

spark = SparkSession \
    .builder \
    .appName("P2 - PSPD - Stats Consumer") \
    .getOrCreate()

stats = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "statistics") \
    .load()

q = stats \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format('console') \
    .outputMode('append') \
    .start()