import os
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, current_timestamp

spark = SparkSession.builder.appName("WindowedAggregation").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Input: socket stream
lines = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Simuliere einen Zeitstempel (Processing Time)
lines = lines.withColumn("timestamp", current_timestamp())

# Windowed count by 1-minute intervals
windowed_counts = lines.groupBy(window("timestamp", "1 minute")).count()

query = windowed_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
