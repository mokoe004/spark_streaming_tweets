import os
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, window

spark = SparkSession.builder.appName("EventTimeWatermark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Beispiel-Datenstream mit Event-Time im JSON-Format (z. B. aus Datei/Kafka)
# Inhalt der Datei: {"event_time": "2025-05-26T10:00:00", "value": "some tweet"}
schema = "event_time TIMESTAMP, value STRING"

df = spark.readStream.schema(schema).json("event_stream_input/")

# Event-Time Aggregation mit Watermark (2 Minuten Verzögerung erlaubt)
aggregated = df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window("event_time", "1 minute")) \
    .count()

query = aggregated.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
