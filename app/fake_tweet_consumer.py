# spark_stream_consumer.py
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[*]", "TweetStreamSocket")
ssc = StreamingContext(sc, 5)

lines = ssc.socketTextStream("localhost", 9999)

# Beispielverarbeitung: Ausgabe der empfangenen Tweets
lines.pprint()

ssc.start()
ssc.awaitTermination()
