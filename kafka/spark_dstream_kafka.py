from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Spark Context und StreamingContext (Batch-Intervall: 2 Sekunden)
sc = SparkContext(appName="TwitterDStreamKafka")
ssc = StreamingContext(sc, 2)

# Kafka Stream (alte API – basiert auf RDDs)
kafkaStream = KafkaUtils.createDirectStream(
    ssc,
    topics=['twitter-stream'],
    kafkaParams={"metadata.broker.list": "localhost:9092"}
)

# Extrahiere den Tweet-Text (value)
lines = kafkaStream.map(lambda msg: msg[1])

# Beispiel: Tweet-Längen berechnen und ausgeben
tweet_lengths = lines.map(lambda tweet: (tweet, len(tweet)))
tweet_lengths.pprint()

# Streaming starten
ssc.start()
ssc.awaitTermination()
