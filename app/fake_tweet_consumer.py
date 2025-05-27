# spark_stream_consumer.py
import os
os.environ["HADOOP_HOME"] = "C:/hadoop"
os.environ["hadoop.home.dir"] = "C:/hadoop"
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import joblib
from sklearn.naive_bayes import MultinomialNB
from sklearn.feature_extraction.text import CountVectorizer

# Modell & Vektorizer laden
model = joblib.load("../models/sentiment_model.pkl")
vectorizer = joblib.load("../models/vectorizer.pkl")

# Spark Streaming
sc = SparkContext("local[*]", "TweetStreamSocket")
ssc = StreamingContext(sc, 5)  # 5-Sekunden-Batch-Intervalle

# Checkpoint-Verzeichnis (wird für window benötigt)
ssc.checkpoint("checkpoint/")

lines = ssc.socketTextStream("localhost", 9999)

# 🔍 Sentimentanalyse auf Einzel-Tweets
def analyze_sentiment(rdd):
    tweets = rdd.collect()
    if tweets:
        X = vectorizer.transform(tweets)
        preds = model.predict(X)
        for tweet, pred in zip(tweets, preds):
            label = {0: "NEG", 1: "NEUTRAL", 2: "POS"}[pred]
            print(f"{label}: {tweet}")

lines.foreachRDD(analyze_sentiment)

# 🪟 NEU: Windowed Word Count
words = lines.flatMap(lambda line: line.lower().split())
word_pairs = words.map(lambda word: (word, 1))

# Window: 30 Sekunden Fenster, alle 10 Sekunden neu berechnen
windowed_word_counts = word_pairs.reduceByKeyAndWindow(
    func=lambda x, y: x + y,
    invFunc=lambda x, y: x - y,
    windowDuration=30,
    slideDuration=10
)

# Ausgabe der häufigsten Wörter im Fenster
def print_top_words(rdd):
    sorted_words = rdd.sortBy(lambda x: -x[1]).take(10)
    if sorted_words:
        print("🔠 Top-Wörter (letzte 30 Sek):")
        for word, count in sorted_words:
            print(f"  {word}: {count}")

windowed_word_counts.foreachRDD(print_top_words)

# Start Streaming
ssc.start()
ssc.awaitTermination()
