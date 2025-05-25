from kafka import KafkaProducer
import time
import csv

producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic = 'twitter-stream'

with open('../tweets_processed.csv', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader)
    for row in reader:
        tweet_text = row[5]  # Text  in Spalte 6
        producer.send(topic, tweet_text.encode('utf-8'))
        print(f"Gesendet: {tweet_text}")
        time.sleep(2.5)
