# fake_tweet_producer.py
import socket
import time
import csv

HOST = 'localhost'
PORT = 9999

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Listening on {HOST}:{PORT}")
    conn, addr = s.accept()
    with conn:
        print(f"Connected by {addr}")
        with open('tweets_processed.csv', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                tweet_text = row[5]  # 6. Spalte
                conn.sendall((tweet_text + '\n').encode('utf-8'))
                print(f"Gesendet: {tweet_text}")
                time.sleep(2.5)
