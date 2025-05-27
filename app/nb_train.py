# sentiment_model_trainer.py
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
import joblib

# Laden & vorbereiten
df = pd.read_csv("../tweets_processed.csv", encoding='latin-1', header=None)
df = df[[0, 5]]
df.columns = ["sentiment", "text"]
df["sentiment"] = df["sentiment"].replace({0: 0, 2: 1, 4: 2})  # 0=neg, 1=neutral, 2=pos

# Vektorisierung
vectorizer = CountVectorizer(stop_words='english')
X = vectorizer.fit_transform(df["text"])
y = df["sentiment"]

# Modell trainieren
model = MultinomialNB()
model.fit(X, y)

# Speichern
joblib.dump(model, "../models/sentiment_model.pkl")
joblib.dump(vectorizer, "../models/vectorizer.pkl")
