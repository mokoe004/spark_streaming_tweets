from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("StreamStreamJoin").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Simuliere Tweets: user_id,tweet
tweets_schema = "user_id STRING, tweet STRING"
tweets = spark.readStream \
    .option("sep", ",") \
    .schema(tweets_schema) \
    .csv("tweets_input/")  # Ordner mit CSV-Dateien

# Simuliere Userinfos: user_id,country
users_schema = "user_id STRING, country STRING"
users = spark.readStream \
    .option("sep", ",") \
    .schema(users_schema) \
    .csv("user_info_input/")  # Ordner mit User-Updates

# Join beider Streams
joined = tweets.join(users, "user_id")

query = joined.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
