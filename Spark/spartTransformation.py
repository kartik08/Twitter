from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from textblob import  TextBlob
from pyspark.sql.functions import *
from pyspark.sql.types import *
rawTopic = "Twitter"
transformTopic = "TransformData"
def clean_tweet(df):
    text_tweet = df.select(func.col("value").cast("string"))
    words = text_tweet.select(func.explode(func.split(func.col("value"), "t_end")).alias("word"))
    words = words.na.replace("", None)
    words = words.na.drop()
    words = words.withColumn("word", func.regexp_replace("word", r"http\S+", ""))
    words = words.withColumn("word", func.regexp_replace("word", "@\w+", ""))
    words = words.withColumn("word", func.regexp_replace("word", "#", ""))
    words = words.withColumn("word", func.regexp_replace("word", "RT", ""))
    words = words.withColumn("word", func.regexp_replace("word", ":", ""))
    return words
# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Stream-twitter-data").getOrCreate()

    # read tweet from kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", rawTopic) \
        .option("startingOffsets", "latest").load()

    # clean and read the data
    words_df = clean_tweet(df)

    # analyze text to define polarity and subjectivity
    word_sentiment = text_classification(words_df)

    word_json = word_sentiment.select(func.to_json(func.struct("word", "polarity", "subjectivity")).alias("value"))

    # write output to kafka
    query = word_json.writeStream \
        .format("Kafka") \
        .option("topic", transformTopic) \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("checkpointLocation", "checkpoint") \
        .option("startingOffsets", "latest") \
        .option("kafka.max.request.size", "10000000") \
        .option("kafka.message.max.bytes", "10000000") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

    spark.stop()







