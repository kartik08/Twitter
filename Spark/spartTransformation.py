import pyspark
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
rawTopic = "Topic"
transformTopic = "TransformData"
def cleanData(df):
    print(df)


# if __name__ == "__main__":
print("Kartik")
spark = pyspark.sql.SparkSession.builder.master("local[*]").appName("Stream-twitter-data").getOrCreate()
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe", rawTopic).option("startingOffsets", "latest").load()
