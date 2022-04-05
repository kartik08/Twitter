from Kakfa.kafkaAdmin import checkTopic
import Twiiter.DataFetch
import Spark.spartTransformation
if __name__ == '__main__':
    print("Configuring the Kafka")
    try:
        rawData = checkTopic("Twitter")
        transformData = checkTopic("TransformData")
    except Exception as data:
        print(data)



