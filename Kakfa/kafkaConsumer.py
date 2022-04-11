from kafka import KafkaConsumer
import json
from Mongo import  mongo
TOPIC = "TransformData"
consumer = {}
client = mongo.mongoConnection()
db = mongo.createDB("Twitter",client)
collection = mongo.createCollection("TransformData",db)
#  connecting to kafka
print("connecting to kafka")
try:
    consumer = KafkaConsumer(TOPIC, bootstrap_servers='localhost:9092')
    for event in consumer:
        msg = event.value.decode('utf-8')
        tweets = json.loads(msg)
        collection.insert_one(tweets)
except Exception as e:
    print("could not connect",e)

else:
    print("connected")

