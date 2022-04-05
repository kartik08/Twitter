from kafka import KafkaConsumer

TOPIC = "Twitter"
consumer = {}

#  connecting to kafka
print("connecting to kafka")
try:
    consumer = KafkaConsumer(TOPIC, bootstrap_servers='localhost:9092')
    for event in consumer:
        print(event)
except Exception:
    print("could not connect")

else:
    print("connected")

