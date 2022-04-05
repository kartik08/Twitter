from kafka.admin import KafkaAdminClient,NewTopic

client = KafkaAdminClient(bootstrap_servers = "localhost:9092")

def checkTopic(name):
    topic = []
    topic.append(NewTopic(name=name, num_partitions=1, replication_factor=1))
    if name in client.list_topics():
        print("Topic is Already Present.")
        return True
    else:
        print("Creating Topic")
        client.create_topics(topic,validate_only=False)
        return True
    raise Exception("Topic not Created")
