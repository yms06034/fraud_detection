from kafka import KafkaConsumer

brokers = ["localhost:9091","localhost:9092","localhost:9093"]
topicName = "first-cluster-topic"

consumer = KafkaConsumer(topicName, bootstrap_servers=brokers)

for message in consumer:
    print(message)

