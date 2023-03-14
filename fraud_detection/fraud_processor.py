from kafka import KafkaConsumer
import json

FRAUD_TOPIC = "fraud_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(FRAUD_TOPIC, bootstrap_servers=brokers)

for message in consumer:
  msg = json.loads(message.value.decode())
  amount = msg["AMOUNT"]
  to = msg["TO"]

  if msg['TO'] == "stranger":
    print(f"[ALERT] fraud detecte payment to: {to} - {amount}")

  else:
    print(f"[PROCESSING BITCOIN] payment to: {to} - {amount}")