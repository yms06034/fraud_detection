from kafka import KafkaConsumer
import json

LEGIT_TOPIC = "legit_payments"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
consumer = KafkaConsumer(LEGIT_TOPIC, bootstrap_servers=brokers)

for message in consumer:
  msg = json.loads(message.value.decode())
  amount = msg["AMOUNT"]
  to = msg["TO"]

  if msg["PAYMENT_TYPE"] == "VISA":
    print(f"[VISA] fraud detecte payment to: {to} - {amount}")
  elif msg["PAYMENT_TYPE"] == "MASTERCARD":
    print(f"[MASTERCARD] fraud detecte payment to: {to} - {amount}")
  else:
    print("[ALERT] unable to process payments")