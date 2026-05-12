from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_transaction():
    return {
        "transaction_id": random.randint(1000, 9999),
        "account_id": random.randint(10000, 99999),
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "timestamp": time.time(),
    }


print("Enviando 10 transacciones...")
for _ in range(10):
    txn = generate_transaction()
    producer.send("transactions", value=txn)
    print(f"Enviado: {txn}")
    time.sleep(1)

producer.close()
