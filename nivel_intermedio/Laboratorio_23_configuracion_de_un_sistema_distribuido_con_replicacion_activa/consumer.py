from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "transactions",
    bootstrap_servers=["localhost:9092", "localhost:9093", "localhost:9094"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="transaction_group",
    auto_offset_reset="earliest",
)

print("Esperando mensajes... (Ctrl+C para salir)")
for msg in consumer:
    print(f"Procesado: {msg.value}")
