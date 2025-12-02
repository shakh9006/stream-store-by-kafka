from confluent_kafka import Producer
import uuid
import json

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered message: {msg.value().decode('utf-8')}")
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

order = {
    "order_id": str(uuid.uuid4()),
    "username": "john_doe",
    "item": "mashroom pizza",
    "quantity": 2,
}

value = json.dumps(order).encode('utf-8')

producer.produce(
    topic="orders",
    value=value,
    callback=delivery_report
)

producer.flush()