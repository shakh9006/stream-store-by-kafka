import json

from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest',
})

consumer.subscribe(['orders'])

print("Consumer running. Subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer failed: {msg.error()}")
            continue
        
        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(f"Received order: {order['quantity']} x {order['item']} by {order['username']}")

except KeyboardInterrupt:
    print("Consumer interrupted by user")
finally:
    consumer.close()
    print("Consumer closed")