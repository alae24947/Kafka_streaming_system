from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Products list
products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor"]

###########################################validation
sent_count = 0        # Total messages sent
all_orders = []       # Store messages for later comparison
filtered_count = 0    # Count orders with price > 1000 for validation


while True:
    # Generate random order
    order = {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "product": random.choice(products),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(50, 2000), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

    # Send order to Kafka topic 'orders'
    producer.send("orders", order)

    # Update counters for validation
    sent_count += 1
    all_orders.append(order)
    if order['price'] > 1000:
        filtered_count += 1

    # Print order and validation stats
    print(f"Sent ({sent_count}) | Filtered (>1000€): {filtered_count}")
    print(order)

    # Wait 1 second before sending next order no crach
    time.sleep(1)
