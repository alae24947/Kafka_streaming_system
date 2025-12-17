from kafka import KafkaConsumer
import json
from pymongo import MongoClient
from datetime import datetime, timezone

# Kafka Consumer
consumer = KafkaConsumer(
    "orders",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_db"]
orders_col = db["large_orders"]
stats_col = db["stats"]
invalid_col = db["invalid_orders"]

# Aggregation variables
count = 0
total_price = 0
min_price = float("inf")
max_price = 0

REQUIRED_FIELDS = ["order_id", "product", "price", "quantity", "timestamp"]

def is_valid_order(order):
    # Check required fields
    for field in REQUIRED_FIELDS:
        if field not in order:
            return False

    # Type validation
    if not isinstance(order["price"], (int, float)):
        return False
    if not isinstance(order["quantity"], int):
        return False

    # Value validation
    if order["price"] <= 0 or order["price"] > 100000:
        return False
    if order["quantity"] <= 0:
        return False

    # Timestamp validation
    try:
        datetime.fromisoformat(order["timestamp"])
    except ValueError:
        return False

    return True


for msg in consumer:
    order = msg.value
    print("Received:", order)

    # Validation
    if not is_valid_order(order):
        print("Invalid order received")
        invalid_col.insert_one({
            "order": order,
            "error": "Invalid data",
            "received_at": datetime.now(timezone.utc)
        })
        continue

    # Filtering rule
    if order["price"] > 1000:
        orders_col.insert_one(order)

        # Aggregations
        count += 1
        total_price += order["price"]
        min_price = min(min_price, order["price"])
        max_price = max(max_price, order["price"])

        avg_price = total_price / count

        stats = {
            "timestamp": datetime.now(timezone.utc),
            "count": count,
            "average_price": round(avg_price, 2),
            "min_price": min_price,
            "max_price": max_price
        }

        stats_col.insert_one(stats)
        print("Stats updated:", stats)
    else:
        print("Order ignored (price <= 1000)")
