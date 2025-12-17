from pymongo import MongoClient
import matplotlib.pyplot as plt
from collections import Counter
import statistics

client = MongoClient("mongodb://localhost:27017/")
db = client["ecommerce_db"]

orders_col = db["large_orders"]
stats_col = db["stats"]
invalid_col = db["invalid_orders"]

orders = list(orders_col.find())

# Orders per product

product_counts = Counter([o["product"] for o in orders])

plt.figure()
plt.bar(product_counts.keys(), product_counts.values())
plt.title("Large Orders per Product")
plt.xlabel("Product")
plt.ylabel("Number of Orders")
plt.show()


#  Histogram: Price distribution

prices = [o["price"] for o in orders]

plt.figure()
plt.hist(prices, bins=10)
plt.title("Price Distribution (>1000)")
plt.xlabel("Price")
plt.ylabel("Frequency")
plt.show()


#  Price spread

plt.figure()
plt.boxplot(prices, vert=False)
plt.title("Price Dispersion of Large Orders")
plt.xlabel("Price")
plt.show()


#  Average price over time

stats = list(stats_col.find({"timestamp": {"$exists": True}}).sort("timestamp", 1))

timestamps = [s["timestamp"] for s in stats]
avg_prices = [s["average_price"] for s in stats]

plt.figure()
plt.plot(timestamps, avg_prices)
plt.title("Average Price Evolution Over Time")
plt.xlabel("Time")
plt.ylabel("Average Price")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()


# Valid vs Invalid orders

valid_count = orders_col.count_documents({})
invalid_count = invalid_col.count_documents({})

plt.figure()
plt.pie(
    [valid_count, invalid_count],
    labels=["Valid Orders", "Invalid Orders"],
    autopct="%1.1f%%"
)
plt.title("Data Validation Results")
plt.show()
