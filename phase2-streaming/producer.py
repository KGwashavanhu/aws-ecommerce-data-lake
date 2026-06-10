import boto3
import json
import random
import time
from datetime import datetime

# This script simulates an ecommerce store generating orders in real time
# It sends fake order events into our Kinesis Data Stream one by one

# Connect to Kinesis in the same region as our stream
client = boto3.client('kinesis', region_name='us-east-1')

# Sample data to randomly pick from when generating fake orders
PRODUCTS = ['laptop', 'phone', 'headphones', 'keyboard', 'monitor', 'mouse']
CUSTOMERS = ['cust_001', 'cust_002', 'cust_003', 'cust_004', 'cust_005']
STATUSES = ['placed', 'processing', 'shipped']

def generate_order():
    """
    Generates a single fake order event as a dictionary.
    Each call produces a different random order.
    """
    return {
        'order_id': f"ORD-{random.randint(10000, 99999)}",
        'customer_id': random.choice(CUSTOMERS),
        'product': random.choice(PRODUCTS),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10.0, 500.0), 2),
        'status': random.choice(STATUSES),
        'timestamp': datetime.utcnow().isoformat()
    }

def send_to_kinesis(order):
    """
    Sends a single order event to our Kinesis Data Stream.
    
    Data must be serialized to a string (JSON) before sending.
    PartitionKey determines which shard the record goes to —
    using order_id spreads records evenly across shards.
    """
    response = client.put_record(
        StreamName='kuda-ecommerce-stream',
        Data=json.dumps(order),         # Convert dict to JSON string
        PartitionKey=order['order_id']  # Used to route to a shard
    )
    return response

if __name__ == "__main__":
    print("Starting order stream... Press Ctrl+C to stop.\n")
    
    order_count = 0
    
    # Keep sending orders every second until we manually stop it
    while True:
        order = generate_order()
        response = send_to_kinesis(order)
        order_count += 1
        print(f"Order {order_count} sent: {order['order_id']} | {order['product']} | ${order['price']}")
        time.sleep(1)  # Wait 1 second between orders to simulate real traffic