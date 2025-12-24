import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import numpy as np
import sys
import traceback

class OrdersStreamGenerator:
    def __init__(self, bootstrap_servers="kafka:9092"):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                linger_ms=50,
                retries=3
            )
            print(f"✅ Connected to Kafka at {bootstrap_servers}")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            sys.exit(1)

        self.customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, 1001)]
        self.restaurant_ids = [f"R{str(i).zfill(4)}" for i in range(1, 51)]
        self.zones = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
        self.food_categories = ["Biryani", "Pizza", "Burger", "Chinese", "Italian", "Dessert"]

    def simulate_order_event(self):
        try:
            order_id = f"ORD{int(time.time() * 1000)}{random.randint(100, 999)}"
            customer_id = random.choice(self.customer_ids)
            restaurant_id = random.choice(self.restaurant_ids)
            zone_id = random.choice(self.zones)

            order_value = np.random.exponential(scale=400) + 200
            order_value = min(order_value, 2000)

            discount_amount = order_value * 0.15 if random.random() < 0.3 else 0

            payment_type = random.choices(
                ["credit_card", "debit_card", "upi", "wallet"],
                weights=[0.3, 0.3, 0.3, 0.1]
            )[0]

            estimated_delivery = min(np.random.exponential(scale=20) + 20, 60)
            actual_delivery = max(
                estimated_delivery * 0.8,
                estimated_delivery + np.random.normal(0, 5)
            )

            delivery_delay = actual_delivery - estimated_delivery

            cancel_threshold = estimated_delivery * 1.5
            cancellation_prob = min(
                0.7,
                (delivery_delay / cancel_threshold)
            ) if delivery_delay > 0 else 0.05

            is_delivered = random.random() > cancellation_prob
            customer_rating = round(random.uniform(3.5, 5.0), 1) if is_delivered else None

            event = {
                "order_id": order_id,
                "customer_id": customer_id,
                "restaurant_id": restaurant_id,
                "zone_id": zone_id,
                "food_category": random.choice(self.food_categories),
                "order_value": round(float(order_value), 2),
                "discount_amount": round(float(discount_amount), 2),
                "final_amount": round(float(order_value - discount_amount), 2),
                "payment_type": payment_type,
                "estimated_delivery_minutes": round(float(estimated_delivery), 2),
                "actual_delivery_minutes": round(float(actual_delivery), 2),
                "delivery_delay_minutes": round(float(delivery_delay), 2),
                "cancellation_probability": round(float(cancellation_prob), 3),
                "is_cancelled": not is_delivered,
                "customer_rating": customer_rating,
                "event_time": datetime.utcnow().isoformat()   # ✅ FIX
            }

            return event

        except Exception as e:
            print(f"❌ Error generating order event: {e}")
            traceback.print_exc()
            return None

    def start_streaming(self, interval=5):
        print("📦 Orders stream generator started")
        print("📤 Topic: orders_stream")

        count = 0
        try:
            while True:
                event = self.simulate_order_event()
                if event:
                    self.producer.send("orders_stream", value=event)
                    self.producer.flush()
                    count += 1
                    print(f"[ORDER #{count}] {event['order_id']} sent")
                time.sleep(random.uniform(2, interval))

        except KeyboardInterrupt:
            print("\n🛑 Orders stream stopped")
            self.producer.close()
            sys.exit(0)
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            traceback.print_exc()
            self.producer.close()
            sys.exit(1)

if __name__ == "__main__":
    print("🚀 Starting Orders Stream Generator...")
    generator = OrdersStreamGenerator(bootstrap_servers="kafka:9092")
    generator.start_streaming(interval=5)
