import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import numpy as np
import sys
import traceback

class KitchenStreamGenerator:
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

        self.restaurant_ids = [f"R{str(i).zfill(4)}" for i in range(1, 51)]
        self.chef_ids = [f"C{str(i).zfill(4)}" for i in range(1, 101)]
        self.restaurant_load = {rid: 0.3 for rid in self.restaurant_ids}

    def simulate_kitchen_event(self):
        try:
            restaurant_id = random.choice(self.restaurant_ids)
            chef_id = random.choice(self.chef_ids)
            order_id = f"ORD{int(time.time() * 1000)}{random.randint(100, 999)}"

            items_count = int(np.random.exponential(scale=3) + 1)
            items_count = min(items_count, 15)

            current_load = min(self.restaurant_load[restaurant_id] + np.random.normal(0, 0.1), 1.0)
            self.restaurant_load[restaurant_id] = max(0, current_load)

            base_prep_time = 5 + (items_count * 2)
            load_multiplier = 1 + (current_load * 0.5)

            hour = datetime.utcnow().hour
            is_peak = hour in [12, 13, 19, 20, 21]
            peak_multiplier = 1.3 if is_peak else 1.0

            prep_time = base_prep_time * load_multiplier * peak_multiplier
            prep_delay = max(0, np.random.normal(prep_time, prep_time * 0.2) - prep_time)

            prep_start = datetime.utcnow()
            prep_end = prep_start + timedelta(minutes=prep_time + prep_delay)

            event = {
                "order_id": order_id,
                "restaurant_id": restaurant_id,
                "chef_id": chef_id,
                "items_count": items_count,
                "prep_start_time": prep_start.isoformat(),
                "prep_end_time": prep_end.isoformat(),
                "prep_delay_minutes": round(float(prep_delay), 2),
                "predicted_prep_delay": round(float(prep_delay), 2),
                "priority_flag": random.choice([True, False]),
                "order_type": random.choice(["delivery", "dine-in"]),
                "event_time": datetime.utcnow().isoformat()   # ✅ CRITICAL FIX
            }

            return event

        except Exception as e:
            print(f"❌ Error simulating event: {e}")
            traceback.print_exc()
            return None

    def start_streaming(self, interval=6):
        print("🍳 Kitchen stream generator started")
        print("📤 Topic: kitchen_stream")

        count = 0
        try:
            while True:
                event = self.simulate_kitchen_event()
                if event:
                    self.producer.send("kitchen_stream", value=event)
                    self.producer.flush()
                    count += 1
                    print(f"[KITCHEN #{count}] {event['order_id']} sent")
                time.sleep(random.uniform(2, interval))

        except KeyboardInterrupt:
            print("\n🛑 Kitchen stream stopped")
            self.producer.close()
            sys.exit(0)
        except Exception as e:
            print(f"❌ Fatal error: {e}")
            traceback.print_exc()
            self.producer.close()
            sys.exit(1)

if __name__ == "__main__":
    print("🚀 Starting Kitchen Stream Generator...")
    generator = KitchenStreamGenerator(bootstrap_servers="kafka:9092")
    generator.start_streaming(interval=6)
