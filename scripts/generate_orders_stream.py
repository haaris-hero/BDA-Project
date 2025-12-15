import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import numpy as np

class OrdersStreamGenerator:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, 1001)]
        self.restaurant_ids = [f"R{str(i).zfill(4)}" for i in range(1, 51)]
        self.zones = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
        self.food_categories = ["Biryani", "Pizza", "Burger", "Chinese", "Italian", "Dessert"]
        
    def simulate_order_event(self):
        order_id = f"ORD{int(time.time()*1000)}{random.randint(100, 999)}"
        customer_id = random.choice(self.customer_ids)
        restaurant_id = random.choice(self.restaurant_ids)
        zone_id = random.choice(self.zones)
        
        order_value = np.random.exponential(scale=400) + 200
        order_value = min(order_value, 2000)
        
        discount_prob = 0.3
        discount_amount = order_value * 0.15 if random.random() < discount_prob else 0
        
        payment_type = random.choices(["credit_card", "debit_card", "upi", "wallet"], weights=[0.3, 0.3, 0.3, 0.1])[0]
        
        base_estimate = np.random.exponential(scale=20) + 20
        estimated_delivery = min(base_estimate, 60)
        
        actual_delivery = estimated_delivery + np.random.normal(0, 5)
        actual_delivery = max(actual_delivery, estimated_delivery * 0.8)
        
        delivery_delay = actual_delivery - estimated_delivery
        
        cancel_threshold = estimated_delivery * 1.5
        cancellation_prob = min(0.7, (delivery_delay / cancel_threshold)) if delivery_delay > 0 else 0.05
        
        food_category = random.choice(self.food_categories)
        
        is_delivered = random.random() > cancellation_prob
        customer_rating = round(random.uniform(3.5, 5.0), 1) if is_delivered else None
        
        event = {
            "order_id": order_id,
            "customer_id": customer_id,
            "restaurant_id": restaurant_id,
            "zone_id": zone_id,
            "food_category": food_category,
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
            "timestamp": datetime.now().isoformat()
        }
        return event
    
    def start_streaming(self, interval=4):
        print("📦 Orders stream generator started...")
        try:
            while True:
                event = self.simulate_order_event()
                self.producer.send('orders_stream', value=event)
                print(f"[ORDER] {event['order_id']} sent to Kafka")
                time.sleep(random.uniform(2, interval))
        except KeyboardInterrupt:
            print("\n🛑 Orders stream generator stopped")
            self.producer.close()

if __name__ == "__main__":
    generator = OrdersStreamGenerator(bootstrap_servers='kafka:9092')
    generator.start_streaming(interval=5)
