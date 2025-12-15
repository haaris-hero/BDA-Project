import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import numpy as np

class RiderStreamGenerator:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.rider_ids = [f"RIDER{str(i).zfill(4)}" for i in range(1, 101)]
        self.zones = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
        self.rider_locations = {rid: (random.uniform(-74.0, -73.9), random.uniform(40.7, 40.8)) for rid in self.rider_ids}
        self.rider_idle_time = {rid: 0 for rid in self.rider_ids}
        
    def simulate_rider_event(self):
        rider_id = random.choice(self.rider_ids)
        zone = random.choice(self.zones)
        
        lat, lon = self.rider_locations[rider_id]
        lat += np.random.normal(0, 0.01)
        lon += np.random.normal(0, 0.01)
        self.rider_locations[rider_id] = (lat, lon)
        
        status_weights = [0.3, 0.5, 0.15, 0.05]
        rider_status = random.choices(["idle", "assigned", "pickup", "enroute"], weights=status_weights)[0]
        
        base_traffic = np.random.exponential(scale=5)
        traffic_delay = min(base_traffic, 30)
        
        pickup_delay = max(0, np.random.normal(traffic_delay * 0.6, 2))
        dropoff_delay = max(0, np.random.normal(traffic_delay * 0.8, 3))
        
        distance = np.random.exponential(scale=4)
        distance = min(distance, 15)
        
        hour = datetime.now().hour
        trip_count = int(np.random.poisson(lam=hour/2))
        
        if rider_status == "idle":
            self.rider_idle_time[rider_id] += 1
        else:
            self.rider_idle_time[rider_id] = 0
        
        event = {
            "rider_id": rider_id,
            "zone_id": zone,
            "rider_location": {"latitude": round(lat, 4), "longitude": round(lon, 4)},
            "rider_status": rider_status,
            "traffic_delay_minutes": round(float(traffic_delay), 2),
            "pickup_delay_minutes": round(float(pickup_delay), 2),
            "dropoff_delay_minutes": round(float(dropoff_delay), 2),
            "distance_to_restaurant_km": round(float(distance), 2),
            "trip_count_today": int(trip_count),
            "idle_time_minutes": int(self.rider_idle_time[rider_id]),
            "timestamp": datetime.now().isoformat()
        }
        return event
    
    def start_streaming(self, interval=2):
        print("🚴 Rider stream generator started...")
        try:
            while True:
                event = self.simulate_rider_event()
                self.producer.send('rider_stream', value=event)
                print(f"[RIDER] {event['rider_id']} sent to Kafka")
                time.sleep(random.uniform(1, interval))
        except KeyboardInterrupt:
            print("\n🛑 Rider stream generator stopped")
            self.producer.close()

if __name__ == "__main__":
    generator = RiderStreamGenerator(bootstrap_servers='kafka:9092')
    generator.start_streaming(interval=3)
