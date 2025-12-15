db = db.getSiblingDB('food_delivery');

// Kitchen Events Collection
db.createCollection('kitchen_events');
db.kitchen_events.createIndex({ "order_id": 1 }, { unique: true });
db.kitchen_events.createIndex({ "restaurant_id": 1 });
db.kitchen_events.createIndex({ "timestamp": 1 });
db.kitchen_events.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 3600 });

// Rider Events Collection
db.createCollection('rider_events');
db.rider_events.createIndex({ "rider_id": 1 });
db.rider_events.createIndex({ "zone_id": 1 });
db.rider_events.createIndex({ "timestamp": 1 });
db.rider_events.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 3600 });

// Orders Events Collection
db.createCollection('orders_events');
db.orders_events.createIndex({ "order_id": 1 }, { unique: true });
db.orders_events.createIndex({ "customer_id": 1 });
db.orders_events.createIndex({ "restaurant_id": 1 });
db.orders_events.createIndex({ "zone_id": 1 });
db.orders_events.createIndex({ "timestamp": 1 });
db.orders_events.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 3600 });

// Analytics KPI Collection
db.createCollection('analytics_kpis');
db.analytics_kpis.createIndex({ "computed_at": 1 });

// Metadata for archiving
db.createCollection('archive_metadata');
db.archive_metadata.createIndex({ "archived_at": 1 });

// Dimension Tables
db.createCollection('dim_customer');
db.dim_customer.createIndex({ "customer_id": 1 }, { unique: true });

db.createCollection('dim_restaurant');
db.dim_restaurant.createIndex({ "restaurant_id": 1 }, { unique: true });

db.createCollection('dim_rider');
db.dim_rider.createIndex({ "rider_id": 1 }, { unique: true });

db.createCollection('dim_zone');
db.dim_zone.createIndex({ "zone_id": 1 }, { unique: true });

db.createCollection('dim_time');
db.createCollection('dim_food_category');

print("Food Delivery Database initialized successfully");
