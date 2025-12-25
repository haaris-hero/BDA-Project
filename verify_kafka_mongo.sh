#!/bin/bash
# Verification script for Kafka and MongoDB
# Run this BEFORE starting streamers to ensure services are ready

# Don't exit on error for optional tests
set +e

echo "=========================================="
echo "🔍 Kafka & MongoDB Verification Script"
echo "=========================================="
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
    fi
}

# Function to print warning
print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

echo "STEP 1: Checking Docker containers..."
echo "-----------------------------------"
if docker-compose ps | grep -q "kafka.*Up" && docker-compose ps | grep -q "mongo.*Up"; then
    print_status 0 "Kafka and MongoDB containers are running"
    docker-compose ps | grep -E "(kafka|mongo|zookeeper)" | head -3
else
    print_status 1 "Kafka or MongoDB containers are not running"
    echo "Run: docker-compose up -d"
    exit 1
fi
echo ""

echo "STEP 2: Verifying Kafka connectivity..."
echo "-----------------------------------"
if docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server kafka:9092 > /dev/null 2>&1; then
    print_status 0 "Kafka broker is accessible"
else
    print_status 1 "Cannot connect to Kafka broker"
    echo "Check Kafka logs: docker-compose logs kafka"
    exit 1
fi
echo ""

echo "STEP 3: Checking Kafka topics..."
echo "-----------------------------------"
TOPICS=$(docker-compose exec -T kafka kafka-topics --list --bootstrap-server kafka:9092 2>/dev/null)
REQUIRED_TOPICS=("kitchen_stream" "rider_stream" "orders_stream")

for topic in "${REQUIRED_TOPICS[@]}"; do
    if echo "$TOPICS" | grep -q "^${topic}$"; then
        print_status 0 "Topic '$topic' exists"
    else
        print_warning "Topic '$topic' does not exist"
        echo "  Creating topic..."
        docker-compose exec -T kafka kafka-topics --create \
            --bootstrap-server kafka:9092 \
            --topic "$topic" \
            --partitions 3 \
            --replication-factor 1 \
            --if-not-exists > /dev/null 2>&1
        print_status $? "Topic '$topic' created"
    fi
done
echo ""

echo "STEP 4: Verifying MongoDB connectivity..."
echo "-----------------------------------"
if docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    print_status 0 "MongoDB is accessible"
else
    print_status 1 "Cannot connect to MongoDB"
    echo "Check MongoDB logs: docker logs mongo"
    exit 1
fi
echo ""

echo "STEP 5: Checking MongoDB database and collections..."
echo "-----------------------------------"
DB_CHECK=$(docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --quiet --eval "
    db = db.getSiblingDB('food_delivery');
    var collections = db.getCollectionNames();
    print('Collections: ' + collections.join(', '));
    print('Kitchen events: ' + db.kitchen_events.countDocuments());
    print('Rider events: ' + db.rider_events.countDocuments());
    print('Orders events: ' + db.orders_events.countDocuments());
" 2>&1)

if echo "$DB_CHECK" | grep -q "Collections:"; then
    print_status 0 "MongoDB database 'food_delivery' is accessible"
    echo "$DB_CHECK" | grep -E "(Collections|events:)"
    
    # Check if collections exist
    COLLECTIONS=$(echo "$DB_CHECK" | grep "Collections:" | cut -d: -f2 | tr ',' ' ')
    REQUIRED_COLLECTIONS=("kitchen_events" "rider_events" "orders_events")
    
    for col in "${REQUIRED_COLLECTIONS[@]}"; do
        if echo "$COLLECTIONS" | grep -q "$col"; then
            print_status 0 "Collection '$col' exists"
        else
            print_warning "Collection '$col' does not exist"
            echo "  Creating collection..."
            docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --quiet --eval "
                db = db.getSiblingDB('food_delivery');
                db.createCollection('$col');
                print('Collection $col created');
            " > /dev/null 2>&1
            print_status $? "Collection '$col' created"
        fi
    done
else
    print_status 1 "Cannot access MongoDB database 'food_delivery'"
    echo "Error: $DB_CHECK"
    exit 1
fi
echo ""

echo "STEP 6: Testing Kafka producer/consumer (optional)..."
echo "-----------------------------------"
read -p "Do you want to test producing and consuming a test message? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    TEST_TOPIC="test_verification_$(date +%s)"
    TEST_MESSAGE='{"test": "message", "timestamp": "'$(date -Iseconds)'"}'
    
    echo "Creating test topic: $TEST_TOPIC"
    docker-compose exec -T kafka kafka-topics --create \
        --bootstrap-server kafka:9092 \
        --topic "$TEST_TOPIC" \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists > /dev/null 2>&1
    
    echo "Producing test message..."
    echo "$TEST_MESSAGE" | docker-compose exec -T kafka kafka-console-producer \
        --bootstrap-server kafka:9092 \
        --topic "$TEST_TOPIC" > /dev/null 2>&1
    
    sleep 1
    
    echo "Consuming test message..."
    CONSUMED=$(timeout 3 docker-compose exec -T kafka kafka-console-consumer \
        --bootstrap-server kafka:9092 \
        --topic "$TEST_TOPIC" \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms 3000 2>/dev/null || true)
    
    if [ ! -z "$CONSUMED" ]; then
        print_status 0 "Kafka producer/consumer test successful"
    else
        print_status 1 "Kafka producer/consumer test failed"
    fi
    
    # Cleanup
    docker-compose exec -T kafka kafka-topics --delete \
        --bootstrap-server kafka:9092 \
        --topic "$TEST_TOPIC" > /dev/null 2>&1
fi
echo ""

echo "STEP 7: Testing MongoDB write/read (optional)..."
echo "-----------------------------------"
read -p "Do you want to test writing and reading from MongoDB? (y/n): " -n 1 -r
echo ""
if [[ $REPLY =~ ^[Yy]$ ]]; then
    TEST_DOC='{"test": "verification", "timestamp": "'$(date -Iseconds)'", "random": '$RANDOM'}'
    
    echo "Writing test document..."
    WRITE_RESULT=$(docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --quiet --eval "
        db = db.getSiblingDB('food_delivery');
        result = db.test_verification.insertOne($TEST_DOC);
        print('Inserted ID: ' + result.insertedId);
    " 2>&1)
    
    if echo "$WRITE_RESULT" | grep -q "Inserted ID"; then
        print_status 0 "MongoDB write test successful"
        
        echo "Reading test document..."
        READ_RESULT=$(docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --quiet --eval "
            db = db.getSiblingDB('food_delivery');
            doc = db.test_verification.findOne();
            printjson(doc);
        " 2>&1)
        
        if echo "$READ_RESULT" | grep -q "test"; then
            print_status 0 "MongoDB read test successful"
        else
            print_status 1 "MongoDB read test failed"
        fi
        
        # Cleanup
        docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --quiet --eval "
            db = db.getSiblingDB('food_delivery');
            db.test_verification.drop();
        " > /dev/null 2>&1
    else
        print_status 1 "MongoDB write test failed"
        echo "Error: $WRITE_RESULT"
    fi
fi
echo ""

echo "=========================================="
echo "📊 Summary"
echo "=========================================="
echo ""
echo "Kafka Status:"
docker-compose exec -T kafka kafka-topics --list --bootstrap-server kafka:9092 | sed 's/^/  - /'
echo ""
echo "MongoDB Collections:"
docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --quiet --eval "
    db = db.getSiblingDB('food_delivery');
    collections = db.getCollectionNames();
    collections.forEach(function(c) { print('  - ' + c + ': ' + db[c].countDocuments({}) + ' documents'); });
" 2>/dev/null | grep -E "^\s+-" || echo "  (No collections found or error reading)"
echo ""

echo "=========================================="
echo -e "${GREEN}✅ Verification Complete!${NC}"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Start streamers in 3 separate terminals:"
echo "   Terminal 1: docker run -it --rm --network food-delivery-pipeline_food-delivery-net -v \$(pwd)/scripts:/scripts python:3.9-slim bash"
echo "   Then inside container: pip install kafka-python numpy && python /scripts/generate_orders_stream.py"
echo ""
echo "2. Monitor Kafka messages:"
echo "   docker-compose exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic orders_stream --from-beginning"
echo ""
echo "3. Monitor MongoDB data:"
echo "   docker exec mongo mongo -u root -p password123 --authenticationDatabase admin --eval \"db = db.getSiblingDB('food_delivery'); print('Orders: ' + db.orders_events.countDocuments());\""
echo ""

