#!/bin/bash
# Simple Redis Test Script

echo "=== Redis Cache Test ==="
echo ""

# Check Redis status
echo "1. Redis Status:"
if docker exec redis redis-cli PING > /dev/null 2>&1; then
    echo "   ✅ Redis is running"
else
    echo "   ❌ Redis is not running"
    exit 1
fi

echo ""

# Check for keys
echo "2. Redis Keys:"
KEYS=$(docker exec redis redis-cli KEYS "*" 2>/dev/null)
if [ -z "$KEYS" ]; then
    echo "   ⚠️  No keys found"
    echo ""
    echo "   To populate Redis cache:"
    echo "   1. Trigger Spark job: docker exec airflow-scheduler airflow dags trigger spark_compute_kpis"
    echo "   2. Wait 30-60 seconds"
    echo "   3. Run this script again"
else
    echo "   ✅ Found keys:"
    echo "$KEYS" | while read key; do
        if [ ! -z "$key" ]; then
            TTL=$(docker exec redis redis-cli TTL "$key" 2>/dev/null)
            echo "      - $key (expires in ${TTL}s)"
        fi
    done
fi

echo ""

# Check specific KPI keys
echo "3. KPI Cache Status:"
for key in "kpi:kitchen_load" "kpi:rider_efficiency" "kpi:zone_demand" "kpi:restaurant_performance"; do
    EXISTS=$(docker exec redis redis-cli EXISTS "$key" 2>/dev/null)
    if [ "$EXISTS" = "1" ]; then
        TTL=$(docker exec redis redis-cli TTL "$key" 2>/dev/null)
        echo "   ✅ $key (TTL: ${TTL}s)"
        
        # Show sample
        echo "      Sample:"
        docker exec redis redis-cli GET "$key" 2>/dev/null | python3 -c "import sys, json; data=json.load(sys.stdin); print(f\"      Computed at: {data.get('computed_at', 'N/A')}\"); print(f\"      Records: {len(data.get('data', []))}\")" 2>/dev/null || echo "      (Unable to parse)"
    else
        echo "   ❌ $key not found"
    fi
done

echo ""
echo "=== Test Complete ==="

