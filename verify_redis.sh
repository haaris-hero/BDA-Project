#!/bin/bash
# Redis Cache Verification Script

echo "=========================================="
echo "Redis Cache Verification"
echo "=========================================="
echo ""

# 1. Check Redis is running
echo "1. Checking Redis Status..."
if docker exec redis redis-cli PING > /dev/null 2>&1; then
    echo "   ✅ Redis is running"
else
    echo "   ❌ Redis is not running"
    exit 1
fi

echo ""

# 2. Check Redis keys
echo "2. Checking Redis Keys..."
KEYS=$(docker exec redis redis-cli KEYS "*" 2>/dev/null)
if [ -z "$KEYS" ]; then
    echo "   ⚠️  No keys found in Redis (Spark job may not have run yet)"
    echo "   Expected keys: kpi:kitchen_load, kpi:rider_efficiency, kpi:zone_demand, kpi:restaurant_performance"
else
    echo "   ✅ Found keys in Redis:"
    echo "$KEYS" | while read key; do
        if [ ! -z "$key" ]; then
            TTL=$(docker exec redis redis-cli TTL "$key" 2>/dev/null)
            echo "      - $key (TTL: ${TTL}s)"
        fi
    done
fi

echo ""

# 3. Check specific KPI keys
echo "3. Checking KPI Cache Keys..."
KPI_KEYS=("kpi:kitchen_load" "kpi:rider_efficiency" "kpi:zone_demand" "kpi:restaurant_performance")

for key in "${KPI_KEYS[@]}"; do
    EXISTS=$(docker exec redis redis-cli EXISTS "$key" 2>/dev/null)
    if [ "$EXISTS" = "1" ]; then
        TTL=$(docker exec redis redis-cli TTL "$key" 2>/dev/null)
        SIZE=$(docker exec redis redis-cli STRLEN "$key" 2>/dev/null)
        echo "   ✅ $key exists (TTL: ${TTL}s, Size: ${SIZE} bytes)"
        
        # Show sample data
        echo "      Sample data:"
        docker exec redis redis-cli GET "$key" 2>/dev/null | python3 -m json.tool 2>/dev/null | head -10 | sed 's/^/      /'
    else
        echo "   ❌ $key not found"
    fi
    echo ""
done

# 4. Check Spark job status
echo "4. Checking Spark Job Status..."
SPARK_RUNS=$(docker exec airflow-scheduler airflow dags list-runs --dag-id spark_compute_kpis --state success --no-backfill 2>&1 | grep -c "spark_compute_kpis" || echo "0")
if [ "$SPARK_RUNS" -gt "0" ]; then
    echo "   ✅ Spark job has run successfully ($SPARK_RUNS times)"
else
    echo "   ⚠️  Spark job hasn't run successfully yet"
    echo "   Trigger it with: docker exec airflow-scheduler airflow dags trigger spark_compute_kpis"
fi

echo ""

# 5. Monitor Redis in real-time (optional)
echo "5. Redis Monitor (Press Ctrl+C to exit)..."
echo "   Watching for new keys..."
echo ""
docker exec redis redis-cli --csv MONITOR 2>/dev/null | head -20 || echo "   (No activity yet)"

echo ""
echo "=========================================="
echo "Verification Complete"
echo "=========================================="
echo ""
echo "To manually test Redis caching:"
echo "1. Trigger Spark job: docker exec airflow-scheduler airflow dags trigger spark_compute_kpis"
echo "2. Wait 30-60 seconds for job to complete"
echo "3. Run this script again: ./verify_redis.sh"
echo ""
echo "To view Redis keys interactively:"
echo "  docker exec -it redis redis-cli"
echo "  > KEYS *"
echo "  > GET kpi:kitchen_load"
echo "  > TTL kpi:kitchen_load"

