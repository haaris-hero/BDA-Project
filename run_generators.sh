#!/bin/bash

# Function to run generator with restart on failure
run_generator() {
    local script=$1
    local name=$2
    
    while true; do
        echo "🔄 Starting $name..."
        docker exec -it airflow-webserver python /home/airflow/scripts/$script
        echo "⚠️  $name stopped, restarting in 5 seconds..."
        sleep 5
    done
}

# Run all 3 in background
run_generator "generate_kitchen_stream.py" "Kitchen Generator" &
run_generator "generate_rider_stream.py" "Rider Generator" &
run_generator "generate_orders_stream.py" "Orders Generator" &

# Keep script running
wait
