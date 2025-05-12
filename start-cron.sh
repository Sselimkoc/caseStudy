#!/bin/bash

# Start cron in foreground
echo "Starting cron service..."
echo "Current crontab:"
cat /etc/crontabs/root

# Give API service time to start
echo "Waiting for API service to initialize..."
sleep 30

# ==========================================================
# INITIAL TEST REQUESTS 
# ==========================================================


# Parallel multi-region scan for faster processing
echo "Starting parallel multi-region scan with 4 worker threads and 10 page limit for US regions..."
# Explicitly list the region names as expected by the API
curl -X POST "http://api:8000/scrape-multiregion" \
  -H "Content-Type: application/json" \
  -d '{"max_workers": 4, "max_pages": 10, "regions": ["western_us", "eastern_us", "midwest_us", "southern_us"]}'
sleep 5

# Test address updates
echo "Testing address update functionality with parallel geocoding (8 workers)..."
curl -X POST http://api:8000/update-addresses -H "Content-Type: application/json" -d '{"limit": 100, "max_workers": 8}'
sleep 5

# ==========================================================
# START CRON DAEMON
# ==========================================================
echo "All initial tests completed. Starting cron daemon..."
crond -f -l 8 