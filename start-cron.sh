#!/bin/bash

# Start cron in foreground
echo "Starting cron service..."
echo "Current crontab:"
cat /etc/crontabs/root

# Give API service time to start
echo "Waiting for API service to initialize..."
sleep 30

# ==========================================================
# INITIAL TEST REQUESTS (low page counts for quick testing)
# ==========================================================

# Full US scan with limited pages for testing
echo "Starting initial US scan test with all regions..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"scrape_full_us": true, "max_pages": 2}'
sleep 5

# Also test with broader US bounds for full coverage
echo "Testing with full US bounds..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"region": "us", "max_pages": 3}'
sleep 5

# Test major geographic regions
echo "Testing Western US region..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"region": "western_us", "max_pages": 2}'
sleep 5

echo "Testing Eastern US region..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"region": "eastern_us", "max_pages": 2}'
sleep 5

# ==========================================================
# START CRON DAEMON
# ==========================================================
echo "All initial tests completed. Starting cron daemon..."
crond -f -l 8 