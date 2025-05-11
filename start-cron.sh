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

# Test California (state) scan
echo "Initial test: California region..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"region": "california", "max_pages": 1}'
sleep 5

# Test New York (state) scan
echo "Initial test: New York region..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"region": "new_york", "max_pages": 1}'
sleep 5

# Test Western US (broader region) scan
echo "Initial test: Western US region..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"region": "western_us", "max_pages": 1}'
sleep 5

# Test with direct bbox parameter for Yellowstone area
echo "Testing with custom bbox parameter (Yellowstone area)..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"bbox": "-111.2, 44.1, -109.8, 45.1", "max_pages": 1}'
sleep 5

# Test full US scan with very limited pages (just to verify it works)
echo "Testing full US scan function (limited to 1 page per region)..."
curl -X POST http://api:8000/scrape -H "Content-Type: application/json" -d '{"scrape_full_us": true, "max_pages": 1}'

# ==========================================================
# START CRON DAEMON
# ==========================================================
echo "All initial tests completed. Starting cron daemon..."
crond -f -l 8 