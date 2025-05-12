import requests
import time
import logging
import concurrent.futures
from typing import Optional, Dict, Any, Tuple, List
from threading import Lock

# Constants
NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/reverse"
USER_AGENT = "CampgroundScraperApp/1.0" 
REQUEST_TIMEOUT = 10  
RATE_LIMIT_DELAY = 1.1  
MAX_RETRIES = 3  

# Get logger
logger = logging.getLogger(__name__)

# Cache to minimize API calls for the same coordinates
# Format: {(lat, lon): address_string}
geocoding_cache = {}

# Add lock for thread-safe cache access
cache_lock = Lock()

def get_address_from_coordinates(latitude, longitude):
    # Check cache first to avoid redundant API calls
    cache_key = (latitude, longitude)
    
    with cache_lock:
        if cache_key in geocoding_cache:
            return geocoding_cache[cache_key]
    
    retries = 0
    while retries < MAX_RETRIES:
        try:
            time.sleep(RATE_LIMIT_DELAY)
            
            params = {
                "lat": latitude,
                "lon": longitude,
                "format": "json",
                "zoom": 18, 
                "addressdetails": 1
            }
            
            headers = {
                "User-Agent": USER_AGENT
            }
            
            response = requests.get(
                NOMINATIM_BASE_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract the formatted address
                if "display_name" in data:
                    address = data["display_name"]
                    # Cache the result
                    with cache_lock:
                        geocoding_cache[cache_key] = address
                    logger.info(f"Successfully geocoded coordinates ({latitude}, {longitude})")
                    return address
            
            retries += 1
            wait_time = RATE_LIMIT_DELAY * (retries + 1)
            
            if response.status_code != 200:
                logger.warning(f"Geocoding HTTP error ({response.status_code}) for coordinates ({latitude}, {longitude}). Retrying in {wait_time}s... (Attempt {retries}/{MAX_RETRIES})")
                time.sleep(wait_time)
                continue
                
            logger.warning(f"No address found for coordinates ({latitude}, {longitude})")
            return None
                
        except requests.RequestException as e:
            retries += 1
            wait_time = RATE_LIMIT_DELAY * (retries + 1)
            logger.warning(f"Network error for coordinates ({latitude}, {longitude}): {e}. Retrying in {wait_time}s... (Attempt {retries}/{MAX_RETRIES})")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Unexpected error during geocoding: {e}")
            return None
    
    logger.error(f"Failed to geocode coordinates ({latitude}, {longitude}) after {MAX_RETRIES} attempts")
    return None

def batch_geocode(coordinates_list, max_workers=4):
    results = {}
    success_count = 0
    failure_count = 0
    
    total_coords = len(coordinates_list)
    logger.info(f"Starting parallel batch geocoding for {total_coords} coordinate pairs with {max_workers} workers")
    
    # Use ThreadPoolExecutor to geocode in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Create a future for each coordinate pair
        future_to_coords = {
            executor.submit(get_address_from_coordinates, lat, lon): (lat, lon)
            for lat, lon in coordinates_list
        }
        
        # Process results as they complete
        for i, future in enumerate(concurrent.futures.as_completed(future_to_coords)):
            coords = future_to_coords[future]
            try:
                address = future.result()
                results[coords] = address
                
                if address:
                    success_count += 1
                else:
                    failure_count += 1
                    
                # Log progress every 10 coordinates or at the end
                if (i + 1) % 10 == 0 or (i + 1) == total_coords:
                    logger.info(f"Geocoding progress: {i+1}/{total_coords} ({((i+1)/total_coords*100):.1f}%)")
            except Exception as e:
                failure_count += 1
                logger.error(f"Error geocoding coordinates {coords}: {str(e)}")
                results[coords] = None
    
    # Log summary
    total = success_count + failure_count
    if total > 0:
        success_rate = (success_count / total) * 100
        logger.info(f"Parallel batch geocoding completed: {success_rate:.1f}% success rate ({success_count}/{total})")
    
    return results 