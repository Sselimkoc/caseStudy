import requests
import time
from typing import Optional, Dict, Any, Tuple, List

NOMINATIM_BASE_URL = "https://nominatim.openstreetmap.org/reverse"
USER_AGENT = "CampgroundScraperApp/1.0"  
REQUEST_TIMEOUT = 10 
RATE_LIMIT_DELAY = 1.1  

# Cache to minimize API calls for the same coordinates
# Format: {(lat, lon): address_string}
geocoding_cache = {}

def get_address_from_coordinates(latitude, longitude):
   
    # Check cache first to avoid redundant API calls
    cache_key = (latitude, longitude)
    if cache_key in geocoding_cache:
        return geocoding_cache[cache_key]
    
    # Introduce delay to respect rate limiting
    time.sleep(RATE_LIMIT_DELAY)
    
    try:
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
                geocoding_cache[cache_key] = address
                return address
        
        return None
        
    except requests.RequestException as e:
        print(f"Geocoding error for coordinates ({latitude}, {longitude}): {e}")
        return None
    except Exception as e:
        print(f"Unexpected error during geocoding: {e}")
        return None

def batch_geocode(coordinates_list):
 
    results = {}
    
    for lat, lon in coordinates_list:
        address = get_address_from_coordinates(lat, lon)
        results[(lat, lon)] = address
    
    return results 