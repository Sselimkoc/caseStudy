import requests
from pydantic import ValidationError
from src.models.campground import Campground


BASE_URL = "https://thedyrt.com/api/v6/locations/search-results"

DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Referer": "https://thedyrt.com/search",
        "Origin": "https://thedyrt.com"
    }

# The US bounding box coordinates
US_BOUNDS = "-125.0, 24.0, -66.0, 49.5"

TEST_BOUNDS = "-111.0, 44.0, -110.0, 45.0"

def get_campgrounds(bbox, page=1, page_size=5):
    params = {
        "filter[search][drive_time]": "any",
        "filter[search][air_quality]": "any",
        "filter[search][electric_amperage]": "any",
        "filter[search][max_vehicle_length]": "any",
        "filter[search][price]": "any",
        "filter[search][rating]": "any",
        "filter[search][bbox]": bbox,
        "sort": "recommended",
        "page[number]": page,
        "page[size]": page_size
    }

    try:
        response = requests.get(BASE_URL, headers=DEFAULT_HEADERS, params=params)
        response.raise_for_status()
        return response.json().get("data", [])
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch campgrounds: {e}")
        return []

def process_campground(campground_data):
    try:
        # Extract the attributes from the API response
        attrs = campground_data.get("attributes", {})
        print(f"[INFO] Processing campground {campground_data.get('id')}: {attrs}")
        # Create the data structure expected by the Pydantic model
        campground_dict = {
            "id": campground_data.get("id"),
            "type": campground_data.get("type"),
            "links": {
                "self": campground_data.get("links", {}).get("self", "https://thedyrt.com")
            },
            "name": attrs.get("name", ""),
            "latitude": attrs.get("latitude"),
            "longitude": attrs.get("longitude"),
            "region-name": attrs.get("region-name", ""),
            "administrative-area": attrs.get("administrative-area"),
            "nearest-city-name": attrs.get("nearest-city-name"),
            "accommodation-type-names": attrs.get("accommodation-type-names", []),
            "bookable": attrs.get("bookable", False),
            "camper-types": attrs.get("camper-types", []),
            "operator": attrs.get("operator"),
            "photo-url": attrs.get("photo-url"),
            "photo-urls": attrs.get("photo-urls", []),
            "photos-count": attrs.get("photos-count", 0),
            "rating": attrs.get("rating"),
            "reviews-count": attrs.get("reviews-count", 0),
            "slug": attrs.get("slug"),
            "price-low": attrs.get("price-low"),
            "price-high": attrs.get("price-high"),
            "availability-updated-at": attrs.get("availability-updated-at")
        }
        
        # Create and validate a Campground model instance
        campground = Campground(**campground_dict)
        return campground
        
    except ValidationError as e:
        # Log specific validation errors
        print(f"[ERROR] Pydantic validation failed for campground {campground_data.get('id')}: {e.errors()}")
        return None
    except Exception as e:
        # Log other unexpected errors
        print(f"[ERROR] An unexpected error occurred while processing campground {campground_data.get('id')}: {e}")
        return None

def main(max_pages=None):
    """
    Main function to run the scraper.
    """
    raw_campgrounds = []
    final_campgrounds = []
    page = 1
    while True:
        print(f"[INFO] Fetching page {page}...")
        data = get_campgrounds(TEST_BOUNDS, page=page)
        if not data:
            break
        raw_campgrounds.extend(data)
        page += 1

        if max_pages and page > max_pages:
            print(f"[INFO] Reached maximum number of pages ({max_pages}).")
            break
    
    for campground_data in raw_campgrounds:
        campground = process_campground(campground_data)
        if campground:
            final_campgrounds.append(campground)

    if final_campgrounds:
        print("\n[INFO] Example of final campground:")
        example = final_campgrounds[0]
        print(f"  ID: {example.id}")
        print(f"  Name: {example.name}")
        print(f"  Location: {example.latitude}, {example.longitude}")
        print(f"  Region: {example.region_name}")
        print(f"  Rating: {example.rating}")
    
    return final_campgrounds

if __name__ == "__main__":
    main(max_pages=2)