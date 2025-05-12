import requests
from pydantic import ValidationError
from src.models.campground import Campground
from src.db.database import SessionLocal, CampgroundDB, create_tables
from sqlalchemy.exc import SQLAlchemyError
import time
from src.geocoding.nominatim import get_address_from_coordinates



BASE_URL = "https://thedyrt.com/api/v6/locations/search-results"

DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Referer": "https://thedyrt.com/search",
        "Origin": "https://thedyrt.com"
    }

# The US bounding box coordinates (Full country)
US_BOUNDS = "-125.0, 24.0, -66.0, 49.5"

# Regional US bbox definitions for more precise scraping
# Western US (Rocky Mountains region)
WESTERN_US_BOUNDS = "-125.0, 32.0, -105.0, 49.0"

# Eastern US
EASTERN_US_BOUNDS = "-90.0, 24.0, -66.0, 49.5"

# Midwest US
MIDWEST_US_BOUNDS = "-104.0, 36.5, -80.0, 49.0"

# Southern US
SOUTHERN_US_BOUNDS = "-106.0, 25.0, -75.0, 36.5"

# Pacific Northwest
PACIFIC_NW_BOUNDS = "-125.0, 42.0, -116.5, 49.0"

# Southwest US
SOUTHWEST_US_BOUNDS = "-120.0, 31.0, -105.0, 42.0"

# Northeast US
NORTHEAST_US_BOUNDS = "-80.0, 40.0, -66.0, 49.0"

# Southeast US
SOUTHEAST_US_BOUNDS = "-90.0, 24.0, -75.0, 36.5"

# State-specific bounding boxes
# New York area
NY_BOUNDS = "-80.0, 40.0, -71.0, 45.0"

# California area
CA_BOUNDS = "-124.0, 32.0, -114.0, 42.0"

# Texas area
TX_BOUNDS = "-106.5, 25.8, -93.5, 36.5"

# Florida area
FL_BOUNDS = "-87.5, 24.5, -80.0, 31.0"

# Colorado area
CO_BOUNDS = "-109.0, 37.0, -102.0, 41.0"

# Test area (small portion of Yellowstone)
TEST_BOUNDS = "-111.0, 44.0, -110.0, 45.0"

# Define all US regions as a list for systematic scraping
US_REGIONS = [
    WESTERN_US_BOUNDS,
    EASTERN_US_BOUNDS,
    MIDWEST_US_BOUNDS, 
    SOUTHERN_US_BOUNDS,
    PACIFIC_NW_BOUNDS,
    SOUTHWEST_US_BOUNDS,
    NORTHEAST_US_BOUNDS,
    SOUTHEAST_US_BOUNDS
]

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
        print(f"Failed to fetch campgrounds: {e}")
        return []

def process_campground(campground_data):
    try:
        # Extract the attributes from the API response
        attrs = campground_data.get("attributes", {})
        print(f"Processing campground {campground_data.get('id')}: {attrs}")
        
        # Get latitude and longitude
        latitude = attrs.get("latitude")
        longitude = attrs.get("longitude")
        
        # Get address using reverse geocoding
        address = None
        if latitude is not None and longitude is not None:
            address = get_address_from_coordinates(latitude, longitude)
            if address:
                print(f"Found address: {address}")
            else:
                print(f"Could not determine address for coordinates ({latitude}, {longitude})")
        
        # Create the data structure expected by the Pydantic model
        campground_dict = {
            "id": campground_data.get("id"),
            "type": campground_data.get("type"),
            "links": {
                "self": campground_data.get("links", {}).get("self", "https://thedyrt.com")
            },
            "name": attrs.get("name", ""),
            "latitude": latitude,
            "longitude": longitude,
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
            "availability-updated-at": attrs.get("availability-updated-at"),
            "address": address
        }
        
        # Create and validate a Campground model instance
        campground = Campground(**campground_dict)
        return campground
        
    except ValidationError as e:
        # Log specific validation errors
        print(f"Validation error for campground {campground_data.get('id')}: {e.errors()}")
        return None
    except Exception as e:
        # Log other unexpected errors
        print(f"Unexpected error while processing campground {campground_data.get('id')}: {e}")
        return None

def save_to_database(campgrounds):
    print(f"Saving {len(campgrounds)} campgrounds to database")
    
    db = SessionLocal()
    inserted_count = 0
    updated_count = 0
    error_count = 0
    
    try:
        print(f"Database connection established")
        
        for campground in campgrounds:
            try:
                # Check if the campground already exists
                existing = db.query(CampgroundDB).filter(CampgroundDB.id == campground.id).first()
                
                if existing:
                    # Update existing record
                    existing.name = campground.name
                    existing.type = campground.type
                    existing.links_self = str(campground.links.self)
                    existing.latitude = campground.latitude
                    existing.longitude = campground.longitude
                    existing.region_name = campground.region_name
                    existing.administrative_area = campground.administrative_area
                    existing.nearest_city_name = campground.nearest_city_name
                    existing.accommodation_type_names = campground.accommodation_type_names
                    existing.bookable = campground.bookable
                    existing.camper_types = campground.camper_types
                    existing.operator = campground.operator
                    existing.photo_url = str(campground.photo_url) if campground.photo_url else None
                    existing.photo_urls = [str(url) for url in campground.photo_urls] if campground.photo_urls else []
                    existing.photos_count = campground.photos_count
                    existing.rating = campground.rating
                    existing.reviews_count = campground.reviews_count
                    existing.slug = campground.slug
                    existing.price_low = campground.price_low
                    existing.price_high = campground.price_high
                    existing.availability_updated_at = campground.availability_updated_at
                    existing.address = campground.address
                    
                    updated_count += 1
                    print(f"Updated: {campground.name} (ID: {campground.id})")
                else:
                    # Create new record
                    db_campground = CampgroundDB(
                        id=campground.id,
                        type=campground.type,
                        links_self=str(campground.links.self),
                        name=campground.name,
                        latitude=campground.latitude,
                        longitude=campground.longitude,
                        region_name=campground.region_name,
                        administrative_area=campground.administrative_area,
                        nearest_city_name=campground.nearest_city_name,
                        accommodation_type_names=campground.accommodation_type_names,
                        bookable=campground.bookable,
                        camper_types=campground.camper_types,
                        operator=campground.operator,
                        photo_url=str(campground.photo_url) if campground.photo_url else None,
                        photo_urls=[str(url) for url in campground.photo_urls] if campground.photo_urls else [],
                        photos_count=campground.photos_count,
                        rating=campground.rating,
                        reviews_count=campground.reviews_count,
                        slug=campground.slug,
                        price_low=campground.price_low,
                        price_high=campground.price_high,
                        availability_updated_at=campground.availability_updated_at,
                        address=campground.address
                    )
                    db.add(db_campground)
                    inserted_count += 1
                    print(f"Inserted: {campground.name} (ID: {campground.id})")
                
                # Try to commit each record individually to avoid losing all on a single error
                db.commit()
                
            except SQLAlchemyError as e:
                print(f"Database error for campground {campground.id}: {str(e)}")
                error_count += 1
                db.rollback()
            except Exception as e:
                print(f"Unexpected error for campground {campground.id}: {str(e)}")
                error_count += 1
                db.rollback()
                
    except Exception as e:
        print(f"Unexpected error during database operations: {str(e)}")
        db.rollback()
    finally:
        db.close()
        
    return inserted_count, updated_count, error_count

def scrape_region(bbox, max_pages=2):
    """Scrape a specific bbox region"""
    print(f"Scanning region with bbox: {bbox}")
    raw_campgrounds = []
    final_campgrounds = []
    page = 1

    while True:
        print(f"Fetching page {page}...")
        data = get_campgrounds(bbox, page=page, page_size=20)  
        
        if not data:
            print("No more data to fetch.")
            break
            
        raw_campgrounds.extend(data)
        page += 1

        time.sleep(2) 

        if max_pages and page > max_pages:
            print(f"Reached maximum number of pages ({max_pages}).")
            break
    
    for campground_data in raw_campgrounds:
        campground = process_campground(campground_data)
        if campground:
            final_campgrounds.append(campground)

    if final_campgrounds:
        print("\nSaving campgrounds to database...")
        inserted, updated, errors = save_to_database(final_campgrounds)
        
        print(f"\nScan summary for region {bbox}:")
        print(f"  Total campgrounds processed: {len(final_campgrounds)}")
        print(f"  Inserted: {inserted}")
        print(f"  Updated: {updated}")
        print(f"  Errors: {errors}")
        return len(raw_campgrounds), len(final_campgrounds), inserted, updated
    else:
        print(f"No campgrounds to save for region {bbox}!")

    return len(raw_campgrounds), 0, 0, 0

def scrape_all_regions(max_pages_per_region=2):
    """Scrape all US regions systematically"""
    print(f"Starting systematic scan of all US regions...")
    total_raw = 0
    total_processed = 0
    total_inserted = 0
    total_updated = 0
    
    for i, region_bbox in enumerate(US_REGIONS):
        print(f"Scanning region {i+1}/{len(US_REGIONS)}: {region_bbox}")
        raw, processed, inserted, updated = scrape_region(region_bbox, max_pages_per_region)
        
        total_raw += raw
        total_processed += processed
        total_inserted += inserted
        total_updated += updated
        
        time.sleep(5)  
    
    print(f"\nComplete US scan summary:")
    print(f"  Total campgrounds found: {total_raw}")
    print(f"  Total campgrounds processed: {total_processed}")
    print(f"  Campgrounds inserted: {total_inserted}")
    print(f"  Campgrounds updated: {total_updated}")
    
    return total_raw, total_processed, total_inserted, total_updated

def main(max_pages=None, bbox=US_BOUNDS, scrape_full_us=False):
    
    # Create database tables if they don't exist
    create_tables()
    
    if scrape_full_us:
        # Scan all US regions systematically
        return scrape_all_regions(max_pages_per_region=max_pages or 2)
    else:
        # Scan a single region
        return scrape_region(bbox, max_pages or 2)

if __name__ == "__main__":
    main(max_pages=2, scrape_full_us=True)
    