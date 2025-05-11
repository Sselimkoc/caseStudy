import requests
from pydantic import ValidationError
from src.models.campground import Campground
from src.db.database import SessionLocal, CampgroundDB, create_tables
from sqlalchemy.exc import SQLAlchemyError
import time



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

# Western US (Rocky Mountains region)
WESTERN_US_BOUNDS = "-125.0, 32.0, -105.0, 49.0"

# Eastern US
EASTERN_US_BOUNDS = "-90.0, 24.0, -66.0, 49.5"

# New York area
NY_BOUNDS = "-80.0, 40.0, -71.0, 45.0"

# California area
CA_BOUNDS = "-124.0, 32.0, -114.0, 42.0"

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
        print(f"Failed to fetch campgrounds: {e}")
        return []

def process_campground(campground_data):
    try:
        # Extract the attributes from the API response
        attrs = campground_data.get("attributes", {})
        print(f"Processing campground {campground_data.get('id')}: {attrs}")
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
        print(f"Pydantic validation failed for campground {campground_data.get('id')}: {e.errors()}")
        return None
    except Exception as e:
        # Log other unexpected errors
        print(f"An unexpected error occurred while processing campground {campground_data.get('id')}: {e}")
        return None

def save_to_database(campgrounds):
    print(f"Attempting to save {len(campgrounds)} campgrounds to database")
    
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
                    
                    updated_count += 1
                    print(f"Updated campground: {campground.name} (ID: {campground.id})")
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
                        availability_updated_at=campground.availability_updated_at
                    )
                    db.add(db_campground)
                    inserted_count += 1
                    print(f"Inserted new campground: {campground.name} (ID: {campground.id})")
                
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

def main(max_pages=None, bbox=CA_BOUNDS):
    
    raw_campgrounds = []
    final_campgrounds = []
    page = 1

    # Create database tables if they don't exist
    create_tables()
    
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
        print("\n About to save campgrounds to database...")
        inserted, updated, errors = save_to_database(final_campgrounds)
        
        print(f"\n[INFO] Scraping summary:")
        print(f"  Total campgrounds processed: {len(final_campgrounds)}")
        print(f"  Inserted: {inserted}")
        print(f"  Updated: {updated}")
        print(f"  Errors: {errors}")
        return len(raw_campgrounds), len(final_campgrounds), inserted, updated
    else:
        print("[WARNING] No campgrounds to save to database!")

    return len(raw_campgrounds), 0, 0, 0

if __name__ == "__main__":
    # For testing, use the test bounds and limit to 2 pages
    main(max_pages=2, bbox=CA_BOUNDS)
    