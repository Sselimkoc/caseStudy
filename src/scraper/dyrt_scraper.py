import requests
from pydantic import ValidationError
from src.models.campground import Campground
from src.db.database import SessionLocal, CampgroundDB, create_tables
from sqlalchemy.exc import SQLAlchemyError
import time
import logging
import os
import concurrent.futures
from datetime import datetime
from src.geocoding.nominatim import get_address_from_coordinates

# Create logs directory
logs_dir = os.path.join(os.getcwd(), 'logs')
os.makedirs(logs_dir, exist_ok=True)

# Create log file with today's date
log_filename = os.path.join(logs_dir, f'scraper_{datetime.now().strftime("%Y%m%d")}.log')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler() 
    ]
)

logger = logging.getLogger(__name__)

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
# NY_BOUNDS = "-80.0, 40.0, -71.0, 45.0"

# California area
# CA_BOUNDS = "-124.0, 32.0, -114.0, 42.0"

# Test area (small portion of Yellowstone)
# TEST_BOUNDS = "-111.0, 44.0, -110.0, 45.0"

# Define 4 main US regions for parallel scraping
FOUR_MAIN_US_REGIONS = [
    WESTERN_US_BOUNDS, # Western US
    EASTERN_US_BOUNDS, # Eastern US
    MIDWEST_US_BOUNDS, # Midwest US
    SOUTHERN_US_BOUNDS # Southern US
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

    max_retries = 3
    retries = 0
    
    while retries < max_retries:
        try:
            logger.info(f"API request: page {page}, region {bbox}")
            response = requests.get(BASE_URL, headers=DEFAULT_HEADERS, params=params)
            
            if response.status_code == 200:
                data = response.json().get("data", [])
                logger.info(f"Retrieved {len(data)} campgrounds")
                return data
            
            retries += 1
            wait_time = 2 * retries 
            
            if 400 <= response.status_code < 500:
                if response.status_code == 429:  # Rate limiting
                    logger.warning(f"Rate limit exceeded. Waiting {wait_time} seconds... (Attempt {retries}/{max_retries})")
                else:
                    logger.error(f"Client error: HTTP {response.status_code} - {response.text}")
                    return []  
            elif 500 <= response.status_code < 600:
                logger.warning(f"Server error: HTTP {response.status_code}. Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})")
            
            time.sleep(wait_time)
            
        except requests.exceptions.RequestException as e:
            retries += 1
            wait_time = 2 * retries
            logger.warning(f"Connection error: {e}. Retrying in {wait_time} seconds... (Attempt {retries}/{MAX_retries})")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return []
    
    logger.error(f"Failed to fetch campgrounds after {max_retries} attempts.")
    return []

def process_campground(campground_data):
    try:
        # Extract the attributes from the API response
        attrs = campground_data.get("attributes", {})
        logger.debug(f"Processing campground {campground_data.get('id')}: {attrs.get('name', 'unnamed')}")
        
        # Get latitude and longitude
        latitude = attrs.get("latitude")
        longitude = attrs.get("longitude")
        
        # Get address using reverse geocoding
        address = None
        if latitude is not None and longitude is not None:
            address = get_address_from_coordinates(latitude, longitude)
            if address:
                logger.debug(f"Found address: {address}")
            else:
                logger.debug(f"Could not determine address for coordinates ({latitude}, {longitude})")
        
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
        logger.error(f"Validation error for campground {campground_data.get('id')}: {e.errors()}")
        return None
    except Exception as e:
        # Log other unexpected errors
        logger.error(f"Unexpected error while processing campground {campground_data.get('id')}: {e}")
        return None

def save_to_database(campgrounds):
    logger.info(f"Saving {len(campgrounds)} campgrounds to database")
    
    db = SessionLocal()
    inserted_count = 0
    updated_count = 0
    error_count = 0
    
    try:
        logger.info(f"Database connection established")
        
        for campground in campgrounds:
            # Simple retry mechanism for each database operation
            max_db_retries = 3
            db_retries = 0
            success = False
            
            while not success and db_retries < max_db_retries:
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
                        logger.info(f"Updated: {campground.name} (ID: {campground.id})")
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
                        logger.info(f"Inserted: {campground.name} (ID: {campground.id})")
                    
                    # Try to commit each record individually to avoid losing all on a single error
                    db.commit()
                    success = True
                    
                except SQLAlchemyError as e:
                    db.rollback()
                    db_retries += 1
                    wait_time = 0.5 * db_retries
                    
                    if db_retries >= max_db_retries:
                        logger.error(f"Database error for campground {campground.id} after {max_db_retries} attempts: {str(e)}")
                        error_count += 1
                    else:
                        logger.warning(f"Database error for campground {campground.id}, retrying... (Attempt {db_retries}/{max_db_retries})")
                        time.sleep(wait_time)
                        
                except Exception as e:
                    db.rollback()
                    logger.error(f"Unexpected error for campground {campground.id}: {str(e)}")
                    error_count += 1
                    break
                    
    except Exception as e:
        db.rollback()
        logger.error(f"Critical database error: {str(e)}")
    finally:
        db.close()
        
    return inserted_count, updated_count, error_count

def scrape_region(bbox, max_pages=None):
    logger.info(f"Starting scan for region with bbox: {bbox}")
    
    start_time = time.time()
    raw_campgrounds = []
    final_campgrounds = []
    page = 1
    api_errors = 0
    processing_errors = 0
    db_errors = 0
    
    try:
        # Data collection phase
        while True:
            logger.info(f"Fetching page {page}...")
            
            data = get_campgrounds(bbox, page=page, page_size=20)  
            
            if not data:
                if page == 1:
                    logger.warning("No data retrieved from first page. Region might be empty or API issues.")
                else:
                    logger.info("No more data to fetch, scan complete.")
                break
            
            raw_campgrounds.extend(data)
            logger.info(f"Retrieved {len(data)} campgrounds from page {page}")
            page += 1

            # Rate limiting
            time.sleep(2) 

            # Optional page limit if specified
            if max_pages is not None and page > max_pages:
                logger.info(f"Reached maximum number of pages ({max_pages}). Stopping scan for this region.")
                break
        
        # Data processing phase
        logger.info(f"Processing {len(raw_campgrounds)} campground records...")
        
        for i, campground_data in enumerate(raw_campgrounds):
            try:
                campground = process_campground(campground_data)
                if campground:
                    final_campgrounds.append(campground)
                    if (i+1) % 20 == 0:  # Log every 20 records
                        logger.info(f"Processing: {i+1}/{len(raw_campgrounds)}")
                else:
                    processing_errors += 1
                    logger.warning(f"Failed to process campground data (ID: {campground_data.get('id', 'unknown')}) - Data: {campground_data}") # Added data for better debugging
            except Exception as e:
                processing_errors += 1
                logger.error(f"Error processing campground: {e}", exc_info=True) # Added exc_info for traceback
        
        # Database saving phase
        if final_campgrounds:
            logger.info(f"\nSaving {len(final_campgrounds)} campgrounds to database...")
            inserted, updated, errors = save_to_database(final_campgrounds)
            db_errors = errors
            
            # Summary report
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"\nScan summary for region {bbox}:")
            logger.info(f"  Total runtime: {duration:.2f} seconds")
            logger.info(f"  Campgrounds found: {len(raw_campgrounds)}")
            logger.info(f"  Campgrounds processed: {len(final_campgrounds)}")
            logger.info(f"  Pages scanned: {page-1}")
            logger.info(f"  Inserted: {inserted}")
            logger.info(f"  Updated: {updated}")
            logger.info(f"  API errors: {api_errors}")
            logger.info(f"  Processing errors: {processing_errors}")
            logger.info(f"  Database errors: {db_errors}")
            logger.info(f"  Total errors: {api_errors + processing_errors + db_errors}")
            
            return len(raw_campgrounds), len(final_campgrounds), inserted, updated
        else:
            logger.warning(f"No campgrounds to save for region {bbox}!")

    except Exception as e:
        logger.error(f"Critical error during scan: {str(e)}", exc_info=True)
        
    return len(raw_campgrounds), 0, 0, 0

def parallel_scrape_regions(regions=FOUR_MAIN_US_REGIONS, max_pages=None, max_workers=4):
    """
    Scrapes multiple regions in parallel using a thread pool.
    
    Args:
        regions: List of bounding boxes to scrape in parallel (defaults to 4 main US regions)
        max_pages: Maximum number of pages to scrape per region
        max_workers: Maximum number of worker threads
        
    Returns:
        Tuple of (total_raw, total_processed, total_inserted, total_updated) counts
    """
    try:
        start_time = time.time()
        logger.info(f"Starting parallel scan of {len(regions)} regions with {max_workers} workers")
        
        # Create database tables if they don't exist
        create_tables()
        
        total_raw = 0
        total_processed = 0
        total_inserted = 0
        total_updated = 0
        failed_regions = []
        
        # Adjust worker count if needed
        actual_workers = min(max_workers, len(regions))
        
        # Create thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
            # Start scraping all regions in parallel
            future_to_region = {
                executor.submit(scrape_region, region, max_pages): region 
                for region in regions
            }
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    raw, processed, inserted, updated = future.result()
                    total_raw += raw
                    total_processed += processed
                    total_inserted += inserted
                    total_updated += updated
                    
                    # Check if the completed region is one of the 4 main regions for logging clarity
                    region_name = "a main US region" if region in FOUR_MAIN_US_REGIONS else "a custom region"
                    logger.info(f"Completed scraping {region_name} (bbox: {region}): {raw} found, {processed} processed, {inserted} inserted, {updated} updated")
                except Exception as e:
                    failed_regions.append(region)
                    logger.error(f"Failed to scrape region {region}: {str(e)}")
        
        # Summarize results
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"\nParallel scan summary:")
        logger.info(f"  Total runtime: {duration:.2f} seconds")
        logger.info(f"  Regions scanned: {len(regions)}")
        logger.info(f"  Regions succeeded: {len(regions) - len(failed_regions)}")
        logger.info(f"  Regions failed: {len(failed_regions)}")
        logger.info(f"  Total campgrounds found: {total_raw}")
        logger.info(f"  Total campgrounds processed: {total_processed}")
        logger.info(f"  Total campgrounds inserted: {total_inserted}")
        logger.info(f"  Total campgrounds updated: {total_updated}")
        
        if failed_regions:
            logger.warning(f"Failed regions: {', '.join(failed_regions)}")
            
        return total_raw, total_processed, total_inserted, total_updated
        
    except Exception as e:
        logger.error(f"Critical error in parallel scraping: {str(e)}", exc_info=True)
        return 0, 0, 0, 0

def main(max_pages=10, bbox=US_BOUNDS, parallel=False, max_workers=4):
    """
    Main function to run the scraper with specified parameters.
    
    Args:
        max_pages: Maximum number of pages to scrape per region, or None for unlimited
        bbox: Bounding box coordinates for the region to scrape, defaults to all US
        parallel: If True, splits the US into regions and scrapes in parallel
        max_workers: Number of parallel workers to use when parallel=True
        
    Returns:
        Tuple of (total_raw, total_processed, inserted, updated) counts
    """
    try:
        # Create database tables if they don't exist
        create_tables()
        
        # If we're doing a parallel US scan
        if parallel and bbox == US_BOUNDS:
            logger.info(f"Starting parallel scan of 4 main US regions with {max_workers} workers with max pages {max_pages}")
            return parallel_scrape_regions(FOUR_MAIN_US_REGIONS, max_pages, max_workers)
        
        # Otherwise do a regular scan
        region_name = "Full US" if bbox == US_BOUNDS else f"Region with bbox: {bbox}"
        page_limit = f"with page limit: {max_pages}" if max_pages is not None else "without page limit (auto-collection)"
        
        logger.info(f"Starting scan for {region_name} {page_limit}")
        
        # Run a single region scan with the provided parameters
        result = scrape_region(bbox, max_pages)
        
        return result
            
    except Exception as e:
        logger.error(f"Critical error in main function: {str(e)}", exc_info=True)
        return 0, 0, 0, 0

if __name__ == "__main__":
    # Default run: Parallel US scan with 10 page limit per region and default 4 workers
    main(max_pages=10, parallel=True)
    
