from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from sqlalchemy.orm import Session
import logging
from typing import Optional, List
import concurrent.futures
import time

from src.db.database import get_db, CampgroundDB
from src.scraper.dyrt_scraper import (
    US_BOUNDS, WESTERN_US_BOUNDS, EASTERN_US_BOUNDS,
    MIDWEST_US_BOUNDS, SOUTHERN_US_BOUNDS, PACIFIC_NW_BOUNDS, SOUTHWEST_US_BOUNDS,
    NORTHEAST_US_BOUNDS, SOUTHEAST_US_BOUNDS, FOUR_MAIN_US_REGIONS
)
from src.scraper.dyrt_scraper import main as run_scraper, scrape_region, parallel_scrape_regions
from src.geocoding.nominatim import get_address_from_coordinates, batch_geocode

# Configure logging - Adjust format to remove INFO/WARNING prefixes
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Campground Scraper API",
    description="Simple API for scraping and retrieving campground data",
    version="1.0.0"
)

# Map of predefined bounding boxes
BBOX_MAP = {
    "us": US_BOUNDS,
    "western_us": WESTERN_US_BOUNDS,
    "eastern_us": EASTERN_US_BOUNDS,
    "midwest_us": MIDWEST_US_BOUNDS,
    "southern_us": SOUTHERN_US_BOUNDS,
    "pacific_northwest": PACIFIC_NW_BOUNDS,
    "southwest_us": SOUTHWEST_US_BOUNDS,
    "northeast_us": NORTHEAST_US_BOUNDS,
    "southeast_us": SOUTHEAST_US_BOUNDS
}

# Background task for scraping
def scrape_campgrounds_task(bbox= US_BOUNDS, max_pages = None):
    try:
        region_name = "Full US" if bbox == US_BOUNDS else f"Region with bbox: {bbox}"
        page_limit = f"with page limit: {max_pages}" if max_pages is not None else "without page limit (auto-collection)"
        
        logger.info(f"Starting: Scanning {region_name} {page_limit}")
        total_raw, total_processed, inserted, updated = run_scraper(max_pages=max_pages, bbox=bbox)
        logger.info(f"Completed: Found {total_raw} campgrounds, processed {total_processed}, inserted {inserted}, updated {updated}")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")

# Background task for updating addresses
def update_addresses_task(limit = 100, max_workers = 8):
    db = get_db()
    try:
        # Get campgrounds that don't have an address but have coordinates
        query = db.query(CampgroundDB).filter(
            CampgroundDB.address == None,  # No address
            CampgroundDB.latitude != None,  # Has latitude
            CampgroundDB.longitude != None  # Has longitude
        ).limit(limit)
        
        campgrounds = query.all()
        logger.info(f"Found {len(campgrounds)} campgrounds without address. Starting parallel geocoding with {max_workers} workers...")
        
        coords_list = [(c.latitude, c.longitude) for c in campgrounds]
        address_count = 0
        
        # Process all coordinates in a single batch using parallel geocoding
        start_time = time.time()
        addresses = batch_geocode(coords_list, max_workers=max_workers)
        end_time = time.time()
        
        # Update campgrounds with new addresses
        for i, campground in enumerate(campgrounds):
            coord_key = coords_list[i]
            if coord_key in addresses and addresses[coord_key]:
                campground.address = addresses[coord_key]
                address_count += 1
        
        # Commit all updates at once
        db.commit()
        
        # Calculate performance stats
        duration = end_time - start_time
        processing_speed = len(coords_list) / duration if duration > 0 else 0
        
        logger.info(f"Address update completed: {address_count} addresses added in {duration:.1f} seconds")
        logger.info(f"Geocoding performance: {processing_speed:.2f} coordinates/second with {max_workers} workers")
        
        return {
            "success": True, 
            "addresses_updated": address_count,
            "processing_time_seconds": duration,
            "processing_speed": f"{processing_speed:.2f} coordinates/second"
        }
        
    except Exception as e:
        logger.error(f"Error updating addresses: {str(e)}")
        db.rollback()
        return {"success": False, "error": str(e)}
    finally:
        db.close()

# Background task for parallel multi-region scraping
def scrape_multiregion_task(regions=FOUR_MAIN_US_REGIONS, max_pages=None, max_workers=4):
    try:
        logger.info(f"Starting parallel scan across {len(regions)} regions with {max_workers} workers")
        
        # Call the parallel scraper function
        total_raw, total_processed, total_inserted, total_updated = parallel_scrape_regions(
            regions=regions,
            max_pages=max_pages,
            max_workers=max_workers
        )
        
        logger.info(f"Completed multiregion scan: {total_raw} found, {total_processed} processed, "
                   f"{total_inserted} inserted, {total_updated} updated")
        
        return total_raw, total_processed, total_inserted, total_updated
    except Exception as e:
        logger.error(f"Error in multiregion scan: {str(e)}")
        return 0, 0, 0, 0

@app.get("/")
def read_root():
    return {"message": "Welcome to the Campground Scraper API"}

@app.post("/scrape")
async def scrape_campgrounds(
    background_tasks: BackgroundTasks,
    max_pages: Optional[int] = None,
    region: Optional[str] = None,
    bbox: Optional[str] = None
):
    try:
        # Determine the bounding box to use
        bounding_box = bbox
        
        # If region is specified but not bbox, convert region to bbox
        if not bounding_box and region:
            if region in BBOX_MAP:
                bounding_box = BBOX_MAP[region]
            elif region.lower() == "us":
                bounding_box = US_BOUNDS
            else:
                return {
                    "status": "error",
                    "message": f"Invalid region. Please choose from: {', '.join(BBOX_MAP.keys())} or provide a custom bbox parameter"
                }
        
        # If no region or bbox specified, default to US_BOUNDS
        if not bounding_box:
            bounding_box = US_BOUNDS
        
        # Start the scraper in the background
        background_tasks.add_task(scrape_campgrounds_task, bounding_box, max_pages)
        
        # Prepare the response message
        if bounding_box == US_BOUNDS:
            response_message = "Full US scan started"
        elif region in BBOX_MAP:
            response_message = f"Scan started: region={region}"
        else:
            response_message = f"Scan started: bbox={bounding_box}"
            
        return {
            "message": response_message,
            "status": "processing",
            "auto_collection": True if max_pages is None else False,
            "max_pages": max_pages,
            "bbox": bounding_box
        }
    except Exception as e:
        logger.error(f"Error starting scan: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update-addresses")
async def update_addresses(
    background_tasks: BackgroundTasks,
    limit: int = 100,
    max_workers: int = 8
):
    """
    Update missing addresses for campgrounds that have coordinates but no address.
    This endpoint will run parallel geocoding in the background to fill in the address field.
    
    Args:
        limit: Maximum number of campgrounds to process
        max_workers: Number of parallel workers for geocoding (default: 8)
    """
    try:
        background_tasks.add_task(update_addresses_task, limit, max_workers)
        return {
            "message": f"Address update task started for up to {limit} campgrounds using {max_workers} parallel workers",
            "status": "processing"
        }
    except Exception as e:
        logger.error(f"Error starting address update: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/campgrounds")
def get_campgrounds(
    db: Session = Depends(get_db),
    limit: int = 20,
    region: Optional[str] = None
):
    try:
        # Base query
        query = db.query(CampgroundDB)
        
        # Filter by region if specified
        if region:
            query = query.filter(CampgroundDB.region_name.ilike(f"%{region}%"))
        
        # Get the most recent campgrounds
        campgrounds = query.limit(limit).all()
        
        # Convert to simple list of dictionaries
        result = []
        for campground in campgrounds:
            camp_dict = {
                "id": campground.id,
                "name": campground.name,
                "region_name": campground.region_name,
                "rating": campground.rating,
                "price_low": campground.price_low,
                "price_high": campground.price_high,
            }
            result.append(camp_dict)
        
        return result
    except Exception as e:
        logger.error(f"Error retrieving campgrounds: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/campgrounds/{campground_id}")
def get_campground(campground_id: str, db: Session = Depends(get_db)):
    try:
        campground = db.query(CampgroundDB).filter(CampgroundDB.id == campground_id).first()
        
        if not campground:
            raise HTTPException(status_code=404, detail="Campground not found")
        
        # Convert to dictionary with only essential fields
        result = {
            "id": campground.id,
            "name": campground.name,
            "region_name": campground.region_name,
            "latitude": campground.latitude,
            "longitude": campground.longitude,
            "rating": campground.rating,
            "price_low": campground.price_low,
            "price_high": campground.price_high,
            "photo_url": campground.photo_url,
            "operator": campground.operator,
            "address": campground.address
        }
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving campground {campground_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/campgrounds/{campground_id}/detailed")
def get_campground_detailed(campground_id: str, db: Session = Depends(get_db)):
    """Get detailed information about a specific campground, including its address"""
    try:
        campground = db.query(CampgroundDB).filter(CampgroundDB.id == campground_id).first()
        
        if not campground:
            raise HTTPException(status_code=404, detail="Campground not found")
            
        # Get address from geocoding if not already present
        address = campground.address
        if not address and campground.latitude and campground.longitude:
            try:
                address = get_address_from_coordinates(campground.latitude, campground.longitude)
                if address:
                    # Save the address to the database for future queries
                    campground.address = address
                    db.commit()
            except Exception as e:
                logger.error(f"Error geocoding address: {str(e)}")
        
        # Convert to dictionary with all fields
        result = {
            "id": campground.id,
            "name": campground.name,
            "type": campground.type,
            "links_self": campground.links_self,
            "region_name": campground.region_name,
            "administrative_area": campground.administrative_area,
            "nearest_city_name": campground.nearest_city_name,
            "accommodation_type_names": campground.accommodation_type_names,
            "bookable": campground.bookable,
            "camper_types": campground.camper_types,
            "operator": campground.operator,
            "location": {
                "latitude": campground.latitude,
                "longitude": campground.longitude,
                "address": address
            },
            "photos": {
                "main_photo": campground.photo_url,
                "all_photos": campground.photo_urls,
                "count": campground.photos_count
            },
            "ratings": {
                "rating": campground.rating,
                "reviews_count": campground.reviews_count
            },
            "pricing": {
                "price_low": campground.price_low,
                "price_high": campground.price_high
            },
            "availability_updated_at": campground.availability_updated_at,
            "slug": campground.slug
        }
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving detailed campground {campground_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/regions")
def get_available_regions():
    """Return a list of all available regions that can be used with the scraper"""
    return {
        "regions": list(BBOX_MAP.keys()),
        "full_us_available": True
    }

@app.post("/scrape-multiregion")
async def scrape_campgrounds_multiregion(
    background_tasks: BackgroundTasks,
    request: Request,
    max_pages: Optional[int] = None,
    max_workers: int = 4
):
    try:
        # Parse JSON body to get regions
        data = await request.json()
        regions = data.get("regions")
        
        # Override query params if provided in body
        if "max_pages" in data:
            max_pages = data["max_pages"]
        if "max_workers" in data:
            max_workers = data["max_workers"]
        
        # Validate and prepare regions to scan
        bboxes_to_scan = []
        
        # If specific regions are requested
        if regions:
            for region in regions:
                if region in BBOX_MAP:
                    bboxes_to_scan.append(BBOX_MAP[region])
                else:
                    return {
                        "status": "error", 
                        "message": f"Invalid region '{region}'. Please choose from: {', '.join(BBOX_MAP.keys())}"
                    }
        else:
            # Default to predefined US regions
            bboxes_to_scan = FOUR_MAIN_US_REGIONS
        
        # Adjust worker count if needed
        actual_workers = min(max_workers, len(bboxes_to_scan))
        
        # Start the multiregion scraper in the background
        background_tasks.add_task(
            scrape_multiregion_task, 
            regions=bboxes_to_scan, 
            max_pages=max_pages, 
            max_workers=actual_workers
        )
            
        return {
            "message": f"Parallel scan started across {len(bboxes_to_scan)} regions with {actual_workers} worker threads",
            "status": "processing",
            "regions": len(bboxes_to_scan),
            "threads": actual_workers,
            "max_pages_per_region": max_pages
        }
    except Exception as e:
        logger.error(f"Error starting multiregion scan: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 