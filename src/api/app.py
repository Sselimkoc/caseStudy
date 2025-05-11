from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from sqlalchemy.orm import Session
import logging
from typing import Optional

from src.db.database import get_db, CampgroundDB
from src.scraper.dyrt_scraper import (
    CA_BOUNDS, US_BOUNDS, NY_BOUNDS, WESTERN_US_BOUNDS, EASTERN_US_BOUNDS,
    MIDWEST_US_BOUNDS, SOUTHERN_US_BOUNDS, PACIFIC_NW_BOUNDS, SOUTHWEST_US_BOUNDS,
    NORTHEAST_US_BOUNDS, SOUTHEAST_US_BOUNDS, TX_BOUNDS, FL_BOUNDS, CO_BOUNDS,
    US_REGIONS,
)
from src.scraper.dyrt_scraper import main as run_scraper

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
    "california": CA_BOUNDS,
    "us": US_BOUNDS,
    "new_york": NY_BOUNDS,
    "western_us": WESTERN_US_BOUNDS,
    "eastern_us": EASTERN_US_BOUNDS,
    "midwest_us": MIDWEST_US_BOUNDS,
    "southern_us": SOUTHERN_US_BOUNDS,
    "pacific_northwest": PACIFIC_NW_BOUNDS,
    "southwest_us": SOUTHWEST_US_BOUNDS,
    "northeast_us": NORTHEAST_US_BOUNDS,
    "southeast_us": SOUTHEAST_US_BOUNDS,
    "texas": TX_BOUNDS,
    "florida": FL_BOUNDS,
    "colorado": CO_BOUNDS
}

# Background task for scraping
def scrape_campgrounds_task(bbox: str, max_pages: int = 2, scrape_full_us: bool = False):
    try:
        if scrape_full_us:
            logger.info(f"Starting: Scanning all US regions (page limit={max_pages})")
            total_raw, total_processed, inserted, updated = run_scraper(max_pages=max_pages, scrape_full_us=True)
            logger.info(f"Completed: Found {total_raw} campgrounds, processed {total_processed}, inserted {inserted}, updated {updated}")
        else:
            logger.info(f"Starting: Scanning region {bbox}, (page limit={max_pages})")
            total_raw, total_processed, inserted, updated = run_scraper(max_pages=max_pages, bbox=bbox)
            logger.info(f"Completed: Found {total_raw} campgrounds, processed {total_processed}, inserted {inserted}, updated {updated}")
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")

@app.get("/")
def read_root():
    return {"message": "Welcome to the Campground Scraper API"}

@app.post("/scrape")
async def scrape_campgrounds(
    background_tasks: BackgroundTasks,
    max_pages: int = 2,
    region: Optional[str] = "california",
    bbox: Optional[str] = None,
    scrape_full_us: bool = False
):
    try:
        if scrape_full_us:
            # Start a full US scrape task
            background_tasks.add_task(scrape_campgrounds_task, "", max_pages, scrape_full_us=True)
            return {
                "message": "Full US scan started - this will take some time",
                "status": "processing",
                "max_pages_per_region": max_pages,
                "regions": len(US_REGIONS)
            }
        
        # If bbox is provided directly, use it
        # Otherwise, lookup from predefined regions
        bounding_box = bbox
        if not bounding_box:
            if region in BBOX_MAP:
                bounding_box = BBOX_MAP[region]
            else:
                return {
                    "status": "error",
                    "message": f"Invalid region. Please choose from: {', '.join(BBOX_MAP.keys())} or provide a custom bbox parameter"
                }
        
        # Start the scraper in the background
        background_tasks.add_task(scrape_campgrounds_task, bounding_box, max_pages)
        
        response_message = f"Scan started: bbox={bounding_box}"
        if not bbox and region in BBOX_MAP:
            response_message = f"Scan started: region={region}"
            
        return {
            "message": response_message,
            "status": "processing",
            "max_pages": max_pages,
            "bbox": bounding_box
        }
    except Exception as e:
        logger.error(f"Error starting scan: {str(e)}")
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
            "operator": campground.operator
        }
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving campground {campground_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/regions")
def get_available_regions():
    """Return a list of all available regions that can be used with the scraper"""
    return {
        "regions": list(BBOX_MAP.keys()),
        "full_us_available": True
    } 