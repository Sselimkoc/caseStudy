"""
Main entrypoint for The Dyrt web scraper case study.

Usage:
    The scraper can be run directly (`python main.py`) or via Docker Compose (`docker compose up`).

If you have any questions in mind you can connect to me directly via info@smart-maple.com
"""
from src.db.database import create_tables
from src.scraper.dyrt_scraper import main as run_scraper

def main():
    """
    Main function to run the scraper.
    """
    try:
        # Initialize database tables
        create_tables()
        
        # Run full US scraper to scan all regions systematically
        print("Starting full US scan to collect campgrounds across the entire United States...")
        total_raw, total_processed, inserted, updated = run_scraper(max_pages=3, scrape_full_us=True)
        
        print(f"\nScan completed successfully")
        print(f"  Total campgrounds found: {total_raw}")
        print(f"  Total campgrounds processed: {total_processed}")
        print(f"  Campgrounds inserted: {inserted}")
        print(f"  Campgrounds updated: {updated}")
        
        return 0
    except Exception as e:
        print(f"An error occurred in the main function: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    print(f"Exiting with code {exit_code}")
