import requests
print(requests.__version__)
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

def main(max_pages=None):
    """
    Main function to run the scraper.
    """
    campgrounds = []
    page = 1
    while True:
        print(f"[INFO] Fetching page {page}...")
        data = get_campgrounds(TEST_BOUNDS, page=page)
        if not data:
            break
        campgrounds.extend(data)
        page += 1

        if max_pages and page > max_pages:
            print(f"[INFO] Reached maximum number of pages ({max_pages}).")
            break
    print(f"[INFO] Total campgrounds fetched: {len(campgrounds)}")
    return campgrounds

if __name__ == "__main__":
    main(max_pages=2)