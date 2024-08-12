import requests
from parsel import Selector
import json
from datetime import datetime
import time
from urllib.parse import urljoin
from collections import deque
from typing import List, Optional, Dict, Any
import logging
from scrapy_store_scrapers.items import WalmartStoreItem

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

START_URL = 'https://www.walmart.com/store-directory'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0'
}

def convert_to_12h_format(time_str):
    t = datetime.strptime(time_str, '%H:%M').time()
    return t.strftime('%I:%M %p').lstrip('0')

class WalmartScraper:
    def __init__(self, start_url: str, request_headers: dict):
        self.start_url = start_url
        self.request_headers = request_headers
        self.visited_urls = set()
        self.store_urls = set()
        self.scraped_stores_data = []

    @staticmethod
    def extract_geo_info(geo_info: Dict[str, float]) -> tuple:
        """Extract latitude and longitude from geo info."""
        return geo_info['latitude'], geo_info['longitude']

    @staticmethod
    def format_address(address: Dict[str, str]) -> str:
        """Format the store address."""
        return f"{address['addressLineOne']}, {address['city']}, {address['state']} {address['postalCode']}"

    def format_hours(self, operational_hours: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Format the store operational hours."""
        formatted_hours = {}
        for day_hours in operational_hours:
            formatted_hours[day_hours['day'].lower()] = {
                "open": self.convert_to_12h_format(day_hours['start']),
                "close": self.convert_to_12h_format(day_hours['end'])
            }
        return formatted_hours

    @staticmethod
    def convert_to_12h_format(time_str: str) -> str:
        """Convert 24-hour time format to 12-hour format."""
        if not time_str:
            return time_str
        time_obj = datetime.strptime(time_str, '%H:%M').time()
        return time_obj.strftime('%I:%M %p').lower()

    def get_page(self, url: str) -> Optional[Selector]:
        try:
            response = requests.get(url, headers=self.request_headers, timeout=10)
            response.raise_for_status()
            logger.info(f"Successfully fetched page: {url} (Status Code: {response.status_code})")
            return Selector(response.text)
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    @staticmethod
    def is_store_directory(url: str) -> bool:
        return 'store-directory' in url

    def get_urls(self, selector: Selector, base_url: str) -> List[str]:
        urls = selector.xpath('//h1[contains(text(), "Walmart Store Directory")]/following-sibling::div//div/a[contains(@href, "store")]/@href').getall()
        return [urljoin(base_url, url) for url in urls]

    def scrape_store_urls(self):
        logger.info(f"Starting to scrape store URLs from {self.start_url}")
        selector = self.get_page(self.start_url)
        if selector is None:
            logger.error("Failed to get the store directory page")
            return

        try:
            script_content = selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
            if not script_content:
                raise ValueError("Script content not found")
            
            json_data = json.loads(script_content)

            stores_by_location_json = json_data["props"]["pageProps"]["bootstrapData"]["cv"]["storepages"]["_all_"]["sdStoresPerCityPerState"]
            stores_by_location = json.loads(stores_by_location_json.strip('"'))

            store_ids = self.extract_store_ids(stores_by_location)
            logger.info(f"Found {len(store_ids)} store IDs")

            for store_id in store_ids:
                store_url = f"https://www.walmart.com/store/{store_id}"
                self.store_urls.add(store_url)

        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in scrape_store_urls: {str(e)}")
        except KeyError as e:
            logger.error(f"Key error in scrape_store_urls: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in scrape_store_urls: {str(e)}")

        logger.info(f"Finished scraping store URLs. Total URLs found: {len(self.store_urls)}")

    def extract_store_ids(self, stores_by_location: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        """Extract store IDs from the stores by location data."""
        store_ids = []

        for state, cities in stores_by_location.items():
            for city_data in cities:
                stores = city_data.get('stores', [city_data])
                if not isinstance(stores, list):
                    logger.error(f"Stores data is not a list for city in state {state}: {city_data}")
                    continue

                for store in stores:
                    store_id = store.get('storeId') or store.get('storeid')
                    if store_id:
                        store_ids.append(str(store_id))
                    else:
                        logger.warning(f"No store ID found for store in state {state}: {store}")

        return store_ids

    def scrape_store_details(self, store_url: str):
        logger.info(f"Scraping store details from: {store_url}")
        store_data = self._extract_store_data(store_url)
        if store_data is None:
            logger.warning(f"Failed to extract store data from: {store_url}")
            return None

        self._save_raw_store_data(store_data)
        formatted_data = self._format_store_output(store_data, store_url)
        logger.info(f"Successfully scraped store: {formatted_data['name']}")
        return formatted_data

    def _extract_store_data(self, store_url: str):
        selector = self.get_page(store_url)
        if selector is None:
            return None

        script = selector.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        if script is None:
            return None

        data = json.loads(script)
        return data['props']['pageProps']['initialData']['initialDataNodeDetail']['data']['nodeDetail']

    def _save_raw_store_data(self, store_data: dict):
        save_path = f"{store_data['id']}.json"
        with open(save_path, 'w') as f:
            json.dump(store_data, f, indent=2)

    def _format_store_output(self, store_data: dict, store_url: str) -> WalmartStoreItem:
        store_latitude, store_longitude = self.extract_geo_info(store_data['geoPoint'])
        
        return WalmartStoreItem(
            name=store_data['displayName'],
            number=int(store_data['id']),
            address=self.format_address(store_data['address']),
            phone_number=store_data['phoneNumber'],
            hours=self.format_hours(store_data['operationalHours']),
            location={
                "type": "Point",
                "coordinates": [store_longitude, store_latitude]
            },
            services=[service['displayName'] for service in store_data['services']],
        )


    def run(self):
        logger.info("Starting Walmart scraper")
        self.scrape_store_urls()
        logger.info(f"Total store URLs found: {len(self.store_urls)}")
        self._scrape_all_store_details()
        logger.info("Walmart scraper finished")

    def _scrape_all_store_details(self):
        logger.info("Starting to scrape individual store details")
        for index, store_url in enumerate(self.store_urls, 1):
            logger.info(f"Scraping store {index} of {len(self.store_urls)}")
            store_data = self.scrape_store_details(store_url)
            if store_data:
                self.scraped_stores_data.append(store_data)
            time.sleep(1)  # Be respectful to the server
        logger.info(f"Finished scraping details for {len(self.scraped_stores_data)} stores")

    def save_data(self, filename: str):
        logger.info(f"Saving scraped data to {filename}")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump([dict(item) for item in self.scraped_stores_data], f, indent=2)
        logger.info(f"Data successfully saved to {filename}")

def main():
    scraper = WalmartScraper(START_URL, HEADERS)
    scraper.run()
    scraper.save_data('walmart_stores_data.json')

if __name__ == "__main__":
    main()
