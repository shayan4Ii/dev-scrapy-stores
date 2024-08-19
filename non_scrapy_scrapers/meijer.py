import requests
import json
import logging
from typing import Dict, List, Optional, Any

class MeijerScraper:
    BASE_URL = "https://www.meijer.com/bin/meijer/store/search?locationQuery={}&radius=20"

    def __init__(self, zipcode_file: str = 'zipcodes.json'):
        self.zipcode_file = zipcode_file
        self.logger = self._setup_logger()
        self.session = self._setup_session()

    def _setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _setup_session(self):
        session = requests.Session()
        session.headers.update(self.get_default_headers())
        return session

    def get_default_headers(self) -> Dict[str, str]:
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "max-age=0",
            "dnt": "1",
            "priority": "u=0, i",
            "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
        }

    def read_zipcodes(self) -> List[str]:
        try:
            with open(self.zipcode_file, 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"File not found: {self.zipcode_file}")
            return []
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON file: {self.zipcode_file}")
            return []

        zipcodes = []
        for location in locations:
            zipcodes.extend(location.get('zip_codes', []))
        return zipcodes

    def fetch_and_parse_data(self, zipcode: str) -> Optional[Dict[str, Any]]:
        url = self.BASE_URL.format(zipcode)
        self.logger.info(f"Fetching data from {url}")
        try:
            response = self.session.get(url)
            response.raise_for_status()
            data = response.json()
            self.logger.info("Data fetched and parsed successfully")
            return data
        except requests.RequestException as e:
            self.logger.error(f"An error occurred while fetching the page: {e}")
            return None
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON response: {response.text[:100]}...")
            return None

    def scrape_stores(self) -> List[Dict[str, Any]]:
        self.logger.info("Starting to scrape stores")
        zipcodes = self.read_zipcodes()
        stores = []

        for zipcode in zipcodes:
            data = self.fetch_and_parse_data(zipcode)
            if not data:
                continue

            all_stores = data.get('pointsOfService', [])
            for store_data in all_stores:
                # store = self.parse_store(store_data, zipcode)
                store = store_data
                stores.append(store)

            # Break after processing one zipcode for demonstration
            # break

        self.logger.info(f"Scraped {len(stores)} stores")
        return stores

    def save_to_file(self, stores: List[Dict[str, Any]], filename: str = 'data/meijer_stores.json'):
        with open(filename, 'w') as f:
            json.dump(stores, f, indent=2)
        self.logger.info(f"Saved {len(stores)} stores to {filename}")

def main():
    scraper = MeijerScraper()
    stores = scraper.scrape_stores()
    
    # Print the scraped stores
    for store in stores:
        print(json.dumps(store, indent=2))

    # Save to file
    scraper.save_to_file(stores)

if __name__ == "__main__":
    main()