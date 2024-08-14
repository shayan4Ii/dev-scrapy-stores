import requests
import json
import logging
from typing import Dict, List, Optional, Any
from parsel import Selector

class DearbornScraper:
    BASE_URL = "https://www.dearbornmarket.com/sm/planning/rsid/632/store/"

    def __init__(self):
        self.url = self.BASE_URL
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
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })
        return session

    def fetch_and_parse_data(self) -> Optional[Dict[str, Any]]:
        self.logger.info(f"Fetching data from {self.url}")
        try:
            response = self.session.get(self.url)
            response.raise_for_status()
            
            sel = Selector(response.text)
            script = sel.xpath('//script[contains(., "window.__PRELOADED_STATE__=")]/text()').get()
            
            if not script:
                self.logger.error("Could not find the __PRELOADED_STATE__ data")
                return None

            json_text = script.split('window.__PRELOADED_STATE__=')[1].strip()
            data = json.loads(json_text)
            self.logger.info("Data fetched and parsed successfully")
            return data
        except requests.RequestException as e:
            self.logger.error(f"An error occurred while fetching the page: {e}")
            return None

    @staticmethod
    def parse_store(store: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": store.get('name'),
            "address": f"{store.get('address', {}).get('addressLine1', '')}, {store.get('address', {}).get('city', '')}, {store.get('address', {}).get('state', '')} {store.get('address', {}).get('zip', '')}",
            "phone_number": store.get('phone'),
            "store_id": store.get('id'),
            "latitude": store.get('latitude'),
            "longitude": store.get('longitude'),
            "hours": store.get('hours'),
        }

    def scrape_stores(self) -> List[Dict[str, Any]]:
        self.logger.info("Starting to scrape stores")
        data = self.fetch_and_parse_data()
        if not data:
            return []

        stores = []
        all_stores = data.get('stores', {}).get('availablePlanningStores', {}).get('items', [])
        
        for store_data in all_stores:
            store = self.parse_store(store_data)
            stores.append(store)
        
        self.logger.info(f"Scraped {len(stores)} stores")
        return stores

    def save_to_file(self, stores: List[Dict[str, Any]], filename: str = 'data/dearborn_stores.json'):
        with open(filename, 'w') as f:
            json.dump(stores, f, indent=2)
        self.logger.info(f"Saved {len(stores)} stores to {filename}")

def main():
    scraper = DearbornScraper()
    stores = scraper.scrape_stores()
    
    # Print the scraped stores
    for store in stores:
        print(json.dumps(store, indent=2))

    # Save to file
    scraper.save_to_file(stores)

if __name__ == "__main__":
    main()