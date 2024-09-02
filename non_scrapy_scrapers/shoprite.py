import requests
import json
import logging
import re
from typing import Dict, List, Optional, Any
from parsel import Selector

class ShopRiteScraper:
    BASE_URL = "https://www.shoprite.com/sm/pickup/rsid/3000/store/"

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
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decoding error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error in fetch_and_parse_data method: {e}", exc_info=True)
            return None

    def extract_store_info(self, raw_store_data: dict) -> dict:
        """Extract relevant store information from raw data."""
        try:
            store_data = {
                'name': raw_store_data.get('name'),
                'number': raw_store_data.get('retailerStoreId'),
                'phone_number': raw_store_data.get('phone'),
                'address': self._get_address(raw_store_data),
                'hours': self._get_hours(raw_store_data),
                'location': self._get_location(raw_store_data.get('location', {})),
                'raw_dict': raw_store_data
            }

            for key, value in store_data.items():
                if value is None and key != 'raw_dict':
                    self.logger.warning(f"Missing {key} for store {store_data['name']}")

            return store_data
        except Exception as e:
            self.logger.error(f"Error extracting store info: {e}", exc_info=True)
            return {}

    def _get_address(self, raw_store_data: dict) -> str:
        """Get the formatted store address."""
        try:
            address_parts = [
                raw_store_data.get("addressLine1", ""),
                raw_store_data.get("addressLine2", ""),
                raw_store_data.get("addressLine3", "")
            ]
            street = ", ".join(filter(None, address_parts))

            city = raw_store_data.get("city", "")
            state = raw_store_data.get("countyProvinceState", "")
            zipcode = raw_store_data.get("postCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, raw_store_data: dict) -> dict:
        """Get the store location in GeoJSON Point format."""
        try:
            latitude = raw_store_data.get('latitude')
            longitude = raw_store_data.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}

    @staticmethod
    def normalize_hours_text(hours_text: str) -> str:
        """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
        return re.sub(r'[^a-z0-9]', '', hours_text.lower().replace('to', ''))

    @staticmethod
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(r'(\d+)([ap]m)', r'\1 \2', time_str)

    def _get_hours(self, raw_store_data: dict) -> dict:
        """Extract and parse store hours."""
        try:
            hours = raw_store_data.get("openingHours", "")
            if not hours:
                self.logger.warning(f"No hours found for store {raw_store_data.get('name', 'Unknown')}")
                return {}

            normalized_hours = self.normalize_hours_text(hours)
            return self._parse_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    def _parse_hours(self, hours_text: str) -> dict:
        """Parse normalized hours text into a structured format."""
        try:
            mon_sat_sun_pattern = r"mon(?:day)?-?sat(?:urday)?([\d]+\s?(?:am|pm))([\d]+\s?(?:am|pm))sun(?:day)?([\d]+\s?(?:am|pm))([\d]+\s?(?:am|pm))"
            all_week_pattern = r"mon(?:day)?-?sun(?:day)?([\d]+\s?(?:am|pm))([\d]+\s?(?:am|pm))"

            mon_sat_sun_match = re.search(mon_sat_sun_pattern, hours_text)
            all_week_match = re.search(all_week_pattern, hours_text)

            if mon_sat_sun_match:
                mon_sat_open, mon_sat_close, sun_open, sun_close = map(self.format_time, mon_sat_sun_match.groups())
                weekday_hours = {"open": mon_sat_open, "close": mon_sat_close}
                sunday_hours = {"open": sun_open, "close": sun_close}
            elif all_week_match:
                all_days_open, all_days_close = map(self.format_time, all_week_match.groups())
                weekday_hours = sunday_hours = {"open": all_days_open, "close": all_days_close}
            else:
                raise ValueError("Invalid input format")

            return {day: weekday_hours for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]} | {"sunday": sunday_hours}
        except Exception as e:
            self.logger.error(f"Error parsing hours: {e}", exc_info=True)
            return {}

    def scrape_stores(self) -> List[Dict[str, Any]]:
        self.logger.info("Starting to scrape stores")
        data = self.fetch_and_parse_data()
        if not data:
            return []

        stores = []
        all_stores = data.get('stores', {}).get('availablePlanningStores', {}).get('items', [])
        
        for store_data in all_stores:
            store = self.extract_store_info(store_data)
            stores.append(store)
        
        self.logger.info(f"Scraped {len(stores)} stores")
        return stores

    def save_to_file(self, stores: List[Dict[str, Any]], filename: str = 'data/shoprite_stores.json'):
        with open(filename, 'w') as f:
            json.dump(stores, f, indent=2)
        self.logger.info(f"Saved {len(stores)} stores to {filename}")

def main():
    scraper = ShopRiteScraper()
    stores = scraper.scrape_stores()
    
    # Print the scraped stores
    for store in stores:
        print(json.dumps(store, indent=2))

    # Save to file
    scraper.save_to_file(stores)

if __name__ == "__main__":
    main()