import json
import logging
import os
import signal
import sqlite3
import sys
import time
from datetime import datetime
from typing import Generator, Optional

import coloredlogs
import jsonlines
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

coloredlogs.install()

class HEBStoreLocator:
    """HEB store locator scraper."""

    BASE_URL = "https://www.heb.com/store-locations"
    API_URL = "https://www.heb.com/_next/data/{}/store-locations.json"

    def __init__(self, zipcode_file: str = 'zipcodes.json', db_file: str = 'scraper_progress.db'):
        """Initialize the HEB store locator."""
        self.zip_codes = self._load_zipcodes(zipcode_file)
        self.store_ids = set()
        self.db_file = db_file
        self.build_id: Optional[str] = None
        self.driver: Optional[uc.Chrome] = None
        self.interrupted = False
        self.output_file = f'data/heb-{datetime.now().strftime("%Y%m%d")}.jsonl'
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)

        self.logger = logging.getLogger(__name__)
        self._setup_logging()
        self._init_db()
        signal.signal(signal.SIGINT, self._signal_handler)

    def _setup_logging(self):
        """Set up logging configuration."""
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = logging.FileHandler("logs/heb_store_locator.log")
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _init_db(self):
        """Initialize the SQLite database."""
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS zipcode_status
                     (zipcode TEXT PRIMARY KEY, status TEXT)''')
        conn.commit()
        conn.close()

    def _signal_handler(self, signum, frame):
        """Handle interruption signals."""
        self.logger.info("Interrupt received. Saving state and exiting gracefully...")
        self.interrupted = True
        sys.exit(0)

    def _get_build_id(self) -> str:
        """Get the build ID from the HEB website."""
        self.logger.info("Getting build ID...")
        try:
            self.driver = uc.Chrome()
            self.driver.get('https://google.com')
            self.driver.get(f"{self.BASE_URL}?address=78204&page=1")
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "__NEXT_DATA__")))
            script = self.driver.find_element(By.ID, "__NEXT_DATA__")
            data = json.loads(script.get_attribute('innerHTML'))
            build_id = data.get('buildId', '')
            self.logger.info(f"Build ID obtained: {build_id}")
            return build_id
        except Exception as e:
            self.logger.error(f"Error getting build ID: {str(e)}", exc_info=True)
            raise

    def _fetch_stores(self, zip_code: str, page: int) -> dict:
        """Fetch stores data from API using self.driver."""
        for attempt in range(3):
            try:
                self.logger.info(f"Fetching stores for ZIP: {zip_code}, Page: {page}")
                url = f"{self.API_URL.format(self.build_id)}?address={zip_code}&page={page}"
                self.driver.get(url)
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                data = json.loads(self.driver.find_element(By.TAG_NAME, "pre").text)
                return data
            except Exception as e:
                self.logger.error(f"Error fetching stores (attempt {attempt + 1}): {str(e)}", exc_info=True)
                if attempt < 2:
                    self.logger.info("Retrying in 10 seconds...")
                    time.sleep(10)
                else:
                    raise

    def _parse_store(self, store: dict) -> dict:
        """Parse store information."""
        store_info = {
            'number': str(store.get('storeNumber', '')),
            'name': store.get('name', ''),
            'phone': store.get('phoneNumber', ''),
            'address': self._get_address(store.get('address', {})),
            'location': self._get_location(store),
            'hours': self._get_hours(store.get('storeHours', [])),
            'services': self._get_services(store),
            'url': self._get_url(store),
            'raw': store
        }
        return store_info

    def _get_address(self, address_info: dict) -> str:
        """Format the store address from store information."""
        try:
            street = address_info.get("streetAddress", "").strip()
            city = address_info.get("locality", "").strip()
            state = address_info.get("region", "").strip()
            zipcode = address_info.get("postalCode", "").strip()

            city_state_zip = f"{city}, {state} {zipcode}".strip()
            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

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

    def _get_hours(self, week_hours: list) -> dict:
        """Get store hours."""
        hours_info = {}
        for day_info in week_hours:
            day = day_info.get('day', '').lower()
            if not day:
                self.logger.warning("Missing day information in store hours")
                continue
            hours_info[day] = {
                "open": self._convert_to_12h_format(day_info.get('opens')),
                "close": self._convert_to_12h_format(day_info.get('closes'))
            }
        return hours_info

    def _get_services(self, store_info: dict) -> list:
        """Get store services."""
        for area in store_info.get('areas', []):
            if area.get('name', '').lower() == 'pharmacy':
                return [feature.get('name', '') for feature in area.get('features', [])]
        return []

    def _get_url(self, store_info: dict) -> str:
        """Generate store URL."""
        try:
            address = store_info['address']
            country = address['country']
            region = address['region'].lower()
            locality = address['locality'].lower()
            name = store_info['name']
            store_number = store_info['storeNumber']

            formatted_store_name = name.lower().replace(' ', '-')
            return f"https://www.heb.com/heb-store/{country}/{region}/{locality}/{formatted_store_name}-{store_number}"
        except KeyError as e:
            self.logger.warning(f"Missing key while generating URL: {e}")
            return ""

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return time_str

    def process_zip_code(self, zip_code: str):
        """Process a single ZIP code."""
        self.logger.info(f"Processing ZIP code: {zip_code}")
        page = 1
        total_stores = 0
        while not self.interrupted:
            try:
                data = self._fetch_stores(zip_code, page)
                stores_data = data.get("pageProps", {}).get("currentPageStores", [])
                
                if not stores_data:
                    self.logger.info(f"No more stores found for ZIP: {zip_code}")
                    break

                for store_data in stores_data:
                    store = store_data['store']
                    if store['storeNumber'] not in self.store_ids:
                        self.store_ids.add(store['storeNumber'])
                        parsed_store = self._parse_store(store)
                        self._save_store(parsed_store)
                        self.logger.info(f"Added store: {store['storeNumber']}")
                    else:
                        self.logger.info(f"Skipped duplicate store: {store['storeNumber']}")
                    total_stores += 1

                total_stores_count = data.get("pageProps", {}).get("totalStoresCount", 0)
                self.logger.info(f"Total stores count: {total_stores}/{total_stores_count}")
                if total_stores >= total_stores_count:
                    self.logger.info(f"Finished processing ZIP: {zip_code}")
                    break
                
                page += 1
            except Exception as e:
                self.logger.error(f"Error processing ZIP code {zip_code}: {str(e)}", exc_info=True)
                break

        self._update_zipcode_status(zip_code, 'completed')

    def find_stores(self):
        """Main method to find stores."""
        self.logger.info("Starting to find stores...")
        self._load_store_ids()
        self.build_id = self._get_build_id()
        
        for zip_code in self.zip_codes:
            if self.interrupted:
                break
            if self._get_zipcode_status(zip_code) != 'completed':
                self.process_zip_code(zip_code)
                self.logger.info(f"Finished processing ZIP: {zip_code}")
            else:
                self.logger.info(f"Skipping already processed ZIP: {zip_code}")
            
        self.logger.info(f"Found {len(self.store_ids)} unique stores.")

    def _save_store(self, store: dict):
        """Save store information to file."""
        try:
            os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
            with jsonlines.open(self.output_file, mode='a') as writer:
                writer.write(store)
        except Exception as e:
            self.logger.error(f"Error saving store: {str(e)}", exc_info=True)

    def _load_store_ids(self):
        """Load previously saved store IDs."""
        if os.path.exists(self.output_file):
            try:
                with jsonlines.open(self.output_file) as reader:
                    for store in reader:
                        self.store_ids.add(store['number'])
                self.logger.info(f"Loaded {len(self.store_ids)} previously saved store IDs")
            except Exception as e:
                self.logger.error(f"Error loading store IDs: {str(e)}", exc_info=True)

    def _get_zipcode_status(self, zipcode: str) -> str:
        """Get the status of a ZIP code from the database."""
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("SELECT status FROM zipcode_status WHERE zipcode = ?", (zipcode,))
            result = c.fetchone()
            conn.close()
            return result[0] if result else 'pending'
        except Exception as e:
            self.logger.error(f"Error getting ZIP code status: {str(e)}", exc_info=True)
            return 'pending'

    def _update_zipcode_status(self, zipcode: str, status: str):
        """Update the status of a ZIP code in the database."""
        try:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            c.execute("INSERT OR REPLACE INTO zipcode_status (zipcode, status) VALUES (?, ?)",
                      (zipcode, status))
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error updating ZIP code status: {str(e)}", exc_info=True)

    def _load_zipcodes(self, zipcode_file: str) -> list:
        """Load zipcodes from the JSON file."""
        try:
            with open(zipcode_file, 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            self.logger.error(f"File not found: {zipcode_file}")
            raise FileNotFoundError(f"File not found: {zipcode_file}")
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON file: {zipcode_file}")
            raise ValueError(f"Invalid JSON file: {zipcode_file}")
        
        return [zip_code for location in locations for zip_code in location.get('zip_codes', [])]

    def close_browser(self):
        """Close the browser."""
        if self.driver:
            self.driver.quit()

def main():
    """Main function to run the HEB store locator."""
    logging.info("Starting HEB store locator")
    locator = HEBStoreLocator()
    try:
        locator.find_stores()
        logging.info(f"Found {len(locator.store_ids)} unique stores.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
    finally:
        locator.close_browser()
    logging.info("HEB store locator finished")

if __name__ == "__main__":
    main()