import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from typing import Dict, List, Any
import json
import jsonlines
import sqlite3
from datetime import datetime
import logging
import time
import os
from tqdm import tqdm
import argparse
from playsound import playsound

class ImprovedWalmartScraper:
    def __init__(self, output_file, error_log_file, db_file):
        self.base_url = "https://www.walmart.com"
        self.store_directory_url = f"{self.base_url}/store-directory"
        self.driver = self.setup_driver()
        self.logger = self.setup_logger()
        self.output_file = output_file
        self.error_log_file = error_log_file
        self.db_file = db_file
        self.conn = sqlite3.connect(self.db_file)
        self.setup_database()

    def setup_driver(self):
        return uc.Chrome()

    def setup_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def setup_database(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scrape_progress
            (store_id TEXT PRIMARY KEY, status TEXT)
        ''')
        self.conn.commit()

    def scrape_stores(self):
        store_ids = self.get_store_ids()
        store_ids = list(set(store_ids))
        for store_id in tqdm(store_ids, desc="Scraping stores"):
            if self.check_progress(store_id) == 'completed':
                continue
            try:
                store_data = self.scrape_store(store_id)
                if store_data:
                    self.save_store_data(store_data)
                    self.update_progress(store_id, 'completed')
            except Exception as e:
                self.logger.error(f"Error scraping store {store_id}: {str(e)}")
                self.log_error(store_id, str(e))
                self.update_progress(store_id, 'error')
                if 'blocked' in self.driver.current_url:
                    self.handle_blocked_url(store_id)
            time.sleep(0.5)

    def get_store_ids(self) -> List[str]:
        self.driver.get(self.store_directory_url)
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "__NEXT_DATA__"))
        )
        
        script_content = self.driver.find_element(By.ID, "__NEXT_DATA__").get_attribute('innerHTML')
        json_data = json.loads(script_content)
        
        stores_by_location_json = json_data["props"]["pageProps"]["bootstrapData"]["cv"]["storepages"]["_all_"]["sdStoresPerCityPerState"]
        stores_by_location = json.loads(stores_by_location_json.strip('"'))
        
        return self.extract_store_ids(stores_by_location)

    def extract_store_ids(self, stores_by_location: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        store_ids = []
        for state, cities in stores_by_location.items():
            for city_data in cities:
                stores = city_data.get('stores', [city_data])
                if not isinstance(stores, list):
                    self.logger.error(f"Stores data is not a list for city in state {state}: {city_data}")
                    continue
                for store in stores:
                    store_id = store.get('storeId') or store.get('storeid')
                    if store_id:
                        store_ids.append(str(store_id))
                    else:
                        self.logger.warning(f"No store ID found for store in state {state}: {store}")
        return store_ids

    def scrape_store(self, store_id: str) -> Dict[str, Any]:
        store_url = f"{self.base_url}/store/{store_id}"
        self.driver.get(store_url)
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "__NEXT_DATA__"))
        )
        
        script_content = self.driver.find_element(By.ID, "__NEXT_DATA__").get_attribute('innerHTML')
        json_data = json.loads(script_content)
        store_data = json_data['props']['pageProps']['initialData']['initialDataNodeDetail']['data']['nodeDetail']
        
        return self.parse_store_data(store_data)

    def parse_store_data(self, store_data: Dict[str, Any]) -> Dict[str, Any]:
        store_latitude, store_longitude = self.extract_geo_info(store_data['geoPoint'])
        
        return {
            "name": store_data['displayName'],
            "number": int(store_data['id']),
            "address": self.format_address(store_data['address']),
            "phone_number": store_data['phoneNumber'],
            "hours": self.format_hours(store_data['operationalHours']),
            "location": {
                "type": "Point",
                "coordinates": [store_longitude, store_latitude]
            },
            "services": [service['displayName'] for service in store_data['services']],
        }

    @staticmethod
    def format_address(address: Dict[str, str]) -> str:
        return f"{address['addressLineOne']}, {address['city']}, {address['state']} {address['postalCode']}"

    @staticmethod
    def extract_geo_info(geo_info: Dict[str, float]) -> tuple:
        return geo_info['latitude'], geo_info['longitude']

    def format_hours(self, operational_hours: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        formatted_hours = {}
        for day_hours in operational_hours:
            formatted_hours[day_hours['day'].lower()] = {
                "open": self.convert_to_12h_format(day_hours['start']),
                "close": self.convert_to_12h_format(day_hours['end'])
            }
        return formatted_hours

    @staticmethod
    def convert_to_12h_format(time_str: str) -> str:
        if not time_str:
            return time_str
        time_obj = datetime.strptime(time_str, '%H:%M').time()
        return time_obj.strftime('%I:%M %p').lower()

    def save_store_data(self, store_data: Dict[str, Any]):
        with jsonlines.open(self.output_file, mode='a') as writer:
            writer.write(store_data)

    def check_progress(self, store_id: str) -> str:
        cursor = self.conn.cursor()
        cursor.execute("SELECT status FROM scrape_progress WHERE store_id = ?", (store_id,))
        result = cursor.fetchone()
        return result[0] if result else 'not_started'

    def update_progress(self, store_id: str, status: str):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO scrape_progress (store_id, status)
            VALUES (?, ?)
        ''', (store_id, status))
        self.conn.commit()

    def log_error(self, store_id: str, error_message: str):
        with open(self.error_log_file, 'a') as f:
            json.dump({"store_id": store_id, "error": error_message, "timestamp": datetime.now().isoformat()}, f)
            f.write('\n')

    def handle_blocked_url(self, store_id: str):
        playsound('alert.mp3', block=False)
        print(f"\nStore {store_id} is blocked. Please solve the issue (e.g., CAPTCHA) and press Enter to continue...")
        input()

    def close(self):
        self.driver.quit()
        self.conn.close()

def main():
    parser = argparse.ArgumentParser(description="Improved Walmart Store Scraper")
    parser.add_argument("--output", default="walmart_stores.jsonl", help="Output file for scraped data")
    parser.add_argument("--error-log", default="error_log.json", help="Error log file")
    parser.add_argument("--db", default="scrape_progress.db", help="Database file for progress tracking")
    args = parser.parse_args()

    scraper = ImprovedWalmartScraper(args.output, args.error_log, args.db)
    try:
        scraper.scrape_stores()
    except KeyboardInterrupt:
        print("\nScraping interrupted. Progress has been saved.")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()

# python non_scrapy_scrapers/wallmart.py --output scraped_data.jsonl --error-log errors.json --db scrape_progress.db