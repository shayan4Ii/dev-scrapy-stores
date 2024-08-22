import json
import logging
from typing import List, Dict
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import coloredlogs
import time
import os
import signal
import sys
import sqlite3
import jsonlines

coloredlogs.install()

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("logs/heb_store_locator.log"),
                        logging.StreamHandler()
                    ])

def convert_to_12h_format(time_str):
    t = datetime.strptime(time_str, '%H:%M').time()
    return t.strftime('%I:%M %p').lstrip('0')

class HEBStoreLocator:
    BASE_URL = "https://www.heb.com/store-locations"
    API_URL = "https://www.heb.com/_next/data/{}/store-locations.json"

    def __init__(self, zipcode_file: str = 'zipcodes.json', db_file: str = 'scraper_progress.db', output_file: str = 'data/heb_stores.jsonl'):
        self.zip_codes = self.load_zipcodes(zipcode_file)
        self.store_ids = set()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Sec-Ch-Ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Dnt': '1',
            'Priority': 'u=0, i',
            'Referer': 'https://www.heb.com/store-locations'
        }
        
        self.db_file = db_file
        self.output_file = output_file
        self.build_id = None
        self.driver = None
        self.interrupted = False

        self.init_db()
        signal.signal(signal.SIGINT, self.signal_handler)

    def init_db(self):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS zipcode_status
                     (zipcode TEXT PRIMARY KEY, status TEXT)''')
        conn.commit()
        conn.close()

    def signal_handler(self, signum, frame):
        logging.info("Interrupt received. Saving state and exiting gracefully...")
        self.interrupted = True
        sys.exit(0)

    def get_build_id(self) -> str:
        logging.info("Getting build ID...")
        self.driver = uc.Chrome()
        self.driver.get('https://google.com')
        self.driver.get(f"{self.BASE_URL}?address=78204&page=1")
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "__NEXT_DATA__")))
        script = self.driver.find_element(By.ID, "__NEXT_DATA__")
        data = json.loads(script.get_attribute('innerHTML'))
        build_id = data.get('buildId', '')
        logging.info(f"Build ID obtained: {build_id}")
        return build_id
    
    def fetch_stores(self, zip_code: str, page: int) -> Dict:
        """Fetch stores data from API using self.driver"""
        for attempt in range(3):
            try:
                params = {"address": zip_code, "page": page}
                logging.info(f"Fetching stores for ZIP: {zip_code}, Page: {page}")
                url = self.API_URL.format(self.build_id)
                url = f"{url}?address={zip_code}&page={page}"
                self.driver.get(url)
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "pre")))
                data = json.loads(self.driver.find_element(By.TAG_NAME, "pre").text)
                return data
            except Exception as e:
                logging.error(f"Error fetching stores (attempt {attempt + 1}): {str(e)}")
                if attempt < 2:
                    logging.info("Retrying in 10 seconds...")
                    time.sleep(10)
                else:
                    raise

    def process_zip_code(self, zip_code: str):
        logging.info(f"Processing ZIP code: {zip_code}")
        page = 1
        total_stores = 0
        while True:
            if self.interrupted:
                break
            data = self.fetch_stores(zip_code, page)
            stores_data = data.get("pageProps", {}).get("currentPageStores", [])
            
            if not stores_data:
                logging.info(f"No more stores found for ZIP: {zip_code}")
                break

            for store_data in stores_data:
                store = store_data['store']
                if store['storeNumber'] not in self.store_ids:
                    self.store_ids.add(store['storeNumber'])
                    self.save_store(store)
                    logging.info(f"Added store: {store['storeNumber']}")
                else:
                    logging.info(f"Skipped duplicate store: {store['storeNumber']}")
                total_stores += 1

            total_stores_count = data.get("pageProps", {}).get("totalStoresCount", 0)
            logging.info(f"Total stores count: {total_stores}/{total_stores_count}")
            if total_stores >= total_stores_count:
                logging.info(f"Finished processing ZIP: {zip_code}")
                break
            
            page += 1

        self.update_zipcode_status(zip_code, 'completed')

    def find_stores(self):
        logging.info("Starting to find stores...")
        self.load_store_ids()
        self.build_id = self.get_build_id()
        
        for zip_code in self.zip_codes:
            if self.interrupted:
                break
            if self.get_zipcode_status(zip_code) != 'completed':
                self.process_zip_code(zip_code)
                logging.info(f"Finished processing ZIP: {zip_code}")
            else:
                logging.info(f"Skipping already processed ZIP: {zip_code}")
            
        logging.info(f"Found {len(self.store_ids)} unique stores.")

    def save_store(self, store):
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with jsonlines.open(self.output_file, mode='a') as writer:
            writer.write(store)

    def load_store_ids(self):
        if os.path.exists(self.output_file):
            with jsonlines.open(self.output_file) as reader:
                for store in reader:
                    self.store_ids.add(store['storeNumber'])
            logging.info(f"Loaded {len(self.store_ids)} previously saved store IDs")

    def get_zipcode_status(self, zipcode):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("SELECT status FROM zipcode_status WHERE zipcode = ?", (zipcode,))
        result = c.fetchone()
        conn.close()
        return result[0] if result else 'pending'

    def update_zipcode_status(self, zipcode, status):
        conn = sqlite3.connect(self.db_file)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO zipcode_status (zipcode, status) VALUES (?, ?)",
                  (zipcode, status))
        conn.commit()
        conn.close()

    def load_zipcodes(self, zipcode_file) -> List[str]:
        """Load zipcodes from the JSON file."""
        try:
            with open(zipcode_file, 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            logging.error(f"File not found: {zipcode_file}")
            raise FileNotFoundError(f"File not found: {zipcode_file}")
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON file: {zipcode_file}")
            raise ValueError(f"Invalid JSON file: {zipcode_file}")
        
        zipcodes = []
        for location in locations:
            zipcodes.extend(location.get('zip_codes', []))
        return zipcodes

    def close_browser(self):
        if self.driver:
            self.driver.quit()

if __name__ == "__main__":
    logging.info("Starting HEB store locator")
    locator = HEBStoreLocator()
    try:
        locator.find_stores()
        logging.info(f"Found {len(locator.store_ids)} unique stores.")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        locator.close_browser()
    logging.info("HEB store locator finished")