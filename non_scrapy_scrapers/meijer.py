import json
import requests
import sqlite3
import logging
from typing import List, Dict, Any
from requests.exceptions import RequestException
from datetime import datetime

from tqdm import tqdm

class InvalidJsonResponseException(Exception):
    """Custom exception for invalid JSON responses."""
    pass

class MeijerScraper:
    API_FORMAT_URL = "https://www.meijer.com/bin/meijer/store/search?locationQuery={}&radius=20"

    def __init__(self, zipcode_file: str = 'zipcodes.json', db_file: str = 'meijer_progress.db'):
        self.zipcode_file = zipcode_file
        self.db_file = db_file
        self.setup_logging()
        self.setup_database()

    def setup_logging(self):
        """Set up error logging to write to a file."""
        logging.basicConfig(
            filename='meijer_scraper.log',
            level=logging.ERROR,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def setup_database(self):
        """Set up SQLite database to track scraping progress."""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scrape_progress (
                    zipcode TEXT PRIMARY KEY,
                    status TEXT,
                    last_updated TIMESTAMP
                )
            ''')
            conn.commit()

    def get_default_headers(self) -> Dict[str, str]:
        """Return default headers for requests."""
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

    def load_zipcodes(self) -> List[str]:
        """Load zipcodes from the JSON file."""
        try:
            with open(self.zipcode_file, 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            logging.error(f"File not found: {self.zipcode_file}")
            raise FileNotFoundError(f"File not found: {self.zipcode_file}")
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON file: {self.zipcode_file}")
            raise ValueError(f"Invalid JSON file: {self.zipcode_file}")
        
        zipcodes = []
        for location in locations:
            zipcodes.extend(location.get('zip_codes', []))
        return zipcodes

    def update_progress(self, zipcode: str, status: str):
        """Update the progress of a zipcode in the database."""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO scrape_progress (zipcode, status, last_updated)
                VALUES (?, ?, ?)
            ''', (zipcode, status, datetime.now()))
            conn.commit()

    def get_progress(self, zipcode: str) -> str:
        """Get the progress status of a zipcode from the database."""
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT status FROM scrape_progress WHERE zipcode = ?', (zipcode,))
            result = cursor.fetchone()
            return result[0] if result else 'not_started'

    def scrape_store(self, zipcode: str) -> None:
        """Scrape store data for a given zipcode."""
        if self.get_progress(zipcode) == 'completed':
            print(f"Zipcode {zipcode} already scraped. Skipping.")
            return

        self.update_progress(zipcode, 'in_progress')
        url = self.API_FORMAT_URL.format(zipcode)
        try:
            response = requests.get(url, headers=self.get_default_headers())
            response.raise_for_status()
            response_json = response.json()
        except RequestException as e:
            logging.error(f"Error fetching data for zipcode {zipcode}: {str(e)}")
            self.update_progress(zipcode, 'error')
            return
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON response for zipcode {zipcode}")
            self.update_progress(zipcode, 'error')
            return

        pagination = response_json.get('pagination', {})
        total_results = pagination.get('totalResults', 0)
        page_size = pagination.get('pageSize', 0)

        if total_results > page_size:
            logging.warning(f"Results are paginated. Total results: {total_results}, page size: {page_size}")
            # TODO: Implement pagination handling if needed

        stores = response_json.get('pointsOfService', [])
        for store in stores:
            # store['source_zipcode'] = zipcode
            self.save_store(store)

        self.update_progress(zipcode, 'completed')

    def save_store(self, store: Dict[str, Any]):
        """Save a single store to the JSON file."""
        with open('meijer_stores.json', 'a') as f:
            json.dump(store, f)
            f.write('\n')

    def scrape_stores(self) -> None:
        """Scrape stores for all zipcodes."""
        zipcodes = self.load_zipcodes()
        for zipcode in zipcodes:
            self.scrape_store(zipcode)

    def resume_scraping(self) -> None:
        """Resume scraping from where it left off."""
        zipcodes = self.load_zipcodes()
        for zipcode in tqdm(zipcodes):
            status = self.get_progress(zipcode)
            if status in ['not_started', 'error']:
                self.scrape_store(zipcode)
            elif status == 'in_progress':
                logging.warning(f"Zipcode {zipcode} was in progress. Re-scraping to ensure completeness.")
                self.scrape_store(zipcode)

def main():
    scraper = MeijerScraper()
    try:
        scraper.resume_scraping()
    except KeyboardInterrupt:
        print("\nScraping interrupted. Progress has been saved.")
        logging.info("Scraping interrupted by user.")

if __name__ == "__main__":
    main()