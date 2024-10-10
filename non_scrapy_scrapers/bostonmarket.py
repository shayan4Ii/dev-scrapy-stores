import json
import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException


class BostonMarketScraper:
    """A scraper for Boston Market store information."""

    BASE_URL = "https://www.bostonmarket.com/store-location"

    # JavaScript to search for objects with 'storeNumber'
    JS_SCRIPT = """
    var foundObjects = [];
    var seenObjects = new WeakSet();
    function search(obj) {
        if (obj && typeof obj === 'object' && !seenObjects.has(obj)) {
            seenObjects.add(obj);
            if (obj.hasOwnProperty('storeNumber')) {
                foundObjects.push(obj);
            }
            for (var key in obj) {
                if (obj.hasOwnProperty(key)) {
                    try {
                        search(obj[key]);
                    } catch (e) {
                        // Handle potential exceptions from restricted properties
                    }
                }
            }
        }
    }
    search(window);
    return JSON.stringify(foundObjects);
    """

    # Logger configuration
    LOG_FORMAT = "[%(levelname)s] %(asctime)s %(name)s: %(message)s"
    LOG_FILE_PATH = "logs/logs.log"

    # File saving configuration
    DATA_FOLDER = "data"
    FILENAME_PATTERN = "bostonmarket-{date}.json"

    def __init__(self):
        """Initialize the BostonMarketScraper."""
        self.driver = None
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Set up and configure the logger."""
        logger = logging.getLogger("BostonMarketScraper")
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(self.LOG_FORMAT)

        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(self.LOG_FILE_PATH), exist_ok=True)

        # File handler to write logs to file
        file_handler = logging.FileHandler(self.LOG_FILE_PATH)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Stream handler to output logs to console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger

    def _setup_driver(self):
        """Set up the undetected Chrome driver."""
        try:
            self.driver = uc.Chrome()
        except WebDriverException as e:
            self.logger.error(f"Failed to initialize Chrome driver: {e}", exc_info=True)
            raise

    def _navigate_to_page(self):
        """Navigate to the Boston Market store location page."""
        try:
            self.driver.get(self.BASE_URL)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except TimeoutException:
            self.logger.error("Timeout while loading the page", exc_info=True)
            raise
        except WebDriverException as e:
            self.logger.error(f"Error navigating to {self.BASE_URL}: {e}", exc_info=True)
            raise

    def _execute_js_script(self) -> List[Dict[str, Any]]:
        """Execute the JavaScript to find store objects and return the result."""
        try:
            result = self.driver.execute_script(self.JS_SCRIPT)
            return json.loads(result)
        except WebDriverException as e:
            self.logger.error(f"Error executing JavaScript: {e}", exc_info=True)
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON result: {e}", exc_info=True)
            raise

    @staticmethod
    def _get_location(loc_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("lat")
            longitude = loc_info.get("lng")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)],
                }
        except (ValueError, TypeError) as error:
            logging.warning(f"Invalid latitude or longitude values: {error}")
        return {}


    def parse_store_objects(self, store_objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Parse the raw store objects and extract relevant information."""
        parsed_stores = []
        for store in store_objects:
            try:
                parsed_store = {
                    "store_number": store.get("storeNumber"),
                    "name": store.get("name"),
                    "address": store.get("address").replace(', USA', ''),
                    "location": self._get_location(store.get("position")),
                    "permanently_closed": store.get("status", "") == "Permanent closed",
                    "raw": store,
                }
                parsed_stores.append(parsed_store)
            except Exception as e:
                self.logger.error(f"Error parsing store object: {e}", exc_info=True)
                self.logger.error(f"Problematic store object: {store}")
        
        self.logger.info(f"Parsed {len(parsed_stores)} store objects")
        return parsed_stores

    def scrape_stores(self) -> List[Dict[str, Any]]:
        """Scrape store information from Boston Market website."""
        self.logger.info("Starting to scrape Boston Market stores")
        try:
            self._setup_driver()
            self._navigate_to_page()
            input("Solve the CAPTCHA and press Enter to continue...")
            raw_store_objects = self._execute_js_script()
            
            self.logger.info(f"Found {len(raw_store_objects)} raw store objects")
            parsed_stores = self.parse_store_objects(raw_store_objects)
            return parsed_stores
        except Exception as e:
            self.logger.error(f"Error during scraping: {e}", exc_info=True)
            return []
        finally:
            if self.driver:
                self.driver.quit()
            self.logger.info("Finished scraping stores")

    def save_to_file(self, stores: List[Dict[str, Any]], filename: Optional[str] = None) -> None:
        """Save scraped store data to a JSON file."""
        if filename is None:
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"{self.DATA_FOLDER}/{self.FILENAME_PATTERN.format(date=current_date)}"

        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        try:
            with open(filename, "w") as f:
                json.dump(stores, f, indent=2)
            self.logger.info(f"Saved {len(stores)} stores to {filename}")
        except IOError as e:
            self.logger.error(f"Error saving data to file: {e}", exc_info=True)


def main():
    """Main function to run the Boston Market scraper."""
    scraper = BostonMarketScraper()
    stores = scraper.scrape_stores()
    scraper.save_to_file(stores)


if __name__ == "__main__":
    main()