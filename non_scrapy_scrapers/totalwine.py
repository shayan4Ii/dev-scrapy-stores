import json
import logging
from datetime import datetime
from typing import Generator, Optional

import undetected_chromedriver as uc
from parsel import Selector

class TotalWineScraper:
    """Scraper for TotalWine store information."""

    start_url = "https://www.totalwine.com/store-finder/browse/AZ"

    def __init__(self):
        """Initialize the TotalWineScraper."""
        self.driver = uc.Chrome()
        self.logger = logging.getLogger(__name__)
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filename='logs/logs.log'
        )

    def get_state_codes(self, data: dict) -> list[str]:
        """Extract state codes from the given data."""
        try:
            states_list = data['search']['stores']['metadata']['states']
            return [state['stateIsoCode'] for state in states_list]
        except KeyError as e:
            self.logger.error(f"Error extracting state codes: {e}", exc_info=True)
            return []

    def get_json(self, driver: uc.Chrome) -> dict:
        """Extract JSON data from the page source."""
        try:
            sel = Selector(driver.page_source)
            script_data = sel.xpath('//script[contains(text(), "window.INITIAL_STATE")]/text()').get()
            if not script_data:
                raise ValueError("Script data not found")
            json_text = script_data.replace("window.INITIAL_STATE = ", "")
            return json.loads(json_text)
        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Error parsing JSON data: {e}", exc_info=True)
            return {}

    def get_stores_by_state(self, state_code: str) -> list[dict]:
        """Get parsed stores for a specific state."""
        try:
            self.driver.get(f"https://www.totalwine.com/store-finder/browse/{state_code}")
            data = self.get_json(self.driver)
            return self.get_parsed_stores(data)
        except Exception as e:
            self.logger.error(f"Error getting stores for state {state_code}: {e}", exc_info=True)
            return []

    def get_stores(self, data: dict) -> list[dict]:
        """Extract stores from the given data."""
        try:
            stores = data['search']['stores']['stores']
            self.logger.info(f"Retrieved {len(stores)} stores from data")
            return stores
        except KeyError as e:
            self.logger.error(f"Error extracting stores: {e}", exc_info=True)
            return []

    def get_parsed_stores(self, data: dict) -> list[dict]:
        """Parse all stores from the given data."""
        stores = self.get_stores(data)
        parsed_stores = []
        for store in stores:
            parsed_store = self.parse_store(store)
            if self._is_valid_store(parsed_store):
                parsed_stores.append(parsed_store)
            else:
                self.logger.warning(f"Skipping invalid store: {store.get('storeNumber', 'Unknown')}. Missing required fields.")
        
        self.logger.info(f"Parsed {len(parsed_stores)} valid stores out of {len(stores)} total stores")
        return parsed_stores

    def _is_valid_store(self, store: dict) -> bool:
        """Check if a store has all required fields."""
        required_fields = ['address', 'location', 'url', 'raw']
        missing_fields = [field for field in required_fields if not store.get(field)]
        if missing_fields:
            self.logger.warning(f"Store {store.get('number', 'Unknown')} is missing required fields: {', '.join(missing_fields)}")
        return not missing_fields

    def parse_store(self, store: dict) -> dict:
        """Parse individual store data."""
        parsed_store = {
            'number': store.get('storeNumber'),
            'name': store.get('name'),
            'phone_number': store.get('phoneFormatted'),
            'address': self._get_address(store),
            'location': self._get_location(store),
            'hours': self._get_hours(store),
            'url': f"https://www.totalwine.com/store-info/{store.get('storeNumber', '')}",
            'raw': store
        }
        
        # Log any missing non-required fields
        missing_fields = [field for field in parsed_store if parsed_store[field] is None]
        if missing_fields:
            self.logger.info(f"Store {store.get('storeNumber', 'Unknown')} is missing non-required fields: {', '.join(missing_fields)}")
        
        return parsed_store

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address1", ""),
                store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information for store: {store_info.get('storeNumber', 'Unknown')}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address for store {store_info.get('storeNumber', 'Unknown')}: {e}", exc_info=True)
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

            self.logger.warning(f"Missing latitude or longitude for store: {store_info.get('storeNumber', 'Unknown')}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values for store {store_info.get('storeNumber', 'Unknown')}: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location for store {store_info.get('storeNumber', 'Unknown')}: {error}", exc_info=True)
        return {}

    def _get_hours(self, store_info: dict) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}
            
            raw_hours = store_info.get("storeHours", {}).get("days", [])

            if not raw_hours:
                self.logger.warning(f"Missing store hours for store: {store_info.get('storeNumber', 'Unknown')}")
                return hours
            
            for day_hours in raw_hours:
                day = day_hours.get("dayOfWeek", "").lower()
                open_time = day_hours.get("openingTime", "").lower()
                close_time = day_hours.get("closingTime", "").lower()

                if not all([day, open_time, close_time]):
                    self.logger.info(f"Incomplete hours data for store {store_info.get('storeNumber', 'Unknown')} on {day}: open={open_time}, close={close_time}")

                hours[day] = {
                    "open": open_time,
                    "close": close_time
                }

            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours for store {store_info.get('storeNumber', 'Unknown')}: {e}", exc_info=True)
            return {}

    def scrape_stores(self) -> Generator[dict, None, None]:
        """Scrape stores from all states."""
        try:
            self.driver.get(self.start_url)
            data = self.get_json(self.driver)
            state_codes = self.get_state_codes(data)

            # Remove AZ from state_codes to avoid duplicate stores
            state_codes.remove("AZ")
            self.logger.info(f"Scraping stores for {len(state_codes) + 1} states")

            yield from self.get_parsed_stores(data)
            for state_code in state_codes:
                self.logger.info(f"Scraping stores for state: {state_code}")
                yield from self.get_stores_by_state(state_code)
        except Exception as e:
            self.logger.error(f"Error during store scraping: {e}", exc_info=True)

    def save_to_file(self, stores: list[dict], filename: Optional[str] = None) -> None:
        """Save scraped store data to a JSON file."""
        if filename is None:
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f'data/totalwine-{current_date}.json'
        
        try:
            with open(filename, 'w') as f:
                json.dump(stores, f, indent=2)
            self.logger.info(f"Saved {len(stores)} stores to {filename}")
        except IOError as e:
            self.logger.error(f"Error saving data to file: {e}", exc_info=True)

    def quit(self) -> None:
        """Quit the Selenium driver."""
        self.driver.quit()

if __name__ == "__main__":
    scraper = TotalWineScraper()
    stores = list(scraper.scrape_stores())
    scraper.save_to_file(stores)
    scraper.quit()