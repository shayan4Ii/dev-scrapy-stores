import argparse
import json
import logging
import re
from datetime import datetime
from typing import Any, Generator

import requests

class SaveMartScraperException(Exception):
    """Custom exception for SaveMartScraper."""

class SaveMartScraper:
    """A class to scrape store information from the SaveMart website."""

    def __init__(self, base_url: str = "https://savemart.com", log_level: str = "INFO"):
        """Initialize the SaveMartScraper."""
        self.base_url = base_url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        error_handler = logging.FileHandler('scraper_errors.log')
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        self.logger.addHandler(error_handler)

        self.chain_id = self._get_chain_id()

    def _get_chain_id(self) -> str:
        """Get the chain ID from the SaveMart website."""
        try:
            response = self.session.get(f"{self.base_url}/stores")
            response.raise_for_status()

            chain_id_match = re.search(r'"chainId":"(.*?)"', response.text)
            if not chain_id_match:
                raise SaveMartScraperException("Chain ID not found")
            
            return chain_id_match.group(1)
        except requests.RequestException as error:
            self.logger.error(f"Error fetching chain ID: {str(error)}")
            raise SaveMartScraperException(f"Error fetching chain ID: {str(error)}")

    def scrape_stores(self) -> list[dict[str, Any]]:
        """Main method to scrape store information."""
        self.logger.info("Starting to scrape stores...")
        stores = list(self._get_all_stores())
        self.logger.info(f"Finished scraping. Total stores retrieved: {len(stores)}")
        return stores

    def _get_all_stores(self) -> Generator[dict[str, Any], None, None]:
        """Fetch all stores from the SaveMart API."""
        query = {
            "query": "\n    query allStores($chainId: UUID!) {\n  stores(chainId: $chainId) {\n    all {\n      ...StoreEntryFragment\n    }\n  }\n}\n    \n    fragment StoreEntryFragment on StoreEntry {\n  accessibility\n  address1\n  address2\n  amenities\n  chainId\n  city\n  country\n  departmentDetails {\n    ...StoreDetailsFragment\n  }\n  isOpenNow\n  language\n  latitude\n  longitude\n  number\n  payments\n  postalCode\n  primaryDetails {\n    ...StoreDetailsFragment\n  }\n  shoppingModes\n  state\n  storeId\n  swiftlyServices\n  timeZone\n  ... on FDStoreEntry {\n    isHybrid\n  }\n}\n    \n\n    fragment StoreDetailsFragment on StoreDetails {\n  description\n  hours {\n    ...StoreHoursDailyFragment\n  }\n  images {\n    imageId\n    purpose\n    storeId\n    url\n  }\n  name\n  nextStatus {\n    asOfDate\n    status\n  }\n  contactNumbers {\n    displayName\n    imageId\n    intention\n    value\n  }\n  status {\n    asOfDate\n    status\n  }\n  websites {\n    description\n    value\n  }\n  phoneNumbers {\n    description\n    value\n  }\n}\n    \n\n    fragment StoreHoursDailyFragment on StoreHoursDaily {\n  day\n  hours {\n    close\n    open\n  }\n}\n    ",
            "variables": {
                "chainId": self.chain_id
            }
        }

        try:
            response = self.session.post('https://sm.swiftlyapi.net/graphql', json=query)
            response.raise_for_status()
            data = response.json()
            raw_stores = data['data']['stores']['all']
            
            for store in raw_stores:
                yield self.parse_store(store)

        except requests.RequestException as error:
            self.logger.error(f"Error fetching stores: {str(error)}")
            raise SaveMartScraperException(f"Error fetching stores: {str(error)}")

    def parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        """Parse raw store data into a structured format."""
        parsed_store = {}

        parsed_store["name"] = store.get("primaryDetails", {}).get("name", "")
        parsed_store["number"] = str(store.get("number", ""))
        parsed_store["phone_number"] = self._get_phone_number(store)

        parsed_store["address"] = self._get_address(store)
        parsed_store["location"] = self._get_location(store)
        parsed_store["hours"] = self._get_hours(store)

        parsed_store['url'] = f"https://savemart.com/stores/{store.get('storeId', '')}"

        parsed_store['raw'] = store

        return parsed_store

    def _get_phone_number(self, store_info: dict[str, Any]) -> str:
        """Get the store phone number."""
        try:
            phone_numbers = store_info.get("primaryDetails", {}).get("phoneNumbers", [])
            if phone_numbers:
                return phone_numbers[0].get("value", "")
            self.logger.warning(f"No phone number found for store {store_info.get('number', 'unknown')}")
            return ""
        except Exception as error:
            self.logger.error(f"Error getting phone number: {error}", exc_info=True)
            return ""

    def _get_address(self, store_info: dict[str, Any]) -> str:
        """Get the formatted store address."""
        try:
            address_parts = [
                store_info.get("address1", ""),
                store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address for store {store_info.get('number', 'unknown')}")
            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude for store {store_info.get('number', 'unknown')}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}

    def _get_hours(self, store_info: dict[str, Any]) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            day_index_map = {
                0: "sunday",
                1: "monday",
                2: "tuesday",
                3: "wednesday",
                4: "thursday",
                5: "friday",
                6: "saturday"
            }

            hours = store_info.get('primaryDetails', {}).get('hours', [])
            formatted_hours = {}
            for hour in hours:
                day = day_index_map.get(hour.get('day', -1), 'unknown')

                hours_list = hour.get('hours', [])

                if not hours_list:
                    self.logger.warning(f"Missing hours for {day} in store {store_info.get('number', 'unknown')}")
                    continue
                elif len(hours_list) > 1:
                    self.logger.warning(f"Multiple hours for {day} in store {store_info.get('number', 'unknown')}")
                
                hour_dict = hours_list[0]
                
                open_time = hour_dict.get('open')
                close_time = hour_dict.get('close')
                formatted_hours[day] = {
                    "open": self._convert_to_12h_format(open_time),
                    "close": self._convert_to_12h_format(close_time)
                }
            return formatted_hours
        except Exception as error:
            self.logger.error(f"Error getting store hours: {error}", exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            time_obj = datetime.strptime(time_str, '%H:%M:%S').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return time_str

    def save_to_file(self, data: list[dict[str, Any]], filename: str | None = None) -> None:
        """Save scraped data to a JSON file with the current date in the filename."""
        if filename is None:
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f"data/savemart-{current_date}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.info(f"Data successfully saved to {filename}")
        except IOError as error:
            self.logger.error(f"Error saving data to file: {str(error)}")
            raise SaveMartScraperException(f"Error saving data to file: {str(error)}")

def main() -> None:
    """Main function to run the scraper."""
    parser = argparse.ArgumentParser(description="SaveMart Store Scraper")
    parser.add_argument("--output", help="Output file name")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")
    args = parser.parse_args()

    scraper = SaveMartScraper(log_level=args.log_level)
    stores = scraper.scrape_stores()
    
    print(json.dumps(stores, indent=2))
    
    scraper.save_to_file(stores, args.output)

if __name__ == "__main__":
    main()