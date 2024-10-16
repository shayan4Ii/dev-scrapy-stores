from datetime import datetime
import re
import requests
import json
import logging
from typing import Dict, Generator, List, Optional, Any
from parsel import Selector

class SmartAndFinalScraper:
    BASE_URL = "https://www.smartandfinal.com/sm/delivery/rsid/522/store/"

    def __init__(self):
        """Initialize the SmartAndFinalScraper."""
        self.url = self.BASE_URL
        self.logger = self._setup_logger()
        self.session = self._setup_session()

    def _setup_logger(self) -> logging.Logger:
        """Set up and configure the logger."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _setup_session(self) -> requests.Session:
        """Set up and configure the requests session."""
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

    def fetch_and_parse_data(self) -> Optional[dict[str, Any]]:
        """Fetch and parse data from the SmartAndFinal website."""
        self.logger.info(f"Fetching data from {self.url}")
        try:
            response = self.session.get(self.url)
            response.raise_for_status()
            
            selector = Selector(response.text)
            script = selector.xpath('//script[contains(., "window.__PRELOADED_STATE__=")]/text()').get()
            
            if not script:
                self.logger.error("Could not find the __PRELOADED_STATE__ data")
                return None

            json_text = script.split('window.__PRELOADED_STATE__=')[1].strip()
            data = json.loads(json_text)
            self.logger.info("Data fetched and parsed successfully")
            return data
        except requests.RequestException as e:
            self.logger.error(f"An error occurred while fetching the page: {e}")
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decoding error: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in fetch_and_parse_data method: {e}", exc_info=True)
        return None

    def extract_store_info(self, raw_store_data: dict) -> dict[str, Any]:
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

    def _get_location(self, raw_store_data: dict) -> dict[str, Any]:
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
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(r'(\d+)([ap]m)', r'\1 \2', time_str)

    @staticmethod
    def normalize_hours_text(hours_text: str) -> str:
        """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
        return re.sub(r'[^a-z0-9:]', '', hours_text.lower().replace('to', '').replace('thru', ''))

    def _get_hours(self, raw_store_data: dict) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = raw_store_data.get("openingHours", "")
            if not hours:
                self.logger.warning(f"No hours found for store {raw_store_data.get('name', 'Unknown')}")
                return {}

            normalized_hours = self.normalize_hours_text(hours)
            return self._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    def _parse_business_hours(self, input_text: str) -> dict[str, dict[str, str]]:
        """Parse business hours from input text."""
        DAY_MAPPING = {
            'sun': 'sunday', 'mon': 'monday', 'tue': 'tuesday', 'wed': 'wednesday',
            'thu': 'thursday', 'fri': 'friday', 'sat': 'saturday',
        }
        result = {day: {'open': None, 'close': None} for day in DAY_MAPPING.values()}

        if input_text == "open24hours":
            return {day: {'open': '12:00 am', 'close': '11:59 pm'} for day in DAY_MAPPING.values()}
        elif 'open24hours' in input_text:
            input_text = input_text.replace('open24hours', '12:00am11:59pm')

        # Extract and process day ranges
        day_ranges = self._extract_business_hour_range(input_text)
        for start_day, end_day, open_time, close_time in day_ranges:
            start_index = list(DAY_MAPPING.keys()).index(start_day)
            end_index = list(DAY_MAPPING.keys()).index(end_day)
            if end_index < start_index:  # Handle cases like "Saturday to Sunday"
                end_index += 7
            for i in range(start_index, end_index + 1):
                day = list(DAY_MAPPING.keys())[i % 7]
                full_day = DAY_MAPPING[day]
                if result[full_day]['open'] and result[full_day]['close']:
                    self.logger.debug(f"Day {full_day} already has hours({input_text=}), skipping range {start_day} to {end_day}")
                    continue
                result[full_day]['open'] = open_time
                result[full_day]['close'] = close_time

        # Extract and process individual days (overwriting any conflicting day ranges)
        single_days = self._extract_business_hours(input_text)
        for day, open_time, close_time in single_days:
            full_day = DAY_MAPPING[day]
            if result[full_day]['open'] and result[full_day]['close']:
                self.logger.debug(f"Day {full_day} already has hours({input_text=}), skipping individual day {day}")
                continue
            result[full_day]['open'] = open_time
            result[full_day]['close'] = close_time

        # Log warning for any missing days
        for day, hours in result.items():
            if hours['open'] is None or hours['close'] is None:
                self.logger.warning(f"Missing hours for {day}({input_text=})")

        return result

    def _extract_business_hour_range(self, input_string: str) -> list[tuple[str, str, str, str]]:
        """Extract business hour ranges from input string."""
        days_re = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
        day_suffix_re = r"(?:day)?"
        optional_colon_re = r"(?::)?"
        time_re = r"(\d{1,2}(?::\d{2})?)([ap]m)"

        time_only_re = f"^{time_re}{time_re}$"
        
        if "daily" in input_string:
            time_match = re.search(f"{time_re}{time_re}", input_string)
            if time_match:
                open_time = f"{time_match.group(1)} {time_match.group(2)}"
                close_time = f"{time_match.group(3)} {time_match.group(4)}"
                return [("sun", "sat", open_time, close_time)]
        
        time_only_match = re.match(time_only_re, input_string)
        if re.match(time_only_re, input_string):
            open_time = f"{time_only_match.group(1)} {time_only_match.group(2)}"
            close_time = f"{time_only_match.group(3)} {time_only_match.group(4)}"
            return [("sun", "sat", open_time, close_time)]

        pattern = f"({days_re}{day_suffix_re})({days_re}{day_suffix_re}){optional_colon_re}?{time_re}{time_re}"
        matches = re.finditer(pattern, input_string, re.MULTILINE)
        
        results = []
        for match in matches:
            start_day = match.group(1)[:3]
            end_day = match.group(2)[:3]
            open_time = f"{match.group(3)} {match.group(4)}"
            close_time = f"{match.group(5)} {match.group(6)}"
            results.append((start_day, end_day, open_time, close_time))
        
        return results

    def _extract_business_hours(self, input_string: str) -> list[tuple[str, str, str]]:
        """Extract individual business hours from input string."""
        days_re = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
        day_suffix_re = r"(?:day)?"
        optional_colon_re = r"(?::)?"
        time_re = r"(\d{1,2}(?::\d{2})?)([ap]m)"
        
        pattern = f"({days_re}{day_suffix_re}){optional_colon_re}?{time_re}{time_re}"
        matches = re.finditer(pattern, input_string, re.MULTILINE)
        
        results = []
        for match in matches:
            day = match.group(1)[:3]
            open_time = f"{match.group(2)} {match.group(3)}"
            close_time = f"{match.group(4)} {match.group(5)}"
            results.append((day, open_time, close_time))
        
        return results

    def scrape_stores(self) -> Generator[dict[str, Any], None, None]:
        """Scrape store information and yield each store's data."""
        self.logger.info("Starting to scrape stores")
        data = self.fetch_and_parse_data()
        if not data:
            return

        all_stores = data.get('stores', {}).get('availablePlanningStores', {}).get('items', [])
        
        for store_data in all_stores:
            try:
                store = self.extract_store_info(store_data)
                yield store
            except Exception as e:
                self.logger.error(f"Error processing store data: {e}", exc_info=True)

        self.logger.info("Finished scraping stores")

    def save_to_file(self, stores: list[dict[str, Any]], filename: Optional[str] = None) -> None:
        """Save scraped store data to a JSON file."""
        if filename is None:
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f'data/smartandfinal-{current_date}.json'
        
        try:
            with open(filename, 'w') as f:
                json.dump(stores, f, indent=2)
            self.logger.info(f"Saved {len(stores)} stores to {filename}")
        except IOError as e:
            self.logger.error(f"Error saving data to file: {e}", exc_info=True)


def main():
    scraper = SmartAndFinalScraper()
    stores = list(scraper.scrape_stores())
    
    # Print the scraped stores
    for store in stores:
        print(json.dumps(store, indent=2))

    # Save to file
    scraper.save_to_file(stores)

if __name__ == "__main__":
    main()