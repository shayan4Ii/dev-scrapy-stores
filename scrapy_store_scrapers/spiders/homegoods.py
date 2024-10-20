import re
import json
from typing import Any, Dict, List, Optional, Tuple, Generator
from urllib.parse import quote

import scrapy
from scrapy.http import FormRequest, Response

class HomegoodsSpider(scrapy.Spider):
    """Spider for scraping HomeGoods store information."""

    name = "homegoods"
    allowed_domains = ["www.homegoods.com"]
    start_urls = ['https://www.homegoods.com/locator']

    # Class variables for configuration
    ZIPCODE_FILE_PATH = "data/tacobell_zipcode_data.json"
    STORE_URL_TEMPLATE = "https://www.homegoods.com/store-details/{}/{}"
    
    # Regular expressions
    LOCATION_DATA_RE = r'var locationData = ({.*?});'
    TIME_FORMAT_RE = r'(\d+)([ap]m)'
    NORMALIZE_HOURS_RE = r'[^a-z0-9:]'
    
    # Day mapping
    DAY_MAPPING = {
        'sun': 'sunday', 'mon': 'monday', 'tue': 'tuesday', 'wed': 'wednesday',
        'thu': 'thursday', 'fri': 'friday', 'sat': 'saturday',
    }

    # Request headers
    REQUEST_HEADERS = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'content-type': 'application/x-www-form-urlencoded',
        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_store_numbers: set[str] = set()

    def parse(self, response: Response) -> Generator[FormRequest, None, None]:
        """Parse the initial response and generate requests for each zipcode."""
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            yield FormRequest.from_response(
                response,
                formdata={
                    'lat': str(zipcode["latitude"]),
                    'lng': str(zipcode["longitude"]),
                },
                headers=self.REQUEST_HEADERS,
                callback=self.parse_results,
            )

    def _load_zipcode_data(self) -> list[dict[str, Any]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.ZIPCODE_FILE_PATH, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s", self.ZIPCODE_FILE_PATH)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file: %s", self.ZIPCODE_FILE_PATH)
        return []

    def parse_results(self, response: Response) -> Generator[Dict[str, Any], None, None]:
        """Parse the results from each FormRequest."""
        script = response.xpath('//script[contains(text(), "var locationData =")]/text()').get()
        if script:
            json_match = re.search(self.LOCATION_DATA_RE, script, re.DOTALL)
            if json_match:
                location_data = json.loads(json_match.group(1))
                stores = location_data.get('Stores', [])
                yield from self._process_stores(stores)
            else:
                self.logger.warning("No location data found in script")
        else:
            self.logger.warning("No script containing location data found")

    def _process_stores(self, stores: list[dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
        """Process each store in the list of stores."""
        for store in stores:
            store_number = store.get("StoreID")
            if store_number not in self.processed_store_numbers:
                self.processed_store_numbers.add(store_number)
                item = self._parse_store(store)
                if self._validate_item(item):
                    yield item
            else:
                self.logger.debug("Duplicate store found: %s", store_number)

    def _parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        """Parse individual store data."""
        return {
            "number": store.get("StoreID"),
            "name": store.get("Name"),
            "phone_number": store.get("Phone"),
            "address": self._get_address(store),
            "location": self._get_location(store),
            "services": self._get_services(store),
            "hours": self._get_hours(store),
            "url": self._get_url(store),
            "raw": store
        }

    def _validate_item(self, item: dict[str, Any]) -> bool:
        """Validate the item has all required fields."""
        required_fields = ["address", "location", "url", "raw"]
        for field in required_fields:
            if not item.get(field):
                self.logger.warning("Missing required field: %s", field)
                return False
        return True

    def _get_services(self, store_info: dict[str, Any]) -> list[str]:
        """Extract store services."""
        departments = store_info.get("Departments", [])
        return [department.get("Desc") for department in departments if department.get("Desc")]

    def _get_address(self, store_info: dict[str, Any]) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("Address", ""),
                store_info.get("Address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("City", "")
            state = store_info.get("State", "")
            zipcode = store_info.get("Zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information: %s", store_info)
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("Latitude")
            longitude = loc_info.get("Longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)],
                }
        except (ValueError, TypeError) as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        return {}

    @staticmethod
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(HomegoodsSpider.TIME_FORMAT_RE, r'\1 \2', time_str)

    @staticmethod
    def normalize_hours_text(hours_text: str) -> str:
        """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
        return re.sub(HomegoodsSpider.NORMALIZE_HOURS_RE, '', hours_text.lower().replace('to', '').replace('thru', ''))

    def _get_hours(self, raw_store_data: dict[str, Any]) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = raw_store_data.get("Hours", "")
            if not hours:
                self.logger.warning("No hours found for store %s", raw_store_data.get('StoreID', 'Unknown'))
                return {}

            normalized_hours = self.normalize_hours_text(hours)
            return self._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error("Error getting store hours: %s", e, exc_info=True)
            return {}

    def _parse_business_hours(self, input_text: str) -> dict[str, dict[str, Optional[str]]]:
        """Parse business hours from input text."""
        result = {day: {'open': None, 'close': None} for day in self.DAY_MAPPING.values()}

        if input_text == "open24hours":
            return {day: {'open': '12:00 am', 'close': '11:59 pm'} for day in self.DAY_MAPPING.values()}
        elif 'open24hours' in input_text:
            input_text = input_text.replace('open24hours', '12:00am11:59pm')

        day_ranges = self._extract_business_hour_range(input_text)
        for start_day, end_day, open_time, close_time in day_ranges:
            start_index = list(self.DAY_MAPPING.keys()).index(start_day)
            end_index = list(self.DAY_MAPPING.keys()).index(end_day)
            if end_index < start_index:
                end_index += 7
            for i in range(start_index, end_index + 1):
                day = list(self.DAY_MAPPING.keys())[i % 7]
                full_day = self.DAY_MAPPING[day]
                if result[full_day]['open'] and result[full_day]['close']:
                    self.logger.debug("Day %s already has hours (input_text=%s), skipping range %s to %s",
                                      full_day, input_text, start_day, end_day)
                    continue
                result[full_day]['open'] = open_time
                result[full_day]['close'] = close_time

        single_days = self._extract_business_hours(input_text)
        for day, open_time, close_time in single_days:
            full_day = self.DAY_MAPPING[day]
            if result[full_day]['open'] and result[full_day]['close']:
                self.logger.debug("Day %s already has hours (input_text=%s), skipping individual day %s",
                                  full_day, input_text, day)
                continue
            result[full_day]['open'] = open_time
            result[full_day]['close'] = close_time

        for day, hours in result.items():
            if hours['open'] is None or hours['close'] is None:
                self.logger.warning("Missing hours for %s (input_text=%s)", day, input_text)

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

    def _get_url(self, store_info: dict[str, Any]) -> str:
        """Generate the store URL."""
        store_id = store_info.get("StoreID")
        city = store_info.get("City", "").replace(" ", "-")
        state = store_info.get("State", "")
        zipcode = store_info.get("Zip", "")
        
        if store_id and city and state and zipcode:
            # Create the URL-friendly city-state-zip part
            location = quote(f"{city}-{state}-{zipcode}")
            return self.STORE_URL_TEMPLATE.format(location, store_id)
        else:
            self.logger.warning("Missing information for store URL: %s", store_info)
            return ""