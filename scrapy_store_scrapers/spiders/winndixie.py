import re
import json
from typing import Any, Generator

import scrapy
from scrapy.http import Request, Response


class WinnDixieSpider(scrapy.Spider):
    """Spider for scraping WinnDixie store locations."""

    name = "winndixie"
    allowed_domains = ["www.winndixie.com"]

    custom_settings = {
        'ITEM_PIPELINES': {
            'scrapy_store_scrapers.pipelines.WinnDixieDuplicatesPipeline': 300,
        },
    }

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests for the spider."""
        url = "https://www.winndixie.com/V2/storelocator/getStores"

        try:
            zipcodes = self.load_zipcodes("zipcodes.json")
        except (FileNotFoundError, ValueError) as e:
            self.logger.error(f"Error loading zipcodes: {str(e)}")
            return
        
        for zipcode in zipcodes:
            zipcode = '33165'
            data = {
                "search": zipcode,
                "strDefaultMiles": "25",
                "filter": ""
            }

            yield Request(
                method='POST',
                headers=self.get_headers(),
                url=url,
                body=json.dumps(data),
                callback=self.parse
            )
            break

    def parse(self, response: Response) -> Generator[dict[str, Any], None, None]:
        """Parse the response and yield filtered store data."""
        try:
            stores = response.json()
            # filtered_stores = self.filter_stores(stores)
            for store in stores:
                yield self.parse_store(store)
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON from response: {response.url}")


    def parse_store(self, store_info: dict) -> dict[str, Any]:
        """Parse the store details from the response."""
        parsed_store = {}
        
        parsed_store['number'] = str(store_info.get('StoreCode'))
        parsed_store['name'] = store_info.get('StoreName')
        parsed_store['phone_number'] = store_info.get('Phone')

        parsed_store['address'] = self._get_address(store_info.get('Address', {}))
        parsed_store['location'] = self._get_location(store_info.get('Location', {}))
        parsed_store['hours'] = self._get_hours(store_info)
        parsed_store['services'] = self._get_services(store_info)

        parsed_store['url'] = self._get_url(store_info)
        parsed_store['raw'] = store_info
        
        return parsed_store
    
    def _get_services(self, store_info: dict[str, Any]) -> list[str]:
        """Extract and return services offered by the store."""
        services = store_info.get("departmentList", "")
        return services.split(",") if services else []

    @staticmethod
    def _get_url(store_info):
        base_url = "https://www.winndixie.com/storedetails"
        city = store_info['Address']['City'].lower()
        state = store_info['Address']['State'].lower()
        zipcode = store_info['Address']['Zipcode']
        store_code = store_info['StoreCode']
        
        url = f"{base_url}/{city}/{state}?search={store_code}&zipcode={zipcode}"
        
        return url

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
            hours = raw_store_data.get("WorkingHours", "")
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

    def _get_address(self, address_info: dict[str, Any]) -> str:
        """Get the formatted store address."""
        try:
            address_parts = [
                address_info.get("AddressLine1", ""),
                address_info.get("AddressLine2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("City", "")
            state = address_info.get("State", "")
            zipcode = address_info.get("Zipcode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address for store with address {address_info}")
            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
            return ""

    def _get_location(self, location_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = location_info.get('Latitude')
            longitude = location_info.get('Longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude for store with location info {location_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}


    @staticmethod
    def load_zipcodes(zipcode_file: str) -> list[str]:
        """Load zipcodes from the JSON file."""
        with open(zipcode_file, 'r') as f:
            locations = json.load(f)
        
        zipcodes = set()
        for location in locations:
            zipcodes.update(location.get('zip_codes', []))
        return list(zipcodes)

    @staticmethod
    def filter_stores(stores: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Filter stores based on criteria of website to show on frontend."""
        return [
            store for store in stores
            if (
                'liquor' not in store['Location']['LocationTypeDescription'].lower()
                or (store['StoreCode'] == '1489' and not store['ParentStore'])
            )
        ]

    @staticmethod
    def get_headers() -> dict[str, str]:
        """Return headers for the HTTP request."""
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json;charset=UTF-8",
            "origin": "https://www.winndixie.com",
            "referer": "https://www.winndixie.com/locator",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        }