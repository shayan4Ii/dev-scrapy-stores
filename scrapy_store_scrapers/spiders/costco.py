import re
import json
from typing import Generator
from urllib.parse import quote

import scrapy
from scrapy.http import Response

class InvalidJsonResponseException(Exception):
    pass

class CostcoSpider(scrapy.Spider):
    name = "costco"
    allowed_domains = ["www.costco.com"]
            # discard stores which don't have required fields
    required_fields = ['address', 'location', 'url', 'raw']
    
    custom_settings = {
        'CONCURRENT_REQUESTS': 1,
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,  # Number of retries

        'RETRY_EXCEPTIONS': ['scrapy.exceptions.DropItem', 'json.JSONDecodeError', 'InvalidJsonResponseException']
    }

    API_FORMAT_URL = "https://www.costco.com/AjaxWarehouseBrowseLookupView?langId=-1&numOfWarehouses=50&hasGas=false&hasTires=false&hasFood=false&hasHearing=false&hasPharmacy=false&hasOptical=false&hasBusiness=false&hasPhotoCenter=&tiresCheckout=0&isTransferWarehouse=false&populateWarehouseDetails=true&warehousePickupCheckout=false&latitude={}&longitude={}&countryCode=US"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_store_ids = set()

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Read the JSON file containing latitude and longitude data and generate requests."""
        # Read the JSON file containing latitude and longitude data
        try:
            with open(r'data\tacobell_zipcode_data.json', 'r') as f:
                locations = json.load(f)
        except FileNotFoundError:
            self.logger.error("File not found: data/tacobell_zipcode_data.json")
            return
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON file: data/tacobell_zipcode_data.json")
            return
        
        # Iterate through the locations and yield requests
        for location in locations:
            latitude = location['latitude']
            longitude = location['longitude']

            if not latitude or not longitude:
                self.logger.warning("Invalid latitude or longitude: %s, %s", latitude, longitude)
                continue

            url = self.API_FORMAT_URL.format(latitude, longitude)
            
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                headers=self.get_default_headers()
            )
            break

    def parse(self, response: Response) -> Generator[dict, None, None]:
        """Parse the response and yield warehouse data."""
        try:
            response_json = response.json()
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON response: %s (%s)", response.text, response.url)
            
            raise InvalidJsonResponseException(f"Invalid JSON response from {response.url}")

        for warehouse in response_json:

            store_info = {}

            if not isinstance(warehouse, dict):
                self.logger.warning("Invalid warehouse data: %s", warehouse)
                continue

            warehouse_id = warehouse.get('identifier')
            
            if warehouse_id in self.seen_store_ids:
                self.logger.info(f"Duplicate store found: {warehouse_id}")
                continue
            
            self.seen_store_ids.add(warehouse_id)


            store_info['number'] = warehouse_id
            store_info['name'] = warehouse.get('locationName', '').strip()

            store_info['phone_number'] = warehouse.get('phone', '').strip()
            store_info['address'] = self._get_address(warehouse)
            store_info['location'] = self._get_location(warehouse)

            store_info['services'] = self._get_services(warehouse)
            store_info['hours'] = self._get_hours(warehouse)

            store_info['url'] = self._generate_warehouse_url(warehouse)

            store_info['raw'] = warehouse

            # discard stores which don't have required fields

            self.logger.info(f"Store info: {store_info}")

            yield store_info

    def _generate_warehouse_url(self, warehouse_dict):
        base_url = "https://www.costco.com/warehouse-locations/"
        
        # Extract and process location name and city
        location_name = warehouse_dict.get('locationName', '').lower()
        city = warehouse_dict.get('city', '').lower()
        location_slug = f"{location_name}-{city}"
        
        # Clean the slug (remove special characters and replace spaces with hyphens)
        location_slug = re.sub(r'[^a-z0-9\s-]', '', location_slug)
        location_slug = re.sub(r'\s+', '-', location_slug.strip())
        
        # Get state and store ID
        state = warehouse_dict.get('state', '').lower()
        store_id = warehouse_dict.get('stlocID') or warehouse_dict.get('identifier', '')
        
        # Construct the URL
        url = f"{base_url}{location_slug}-{state}-{store_id}.html"
        
        # Ensure the URL is properly encoded
        return quote(url, safe=':/')

    def _get_services(self, store_info: dict) -> list[str]:
        try:
            services = store_info.get("coreServices", [])
            return [service.get("name") for service in services]
        except Exception as e:
            self.logger.error(f"Error getting services: {e}", exc_info=True)

    def _get_address(self, store_info: dict) -> str:
        try:
            address_parts = [
                store_info.get("address1", "")
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> dict:
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
            hours = ' '.join(raw_store_data.get("warehouseHours", []))
            if not hours:
                self.logger.warning(f"No hours found for store {raw_store_data}")
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

    @staticmethod
    def get_default_headers():
        """Get the default headers for the request."""
        return {
            "accept": "application/json, text/plain, */*",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "en-US,en;q=0.9",
            "connection": "keep-alive",
            "dnt": "1",
            "referer": "https://www.costco.com/warehouse-locations",
            "sec-ch-ua": '"Chromium";v="112", "Google Chrome";v="112"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "upgrade-insecure-requests": "1"
        }