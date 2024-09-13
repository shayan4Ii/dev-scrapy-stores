import json
import re
from datetime import datetime
from typing import Optional, Generator

import scrapy
from scrapy.http import Response


class AcehardwareSpider(scrapy.Spider):
    """Spider for scraping Ace Hardware store information."""

    name = "acehardware"
    allowed_domains = ["www.acehardware.com"]
    start_urls = ["https://www.acehardware.com/store-directory"]

    STORE_URLS_XPATH = '//div[@id="store-directory-list"]/div/div/div/a/@href'
    STORE_JSON_XPATH = '//script[@id="data-mz-preload-store"]/text()'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the store directory and follow links to individual stores."""
        store_urls = response.xpath(self.STORE_URLS_XPATH).getall()

        for url in store_urls:
            yield response.follow(url, self.parse_store)

    def parse_store(self, response: Response) -> Generator[dict, None, None]:
        """Parse individual store data and yield structured information."""
        try:
            store_json = response.xpath(self.STORE_JSON_XPATH).get()
            store_data = self.parse_json(store_json)

            if not store_data:
                self.logger.warning(f"Failed to parse JSON data for URL: {response.url}")
                return

            parsed_store = {
                "number": store_data.get('StoreNumber'),
                "name": store_data.get('StoreName'),
                "phone_number": store_data.get("Phone"),
                "address": self._get_address(store_data),
                "location": self._get_location(store_data),
                "hours": self._get_hours(store_data),
                "services": self._get_services(store_data),
                "url": response.url,
                "raw": store_data
            }

            # Discard items missing required fields
            required_fields = ["address", "location", "url", "raw"]
            if all(parsed_store.get(field) for field in required_fields):
                yield parsed_store
            else:
                self.logger.warning(f"Discarded item due to missing required fields: {parsed_store}")

        except Exception as e:
            self.logger.error(f"Error parsing store data: {e}", exc_info=True)

    def parse_json(self, json_string: Optional[str]) -> Optional[dict]:
        """Parse JSON string and handle potential errors."""
        if not json_string:
            return None
        try:
            return json.loads(json_string)
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON: {e}", exc_info=True)
            return None

    def _get_services(self, raw_store_data: dict) -> list[str]:
        """Extract and parse store services."""
        try:
            services = raw_store_data.get("Services", [])
            if not services:
                self.logger.warning(f"No services found for store {raw_store_data.get('StoreName', 'Unknown')}")
                return []

            return [service.get("CustomTitleDesc") for service in services if service.get('CustomTitleDesc')]
        except Exception as e:
            self.logger.error(f"Error getting store services: {e}", exc_info=True)
            return []

    @staticmethod
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(r'(\d+)([ap]m)', r'\1 \2', time_str)

    @staticmethod
    def normalize_hours_text(hours_text: str) -> str:
        """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
        return re.sub(r'[^a-z0-9:]', '', hours_text.lower().replace('to', '').replace('thru', ''))

    def _get_hours(self, raw_store_data: dict) -> dict[str, dict[str, Optional[str]]]:
        """Extract and parse store hours."""
        try:
            hours_raw = raw_store_data.get("RegularHours", {})
            if not hours_raw:
                self.logger.warning(f"No hours found for store {raw_store_data.get('StoreName', 'Unknown')}")
                return {}

            hours = {}

            for day, day_hours in hours_raw.items():
                if not isinstance(day_hours, dict):
                    continue

                open_time = self._convert_to_12h_format(day_hours.get("openTime", ""))
                close_time = self._convert_to_12h_format(day_hours.get("closeTime", ""))

                if not open_time or not close_time:
                    self.logger.warning(f"Missing open or close time for {day} hours: {day_hours}")
                    hours[day] = {"open": None, "close": None}
                else:
                    hours[day] = {"open": open_time, "close": close_time}

            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            return time_str

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("StoreAddressLn1", ""),
                # store_info.get("street2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("StoreCityNm", "")
            state = store_info.get("StoreStateCd", "")
            zipcode = store_info.get("StoreZipCd", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information: {store_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('Latitude')
            longitude = store_info.get('Longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store with location info: {store_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}