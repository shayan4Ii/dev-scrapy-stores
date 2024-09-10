import json
from datetime import datetime
from typing import Generator

import scrapy
from scrapy.http import Response


class SchnucksSpider(scrapy.Spider):
    """Spider to scrape location data from Schnucks' website."""

    name = "schnucks"
    allowed_domains = ["locations.schnucks.com"]
    start_urls = ["http://locations.schnucks.com/"]

    SCRIPT_TEXT_XPATH = '//script[@id="__NEXT_DATA__"]/text()'

    def parse(self, response: Response) -> Generator[dict, None, None]:
        """Parse the response and extract location data."""
        self.logger.info(f"Parsing response from {response.url}")

        try:
            json_text = response.xpath(self.SCRIPT_TEXT_XPATH).get()
            if not json_text:
                self.logger.error("Failed to find script tag with location data")
                return

            json_data = json.loads(json_text)
            locations = json_data["props"]["pageProps"]["locations"]

            self.logger.info(f"Found {len(locations)} locations")

            for store in locations:
                yield self.parse_store(store)

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON data: {str(e)}")
        except KeyError as e:
            self.logger.error(f"Failed to access expected key in JSON data: {str(e)}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")

    def parse_store(self, store: dict) -> dict:
        """Parse individual store data."""
        parsed_store = {
            "name": store.get("businessName"),
            "number": store.get("storeCode"),
            "phone": self._get_phone(store),
            "address": self._get_address(store),
            "location": self._get_location(store),
            "services": self._get_services(store),
            "hours": self._get_hours(store),
            "url": store.get("websiteURL"),
            "raw": store
        }

        for key, value in parsed_store.items():
            if value is None or value == "" or value == {}:
                self.logger.warning(f"Missing {key} for store: {store.get('businessName', 'Unknown')}")

        return parsed_store

    def _get_hours(self, store_info: dict) -> dict:
        """Extract and format store hours."""
        try:
            days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
            hours = store_info.get('businessHours', [])

            if len(days) != len(hours):
                self.logger.warning(f"Mismatched hours data for store: {store_info.get('businessName', 'Unknown')}")
                return {}

            return {
                day: {
                    'open': self._convert_to_12h_format(hour[0]),
                    'close': self._convert_to_12h_format(hour[1])
                } for day, hour in zip(days, hours)
            }

        except Exception as e:
            self.logger.error(f"Error getting hours: {e}", exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return ""
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            return time_str

    def _get_services(self, store_info: dict) -> list[str]:
        """Extract available services from store information."""
        try:
            custom = store_info.get("custom", {})
            dept_names = [dept.title() for dept in custom.get("depts", {})]
            service_names = [service['name'] for service in custom.get("services", [])]

            return dept_names + service_names
        except Exception as e:
            self.logger.error(f"Error getting services: {e}", exc_info=True)
            return []

    def _get_phone(self, store_info: dict) -> str:
        """Extract store phone number."""
        phone_numbers = store_info.get('phoneNumbers', [])
        return phone_numbers[0] if phone_numbers else ""

    def _get_address(self, store_info: dict) -> str:
        """Get the formatted store address."""
        try:
            address_parts = store_info.get('addressLines', [])
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address for store: {store_info.get('businessName', 'Unknown')}")
            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
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
            self.logger.warning(f"Missing latitude or longitude for store: {store_info.get('businessName', 'Unknown')}")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}