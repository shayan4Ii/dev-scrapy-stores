from datetime import datetime
import json
from typing import Any, Generator, Union

import scrapy
from scrapy.http import Response


class RaleysSpider(scrapy.Spider):
    """Spider for scraping store information from Raleys website."""

    name = "raleys"
    allowed_domains = ["www.raleys.com"]
    api_url = 'https://www.raleys.com/api/store'
    rows_per_page = 75

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Initiate the crawling process by sending the first request."""
        data = self.get_payload(0)
        try:
            yield scrapy.Request(
                method="POST",
                url=self.api_url,
                body=json.dumps(data),
                headers={'Content-Type': 'application/json'},
                callback=self.parse
            )
        except Exception as e:
            self.logger.error(f"Error in start_requests: {e}", exc_info=True)

    def parse(self, response: Response) -> Generator[Union[dict[str, Any], scrapy.Request], None, None]:
        """Parse the response and yield store data or next page request."""
        try:
            data = response.json()
            stores = data.get('data', [])

            if not stores:
                self.logger.warning("No stores found in the response")

            for store in stores:
                yield self.parse_store(store)

            current_offset = data.get('offset', 0)
            total_stores = data.get('total', 0)
            if current_offset < total_stores:
                new_offset = current_offset + self.rows_per_page
                new_data = self.get_payload(new_offset)
                yield scrapy.Request(
                    method="POST",
                    url=self.api_url,
                    body=json.dumps(new_data),
                    headers={'Content-Type': 'application/json'},
                    callback=self.parse
                )
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON from response: {response.text}")
        except KeyError as e:
            self.logger.error(f"Missing key in response data: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse method: {e}", exc_info=True)

    def parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        """Parse individual store data."""
        parsed_store = {
            'number': store.get('number'),
            'name': store.get('name'),
            'phone_number': store.get('phone'),
            'address': self._get_address(store.get('address', {})),
            'location': self._get_location(store.get('coordinates', {})),
            'hours': self._get_hours(store),
            'services': self._get_services(store),
            'url': f'https://www.raleys.com/store/{store.get("number")}',
            'raw': store
        }

        for key, value in parsed_store.items():
            if value in (None, "", [], {}):
                self.logger.warning(f"Missing or empty {key} for store {store.get('name', 'Unknown')}")

        return parsed_store

    def _get_services(self, raw_store_data: dict[str, Any]) -> list[str]:
        """Extract and parse store services."""
        try:
            services = raw_store_data.get("departments", [])
            if not services:
                self.logger.warning(f"No services found for store {raw_store_data.get('name', 'Unknown')}")
                return []

            return [service.get("displayName", "") for service in services if not service.get('isInternal', False)]
        except Exception as e:
            self.logger.error(f"Error getting store services: {e}", exc_info=True)
            return []

    def _get_hours(self, raw_store_data: dict[str, Any]) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours_str = raw_store_data.get("storeHours", "")
            if not hours_str:
                self.logger.warning(f"No hours found for store {raw_store_data.get('name', 'Unknown')}")
                return {}

            open_close_dict = self.parse_time_range(hours_str)
            days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            return {day: open_close_dict for day in days}
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    @staticmethod
    def parse_time_range(time_range_str: str) -> dict[str, str]:
        """Parse time range string into a dictionary of open and close times."""
        expected_format = "Between {open_time} and {close_time}"
        time_format = "%I:%M:%S %p"
        output_format = "%I:%M %p"

        parts = time_range_str.strip().split()

        if len(parts) != 6 or parts[0] != "Between" or parts[3] != "and":
            raise ValueError(f"Invalid time range format. Expected: {expected_format}")

        open_time_str, close_time_str = " ".join(parts[1:3]), " ".join(parts[4:6])

        try:
            open_time = datetime.strptime(open_time_str, time_format)
            close_time = datetime.strptime(close_time_str, time_format)
        except ValueError as e:
            raise ValueError(f"Invalid time format: {e}")

        formatted_open = open_time.strftime(output_format).lstrip("0")
        formatted_close = close_time.strftime(output_format).lstrip("0")

        return {
            "open": formatted_open,
            "close": formatted_close
        }

    def _get_address(self, address_info: dict[str, Any]) -> str:
        """Get the formatted store address."""
        try:
            street = address_info.get("street", "")
            city = address_info.get("city", "")
            state = address_info.get("state", "")
            zipcode = address_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()
            full_address = ", ".join(filter(None, [street, city_state_zip]))

            if not full_address:
                self.logger.warning(f"Missing address for store with address info: {address_info}")

            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
            return ""

    def _get_location(self, location_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = location_info.get('latitude')
            longitude = location_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store with location info: {location_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}

    @staticmethod
    def get_payload(offset: int) -> dict[str, Any]:
        """Generate the payload for the API request."""
        return {
            "offset": offset,
            "rows": RaleysSpider.rows_per_page,
            "searchParameter": {
                "shippingMethod": "pickup",
                "searchString": "",
                "latitude": "",
                "longitude": "",
                "maxMiles": 99999,
                "departmentIds": []
            }
        }