from datetime import datetime
import json
import logging
from typing import Any, Union, Generator

import scrapy
from scrapy.http import Response

class RaleysSpider(scrapy.Spider):
    name = "raleys"
    allowed_domains = ["www.raleys.com"]

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """
        Initiates the crawling process by sending the first request.

        Yields:
            scrapy.Request: The initial request to start crawling.
        """
        url = 'https://www.raleys.com/api/store'
        data = self.get_payload(0)
        try:
            yield scrapy.Request(
                method="POST",
                url=url,
                body=json.dumps(data),
                headers={'Content-Type': 'application/json'},
                callback=self.parse
            )
        except Exception as e:
            self.logger.error(f"Error in start_requests: {str(e)}")

    def parse(self, response: Response) -> Generator[Union[dict, scrapy.Request], None, None]:
        """
        Parses the response and yields store data. If there are more pages,
        it sends a new request for the next page.

        Args:
            response (Response): The response object from the request.

        Yields:
            dict[str, Any]: Store data from the response.
            scrapy.Request: Next page request if there are more pages.
        """
        try:
            data = response.json()
            stores = data['data']

            for store in stores:
                yield self.parse_store(store)

            if data['offset'] <= data['total']:
                url = 'https://www.raleys.com/api/store'
                new_data = self.get_payload(data['offset'])
                yield scrapy.Request(
                    method="POST",
                    url=url,
                    body=json.dumps(new_data),
                    headers={'Content-Type': 'application/json'},
                    callback=self.parse
                )
        except json.JSONDecodeError:
            self.logger.error(f"Failed to decode JSON from response: {response.text}")
        except KeyError as e:
            self.logger.error(f"Missing key in response data: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse method: {str(e)}")


    def parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        parsed_store = {}

        parsed_store['number'] = store.get('number')
        parsed_store['name'] = store.get('name')
        parsed_store['phone_number'] = store.get('phone')

        parsed_store['address'] = self._get_address(store.get('address', {}))
        parsed_store['location'] = self._get_location(store.get('coordinates', {}))
        parsed_store['hours'] = self._get_hours(store)
        parsed_store['services'] = self._get_services(store)

        parsed_store['url'] = f'https://www.raleys.com/store/{store.get("number")}'
        parsed_store['raw'] = store

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
            hours = raw_store_data.get("storeHours", "")
            if not hours:
                self.logger.warning(f"No hours found for store {raw_store_data.get('name', 'Unknown')}")
                return {}

            open_close_dict = self.parse_time_range(hours)

            days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

            return {day: open_close_dict for day in days}

        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}
    
    @staticmethod
    def parse_time_range(time_range_str: str) -> dict[str, str]:
        # Constants for expected string parts
        EXPECTED_FORMAT = "Between {open_time} and {close_time}"
        TIME_FORMAT = "%I:%M:%S %p"
        OUTPUT_FORMAT = "%I:%M %p"

        # Remove extra spaces and split the string
        parts = time_range_str.strip().split()

        # Validate the basic structure of the input string
        if len(parts) != 6 or parts[0] != "Between" or parts[3] != "and":
            raise ValueError(f"Invalid time range format. Expected: {EXPECTED_FORMAT}")

        # Extract open and close times
        open_time_str, close_time_str = " ".join(parts[1:3]), " ".join(parts[4:6])

        # Parse and format times
        try:
            open_time = datetime.strptime(open_time_str, TIME_FORMAT)
            close_time = datetime.strptime(close_time_str, TIME_FORMAT)
        except ValueError as e:
            raise ValueError(f"Invalid time format: {e}")

        # Format times for output, removing leading zeros from hours
        formatted_open = open_time.strftime(OUTPUT_FORMAT).lstrip("0")
        formatted_close = close_time.strftime(OUTPUT_FORMAT).lstrip("0")

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
        """
        Generates the payload for the API request.

        Args:
            offset (int): The offset for pagination.

        Returns:
            dict[str, Any]: The payload dictionary for the API request.
        """
        return {
            "offset": offset,
            "rows": 75,
            "searchParameter": {
                "shippingMethod": "pickup",
                "searchString": "",
                "latitude": "",
                "longitude": "",
                "maxMiles": 99999,
                "departmentIds": []
            }
        }