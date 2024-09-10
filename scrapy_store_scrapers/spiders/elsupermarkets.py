import scrapy
from typing import Any, Generator

class ElsupermarketsSpider(scrapy.Spider):
    """
    Spider for scraping store information from elsupermarkets.com
    """
    name = "elsupermarkets"
    allowed_domains = ["elsupermarkets.com"]
    start_urls = ["https://elsupermarkets.com/wp-json/elsuper/v1/stores"]

    def parse(self, response: scrapy.http.Response) -> Generator[dict, None, None]:
        """
        Parse the JSON response and yield store information with parsed hours

        :param response: The response object from the HTTP request
        :return: Generator of store dictionaries with parsed hours
        """
        self.logger.info(f"Parsing response from {response.url}")
        try:
            stores = response.json()
        except ValueError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            return

        for store in stores:
            try:
                parsed_store = self.parse_store(store)
                yield parsed_store
            except Exception as e:
                self.logger.error(f"Error parsing store: {e}")

    def parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        """
        Parse individual store information and add parsed hours

        :param store: Dictionary containing store information
        :return: Store dictionary with added parsed hours
        """
        parsed_store = {}

        parsed_store["name"] = store.get("title")
        parsed_store["phone_number"] = store.get("phone")
        parsed_store["address"] = store.get("address").replace(', ,', ',').strip()

        parsed_store["hours"] = self._get_hours(store)
        parsed_store["services"] = store.get("services", [])
        parsed_store["location"] = self._get_location(store.get("location", {}))

        parsed_store["url"] = store.get("url")
        parsed_store["raw"] = store

        return parsed_store

    def _get_location(self, location_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = location_info.get('lat')
            longitude = location_info.get('lng')

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


    def _get_hours(self, store: dict) -> dict:
        """
        Parse the store hours from the store data

        :param store: Dictionary containing store information
        :return: Dictionary of store hours for each day of the week
        """
        hours_range = store.get("hours", "")
        open_time, close_time = self.get_open_close_times(hours_range)
        return self.create_hours_dict(open_time, close_time)

    def get_open_close_times(self, hours_range: str) -> tuple[str, str]:
        """
        Parse the open and close times from the hours range string

        :param hours_range: Hours range string
        :return: Tuple of open and close times
        """
        if not hours_range:
            self.logger.warning(f"No hours found for store with hours: {hours_range}")
            return

        hours_range = hours_range.lower()
        try:
            open_time, close_time = hours_range.split("-")
            open_time = open_time.strip()
            close_time = close_time.strip()
        except ValueError:
            self.logger.error(f"Invalid hours format for store with hours: {hours_range}")
        
        return open_time, close_time

    @staticmethod
    def create_hours_dict(open_time: str, close_time: str) -> dict[str, dict[str, str]]:
        """
        Create a dictionary of store hours for each day of the week

        :param open_time: Opening time string
        :param close_time: Closing time string
        :return: Dictionary of store hours for each day
        """
        return {
            day: {"open": open_time, "close": close_time}
            for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        }
