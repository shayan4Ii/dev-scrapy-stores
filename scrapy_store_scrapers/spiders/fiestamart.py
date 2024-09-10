import scrapy
from typing import Any, Generator

class FiestamartSpider(scrapy.Spider):
    """
    Spider for scraping store information from fiestamart.com
    """
    name = "fiestamart"
    allowed_domains = ["fiestamart.com"]
    start_urls = ["https://www.fiestamart.com/wp-json/fiesta/v1/stores"]

    def parse(self, response: scrapy.http.Response) -> Generator[dict[str, Any], None, None]:
        """Parse the JSON response and yield store information."""
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
                self.logger.error(f"Error parsing store: {e}", exc_info=True)

    def parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        """Parse individual store information and add parsed hours."""
        parsed_store = {
            "name": store.get("title"),
            "phone_number": store.get("phone"),
            "address": self._clean_address(store.get("address", "")),
            "hours": self._get_hours(store),
            "services": store.get("services", []),
            "location": self._get_location(store.get("location", {})),
            "url": store.get("url"),
            "raw": store
        }

        for key, value in parsed_store.items():
            if value is None or (isinstance(value, (list, dict)) and not value):
                self.logger.warning(f"Missing or empty data for {key}")

        return parsed_store

    def _clean_address(self, address: str) -> str:
        """Clean and format the address string."""
        return address.replace(", ,", ",").strip()

    def _get_location(self, location_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = location_info.get("lat")
            longitude = location_info.get("lng")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude: {location_info}")
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}

    def _get_hours(self, store: dict[str, Any]) -> dict[str, dict[str, str]]:
        """Parse the store hours from the store data."""
        hours_range = store.get("hours", "")
        if not hours_range:
            self.logger.warning(f"No hours found for store: {store.get('title', 'Unknown')}")
            return {}

        open_time, close_time = self._parse_hours_range(hours_range)
        return self._create_hours_dict(open_time, close_time)

    def _parse_hours_range(self, hours_range: str) -> tuple[str, str]:
        """Parse the open and close times from the hours range string."""
        hours_range = hours_range.lower()
        try:
            open_time, close_time = map(str.strip, hours_range.split("-"))
            return open_time, close_time
        except ValueError:
            self.logger.error(f"Invalid hours format: {hours_range}")
            return "", ""

    @staticmethod
    def _create_hours_dict(open_time: str, close_time: str) -> dict[str, dict[str, str]]:
        """Create a dictionary of store hours for each day of the week."""
        if not open_time or not close_time:
            return {}
        return {
            day: {"open": open_time, "close": close_time}
            for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        }
