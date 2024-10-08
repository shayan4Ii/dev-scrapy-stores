import json
from datetime import datetime
from typing import Any, Generator, Optional

import scrapy
from scrapy.http import Response


class ToyotaUSASpider(scrapy.Spider):
    """Spider for scraping Toyota USA dealer information."""

    name = "toyotausa"
    allowed_domains = ["www.toyota.com"]
    zipcode_file_path = "data/tacobell_zipcode_data.json"
    zipcode_api_format_url = "https://www.toyota.com/service/tcom/locateDealer/zipCode/{zipcode}"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers: set[str] = set()

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Generate initial requests based on zipcode data."""
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            url = self.zipcode_api_format_url.format(zipcode=zipcode["zipcode"])
            yield scrapy.Request(url, callback=self.parse)

    def _load_zipcode_data(self) -> list[dict[str, Any]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s", self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []

    def parse(self, response: Response) -> Generator[dict[str, Any], None, None]:
        """Parse the response and yield store data."""
        try:
            data = response.json()
            stores = data.get("dealers", [])

            for store in stores:
                dealer_number = store.get("code")
                if dealer_number and dealer_number not in self.processed_dealer_numbers:
                    self.processed_dealer_numbers.add(dealer_number)
                    parsed_store = self._parse_store(store)
                    if self._is_valid_store(parsed_store):
                        yield parsed_store
                    else:
                        self.logger.warning("Discarded invalid store: %s", dealer_number)
                elif dealer_number:
                    self.logger.debug("Duplicate store found: %s", dealer_number)
                else:
                    self.logger.warning("Store without dealer number found")
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response", exc_info=True)

    def _parse_store(self, store: dict[str, Any]) -> dict[str, Any]:
        """Parse individual store data."""
        return {
            "number": store.get("code"),
            "name": store.get("name"),
            "phone_number": store.get("phone"),
            "address": self._get_address(store),
            "location": self._get_location(store),
            "hours": self._get_hours(store),
            "url": f"https://www.toyota.com/dealers/dealer/{store.get('code')}",
            "raw": store
        }

    def _is_valid_store(self, store: dict[str, Any]) -> bool:
        """Check if the store has all required fields."""
        required_fields = ["address", "location", "url", "raw"]
        return all(store.get(field) for field in required_fields)

    def _get_address(self, store_info: dict[str, Any]) -> str:
        """Format store address."""
        try:
            address_parts = [store_info.get("address1", "").strip()]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information for store: %s", store_info.get("code"))
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("latitude")
            longitude = loc_info.get("longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning("Missing latitude or longitude for store: %s", loc_info.get("code"))
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

    def _get_hours(self, store_info: dict[str, Any]) -> dict[str, dict[str, Optional[str]]]:
        """Extract and parse store hours."""
        try:
            hours: dict[str, dict[str, Optional[str]]] = {}

            hour_contain_fields = ["general", "sales", "service", "parts"]

            for field in hour_contain_fields:
                hours_list = store_info.get(field, {}).get("hours", [])
                if hours_list:
                    break

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("code"))
                return {}

            days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]

            for day, day_hours in zip(days, hours_list):
                if len(day_hours) != 1:
                    self.logger.warning("Invalid hours data for day: %s %s", day, day_hours)
                    continue

                hours_text = day_hours[0]

                if hours_text.lower() == "closed":
                    hours[day] = {"open": None, "close": None}
                    continue

                open_time, close_time = hours_text.split(",")
                hours[day] = {
                    "open": self._convert_to_12h_format(open_time),
                    "close": self._convert_to_12h_format(close_time)
                }

            return hours
        except Exception as e:
            self.logger.error("Error getting store hours: %s", e, exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            time_obj = datetime.strptime(time_str, '%H%M').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return time_str
