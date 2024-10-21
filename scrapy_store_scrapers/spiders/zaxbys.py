import json
import logging
from datetime import datetime, timedelta
from typing import Any, Generator, Optional

import scrapy
from scrapy.http import Request, Response


class ZaxbysSpider(scrapy.Spider):
    """Spider for scraping Zaxby's restaurant data."""

    name = "zaxbys"
    allowed_domains = ["zapi.zaxbys.com"]

    zipcode_file_path = "data/tacobell_zipcode_data.json"
    STORES_API_URL = "https://zapi.zaxbys.com/v1/stores/near?latitude={latitude}&longitude={longitude}"
    HOURS_API_URL = "https://zapi.zaxbys.com/v1/stores/{store_id}/calendars?from={from_date}&to={to_date}"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers: set[str] = set()

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests based on zipcode data."""
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:

            url = self.STORES_API_URL.format(
                latitude=zipcode["latitude"], longitude=zipcode["longitude"]
            )
            yield scrapy.Request(url, callback=self.parse)

    def _load_zipcode_data(self) -> list[dict]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s", self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []

    def parse(self, response: Response) -> Generator[Request, None, None]:
        """Parse the initial API response and generate requests for store hours."""
        try:
            stores = response.json()
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON response from stores API")
            return

        for store in stores:
            dealer_number = store.get("storeId")
            if dealer_number and dealer_number not in self.processed_dealer_numbers:
                self.processed_dealer_numbers.add(dealer_number)
                yield scrapy.Request(
                    url=self._get_hours_url(dealer_number),
                    callback=self._parse_store_with_hours,
                    meta={"store": store},
                )
            else:
                self.logger.debug("Duplicate or invalid store found: %s", dealer_number)

    def _get_hours_url(self, store_id: str) -> str:
        """Generate URL for fetching store hours."""
        today = datetime.now().date()
        end_date = today + timedelta(days=6)
        return self.HOURS_API_URL.format(
            store_id=store_id,
            from_date=today.strftime("%Y-%m-%d"),
            to_date=end_date.strftime("%Y-%m-%d"),
        )

    def _parse_store_with_hours(self, response: Response) -> Generator[dict, None, None]:
        """Parse store data with hours information."""
        store = response.meta["store"]
        try:
            hours_data = response.json()
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON response from hours API for store: %s", store.get("storeId"))
            return

        store_data = self._parse_store(store, hours_data)
        if self._validate_store_data(store_data):
            yield store_data

    def _parse_store(self, store: dict, hours_data: list) -> dict:
        """Parse individual store data."""
        return {
            "number": store.get("storeId"),
            "name": store.get("storeName"),
            "phone_number": store.get("phone"),
            "address": self._get_address(store),
            "location": self._get_location(store),
            "hours": self._get_hours(hours_data),
            "url": self._get_url(store),
            "raw": store,
        }

    def _get_hours(self, hours_data: list) -> dict:
        """Parse store hours from the API response."""
        parsed_hours = {day: {"open": None, "close": None} for day in [
            "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
        ]}

        day_mapping = {
            "Mon": "monday", "Tue": "tuesday", "Wed": "wednesday",
            "Thu": "thursday", "Fri": "friday", "Sat": "saturday", "Sun": "sunday"
        }

        for day_data in hours_data:
            if day_data["type"] == "business":
                for range_data in day_data["ranges"]:
                    day_of_week = day_mapping[range_data["day"]]
                    open_time = self._format_time(range_data["opensAt"])
                    close_time = self._format_time(range_data["closeAt"])
                    parsed_hours[day_of_week] = {"open": open_time, "close": close_time}

        return parsed_hours

    def _format_time(self, time_str: str) -> str:
        """Format time string."""
        time = datetime.strptime(time_str.split()[1], "%H:%M")
        return time.strftime("%I:%M %p").lower().lstrip("0")

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [store_info.get("address", "").strip()]
            street = ", ".join(filter(None, address_parts))
            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")
            city_state_zip = f"{city}, {state} {zipcode}".strip()
            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information for store: %s", store_info.get("storeId"))
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("latitude")
            longitude = loc_info.get("longitude")
            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude for store: %s", loc_info.get("storeId"))
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

    def _get_url(self, store_info: dict) -> str:
        """Get store URL."""
        state = store_info.get("state", "").lower()
        city = store_info.get("city", "").lower()
        slug = store_info.get("slug", "").lower()
        return f"https://www.zaxbys.com/locations/{state}/{city}/{slug}"

    def _validate_store_data(self, store_data: dict) -> bool:
        """Validate required fields in store data."""
        required_fields = ["address", "location", "url", "raw"]
        for field in required_fields:
            if not store_data.get(field):
                self.logger.warning("Missing required field: %s for store: %s", field, store_data.get("number"))
                return False
        return True
