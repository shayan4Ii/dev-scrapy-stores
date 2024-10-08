import json
import logging
from typing import Any, Generator, Optional

import scrapy
from scrapy.exceptions import DropItem
from scrapy.http import Response


class SubaruUSASpider(scrapy.Spider):
    """Spider for scraping Subaru USA dealer information."""

    name = "subaruusa"
    allowed_domains = ["www.subaru.com"]

    zipcode_file_path = "data/tacobell_zipcode_data.json"
    zipcode_api_format_url = "https://www.subaru.com/services/dealers/distances/by/zipcode?zipcode={zipcode}&count=50&type=Active"

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
            stores = response.json()
            for raw_store in stores:
                store_info = raw_store.get("dealer", {})
                dealer_number = store_info.get("id")
                if dealer_number and dealer_number not in self.processed_dealer_numbers:
                    self.processed_dealer_numbers.add(dealer_number)
                    parsed_store = self._parse_store(store_info)
                    if self._validate_store(parsed_store):
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
            "number": store.get("id"),
            "name": store.get("name"),
            "phone_number": store.get("phoneNumber"),
            "address": self._get_address(store.get("address", {})),
            "location": self._get_location(store.get("location", {})),
            "services": self._get_services(store),
            "url": "https://www.subaru.com/find/a-retailer.html",
            "raw": store
        }

    def _get_address(self, address_info: dict[str, Any]) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("street", ""),
                address_info.get("street2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("city", "")
            state = address_info.get("state", "")
            zipcode = address_info.get("zipcode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information for store: %s", address_info)
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

    def _get_services(self, store_info: dict[str, Any]) -> list[str]:
        """Extract services provided by the store."""
        try:
            services = store_info.get("types", [])

            services_abbreviations = {
                "Phevauth": "Authorized Hybrid Retailer",
                "Cpo": "Certified Pre-Owned Retailer",
                "Sched": "24/7 Online Scheduling",
                "Service": "Subaru Service Department",
                "Eco": "Subaru Eco-Friendly Retailer",
                "Estore": "Subaru Parts Online",
                "Express": "Certified Express Service Retailer",
                "Certifiedcoll": "Certified Collision Center",
                "Jdrental": "Subaru Rental Car Retailer",
                "Jdsubscrip": "Subaru Car Subscription Retailer",
                "Tradeupadv": "Subaru Trade Up Advantage Program Retailer"
            }

            return [services_abbreviations.get(service) for service in services if service in services_abbreviations]
        except Exception as e:
            self.logger.error("Error extracting services: %s", e, exc_info=True)
            return []

    def _validate_store(self, store: dict[str, Any]) -> bool:
        """Validate if the store has all required fields."""
        required_fields = ["address", "location", "url", "raw"]
        for field in required_fields:
            if not store.get(field):
                self.logger.warning("Missing required field: %s", field)
                return False
        return True

