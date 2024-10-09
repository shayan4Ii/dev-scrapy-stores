import json
from typing import Generator, List, Dict, Any

import scrapy
from scrapy.http import Request, Response


class KiausaSpider(scrapy.Spider):
    """Spider for scraping Kia USA dealer information."""

    name = "kiausa"
    allowed_domains = ["www.kia.com"]
    zipcode_file_path = "data/tacobell_zipcode_data.json"
    DEALERS_API_URL = "https://www.kia.com/us/services/en/cpo/dealers"

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json;charset=UTF-8",
        }
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers: set = set()

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests based on zipcode data."""
        zipcodes = self._load_zipcode_data()
        
        for zipcode_info in zipcodes:
            payload = {
                "type": "zip",
                "zipCode": zipcode_info["zipcode"],
                "dealerCertifications": [],
                "dealerServices": [],
                "radius": 25
            }
            body = json.dumps(payload)
            yield scrapy.Request(
                url=self.DEALERS_API_URL,
                method="POST",
                body=body,
                callback=self.parse,
            )

    def _load_zipcode_data(self) -> List[Dict[str, str]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s",
                              self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error(
                "Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []

    def parse(self, response: Response) -> Generator[Dict[str, Any], None, None]:
        """Parse the API response and yield store data."""
        try:
            stores = response.json()
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response")
            return

        for store in stores:
            dealer_number = store.get("code")
            if dealer_number not in self.processed_dealer_numbers:
                self.processed_dealer_numbers.add(dealer_number)
                parsed_store = self._parse_store(store)
                if self._is_valid_store(parsed_store):
                    yield parsed_store
            else:
                self.logger.debug("Duplicate store found: %s", dealer_number)

    def _parse_store(self, store: dict) -> Dict[str, Any]:
        """Parse individual store data."""
        return {
            "number": store.get("code"),
            "name": store.get("name"),
            "phone_number": self._get_phone(store),
            "address": self._get_address(store.get("location", {})),
            "location": self._get_location(store.get("location", {})),
            "services": self._get_services(store),
            "url": "https://www.kia.com/us/en/cpo/find-a-dealer/landing.html",
            "raw": store,
        }

    def _is_valid_store(self, store: Dict[str, Any]) -> bool:
        """Check if the store has all required fields."""
        required_fields = ["address", "location", "url", "raw"]
        for field in required_fields:
            if not store.get(field):
                self.logger.warning("Missing required field: %s", field)
                return False
        return True

    def _get_phone(self, store: dict) -> str:
        """Extract phone number from store data."""
        for phone in store.get("phones", []):
            return phone.get("number", "")
        return ""

    def _get_address(self, address_info: dict) -> str:
        """Format store address."""
        try:
            street = address_info.get("street1", "").strip()
            city = address_info.get("city", "")
            state = address_info.get("state", "")
            zipcode = address_info.get("zipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()
            full_address = ", ".join(filter(None, [street, city_state_zip]))

            if not full_address:
                self.logger.warning(
                    "Missing address information for store: %s", address_info)
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict) -> Dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("latitude")
            longitude = loc_info.get("longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(
                "Missing latitude or longitude for store: %s", loc_info)
            return {}
        except ValueError as error:
            self.logger.warning(
                "Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s",
                              error, exc_info=True)
        return {}

    def _get_services(self, store_info: dict) -> List[str]:
        """Extract and parse services from custom fields."""
        services_map = {
            8: "Certified Wholesale Dealer",
            14: "Participating Kia Maintenance Plan™ Dealer",
            15: "EV9 Dealer Preview",
            17: "Kia EasyBuy Dealer",
            6: "Kia Dealer of Excellence",
            7: "Authorized Kia Express Service Dealer",
        }
        service_codes = store_info.get("featureIds", [])
        available_services = [services_map.get(
            service) for service in service_codes if service in services_map]

        unknown_services = set(service_codes) - set(services_map.keys())
        if unknown_services:
            unknown_services_str = ", ".join(map(str, unknown_services))
            self.logger.debug(
                "Unknown service types found: %s", unknown_services_str)

        return available_services
