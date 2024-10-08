import json
from typing import Any, Generator, Optional

import scrapy
from scrapy.http import Request, Response

class BmwusaSpider(scrapy.Spider):
    """Spider for scraping BMW USA dealer information."""

    name = "bmwusa"
    allowed_domains = ["www.bmwusa.com"]
    zipcode_file_path = "data/tacobell_zipcode_data.json"
    zipcode_api_format_url = "https://www.bmwusa.com/bin/dealerLocatorServlet?getdealerdetailsByRadius/{zipcode}/100?includeSatelliteDealers=true"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers: set = set()

    def start_requests(self) -> Generator[Request, None, None]:
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
        """Parse the JSON response and yield store data."""
        try:
            data = response.json()
            stores = data["dataContent"]["dealerDetails"]["dealerDetailsObjects"]
            
            for store in stores:
                store_contain_fields = ["newVehicleSales", "certifiedPreowned", "service", "ccrc"]
                stores_list = next((store[field] for field in store_contain_fields if store.get(field)), None)
                
                if not stores_list:
                    self.logger.warning("Store info not found: %s", store)
                    continue

                if len(stores_list) > 1:
                    self.logger.warning("Multiple store info: %s", stores_list)
                    continue

                store_info = stores_list[0]
                dealer_number = store_info.get("dpNumber")

                if dealer_number and dealer_number not in self.processed_dealer_numbers:
                    self.processed_dealer_numbers.add(dealer_number)
                    parsed_store = self._parse_store(store_info)
                    if parsed_store:
                        yield parsed_store
                elif dealer_number:
                    self.logger.debug("Duplicate store found: %s", dealer_number)
                else:
                    self.logger.warning("Store missing dealer number: %s", store)
        except Exception as e:
            self.logger.error("Error parsing response: %s url: %s", str(e), response.url, exc_info=True)

    def _parse_store(self, store: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Parse individual store data."""
        try:
            addresses = store.get("address", [])

            if not addresses:
                self.logger.warning("No address found for store: %s", store.get("dealerCd"))
                return None

            if len(addresses) > 1:
                self.logger.warning("Multiple address info found: %s", addresses)

            parsed_store = {
                "number": store.get("dpNumber"),
                "name": store.get("dealerName"),
                "phone_number": store.get("phoneNumber"),
                "address": self._get_address(addresses[0]),
                "location": self._get_location(store),
                "hours": self._get_hours(store),
                "url": "https://www.bmwusa.com/dealer-locator.html",
                "raw": store,
            }

            # Discard items missing required fields
            required_fields = ["address", "location", "url", "raw"]
            if all(parsed_store.get(field) for field in required_fields):
                return parsed_store
            else:
                self.logger.warning("Discarding item due to missing required fields: %s", parsed_store)
                return None
        except Exception as e:
            self.logger.error("Error parsing store: %s", str(e), exc_info=True)
            return None
    
    def _get_address(self, address_info: dict[str, str]) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("lineOne", "").strip(),
                address_info.get("lineTwo", "").strip(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("city", "")
            state = address_info.get("state", "")
            zipcode = address_info.get("zipcode", "")

            if '-' in zipcode:
                zipcode = zipcode.split('-')[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            return full_address
        except Exception as e:
            self.logger.error("Failed to get address: %s", str(e), exc_info=True)
            return ""

    def _get_location(self, store_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get("latitude")
            longitude = store_info.get("longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning("Missing latitude or longitude for store: %s", store_info.get("dealerCd"))
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

    def _get_hours(self, store_info: dict[str, Any]) -> Optional[dict[str, dict[str, Optional[str]]]]:
        """Extract and parse store hours."""
        try:
            hours: dict[str, dict[str, Optional[str]]] = {}

            hours_list = store_info.get("businessHours", [])

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("dealerCd"))
                return None

            if len(hours_list) > 1:
                self.logger.warning("Multiple hours info found: %s", hours_list)
                return None
            
            hours_info = hours_list[0]

            day_abbr_to_day = {
                "mon": "monday", "tue": "tuesday", "wed": "wednesday",
                "thu": "thursday", "fri": "friday", "sat": "saturday", "sun": "sunday"
            }

            for day_abbr, day_name in day_abbr_to_day.items():
                open_time = hours_info.get(f"{day_abbr}_start_time", "").lower()
                close_time = hours_info.get(f"{day_abbr}_end_time", "").lower()
                if hours_info.get(f"{day_abbr}_close_ind") == 'Y':
                    open_time = None
                    close_time = None

                hours[day_name] = {"open": open_time, "close": close_time}

            return hours
        except Exception as e:
            self.logger.error("Failed to get store hours: %s", str(e), exc_info=True)
            return None
