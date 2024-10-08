import json
from typing import Optional, Union, Generator
from urllib.parse import urlencode

import scrapy


class HyundaiusaSpider(scrapy.Spider):
    """Spider for scraping Hyundai USA dealer information."""

    name = "hyundaiusa"
    allowed_domains = ["www.hyundaiusa.com"]
    zipcode_file_path = "data/tacobell_zipcode_data.json"
    zipcode_api_base_url = "https://www.hyundaiusa.com/var/hyundai/services/dealer.dealerByZip.service"

    def __init__(self, *args, **kwargs):
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers: set = set()

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Generate initial requests based on zipcode data."""
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            params = {
                "brand": "hyundai",
                "model": "all",
                "lang": "en-us",
                "zip": zipcode["zipcode"],
                "maxdealers": "10"
            }

            headers = {
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.hyundaiusa.com/us/en/dealer-locator",
                "Referrer-Policy": "strict-origin-when-cross-origin"
            }

            url = f"{self.zipcode_api_base_url}?{urlencode(params)}"
            yield scrapy.Request(url, headers=headers, callback=self.parse)

    def _load_zipcode_data(self) -> list[dict[str, Union[str, float]]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s", self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []

    def parse(self, response: scrapy.http.Response) -> Generator[dict[str, Union[str, float, dict]], None, None]:
        """Parse the JSON response and yield store data."""
        try:
            data = response.json()
            stores = data.get("dealers", [])

            for store in stores:
                dealer_number = store.get("dealerCd")
                if dealer_number and dealer_number not in self.processed_dealer_numbers:
                    self.processed_dealer_numbers.add(dealer_number)
                    parsed_store = self._parse_store(store)
                    if parsed_store:
                        yield parsed_store
                elif dealer_number:
                    self.logger.debug("Duplicate store found: %s", dealer_number)
                else:
                    self.logger.warning("Store missing dealer number: %s", store)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response", exc_info=True)
        except Exception as e:
            self.logger.error("Unexpected error in parse method: %s", str(e), exc_info=True)

    def _parse_store(self, store: dict[str, Union[str, float]]) -> Optional[dict[str, Union[str, float, dict]]]:
        """Parse individual store data."""
        try:
            address = self._get_address(store)
            location = self._get_location(store)
            url = "https://www.hyundaiusa.com/us/en/dealer-locator"

            if not all([address, location, url]):
                self.logger.warning("Missing required fields for store: %s", store.get("dealerCd"))
                return None

            return {
                "number": store.get("dealerCd"),
                "name": store.get("dealerNm"),
                "phone_number": store.get("phone"),
                "address": address,
                "location": location,
                "hours": self._get_hours(store),
                "url": url,
                "raw": store,
            }
        except Exception as e:
            self.logger.error("Failed to parse store: %s", str(e), exc_info=True)
            return None

    def _get_address(self, store_info: dict[str, str]) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address1", "").strip(),
                store_info.get("address2", "").strip(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zipCd", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            return full_address
        except Exception as e:
            self.logger.error("Failed to get address: %s", str(e), exc_info=True)
            return ""

    def _get_location(self, loc_info: dict[str, str]) -> dict[str, Union[str, list[float]]]:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("latitude")
            longitude = loc_info.get("longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning("Missing latitude or longitude for store: %s", loc_info.get("dealerCd"))
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

    def _get_hours(self, store_info: dict[str, Union[str, list[dict[str, str]]]]) -> Optional[dict[str, dict[str, Optional[str]]]]:
        """Extract and parse store hours."""
        try:
            hours: dict[str, dict[str, Optional[str]]] = {}

            hours_list = store_info.get("showroom", []) or store_info.get("operations", [])

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("dealerCd"))
                return None

            day_abbr_to_day = {
                "mon": "monday", "tue": "tuesday", "wed": "wednesday",
                "thu": "thursday", "fri": "friday", "sat": "saturday", "sun": "sunday"
            }

            for day_info in hours_list:
                day_abbr = day_info.get("day", "").lower()
                day = day_abbr_to_day.get(day_abbr)
                hours_text = day_info.get("hour", "").lower()

                if not day or not hours_text:
                    continue

                if hours_text == "closed":
                    hours[day] = {"open": None, "close": None}
                    continue

                open_time, close_time = hours_text.split(" - ")
                if open_time and close_time:
                    hours[day] = {"open": open_time, "close": close_time}

            return hours
        except Exception as e:
            self.logger.error("Failed to get store hours: %s", str(e), exc_info=True)
            return None