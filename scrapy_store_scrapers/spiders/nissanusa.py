import json
import logging
from datetime import datetime
from typing import Any, Generator, Optional

import scrapy
from scrapy.http import Request, Response


class NissanusaSpider(scrapy.Spider):
    """Spider for scraping Nissan USA dealer information."""

    name = "nissanusa"
    API_FORMAT_URL = "https://us.nissan-api.net/v2/dealers?size=100&lat={latitude}&long={longitude}&serviceFilterType=AND&include=openingHours"
    zipcode_file_path = "data/tacobell_zipcode_data.json"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers: set[str] = set()

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests to fetch access token."""
        token_url = "https://us.nissan-api.net/v2/publicAccessToken?locale=en_US&scope=READ&proxy=%2A&brand=nissan&environment=prod"
        token_headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
            "Referer": "https://www.nissanusa.com/"
        }
        yield Request(url=token_url, method="GET", headers=token_headers, callback=self.parse_token)

    def parse_token(self, response: Response) -> Generator[Request, None, None]:
        """Parse the access token and initiate dealer requests."""
        try:
            data = json.loads(response.text)
            access_token = data.get('access_token')

            if not access_token:
                self.logger.error("Access token not found in the response.")
                return

            dealers_headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "accesstoken": f"Bearer {access_token}",
                "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "cross-site",
                "Referer": "https://www.nissanusa.com/"
            }

            zipcodes = self._load_zipcode_data()
            for zipcode in zipcodes:
                dealers_url = self.API_FORMAT_URL.format(
                    latitude=zipcode["latitude"],
                    longitude=zipcode["longitude"]
                )
                yield Request(
                    url=dealers_url,
                    method="GET",
                    headers=dealers_headers,
                    callback=self.parse_dealers
                )
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response for access token.", exc_info=True)
        except Exception as e:
            self.logger.error("Unexpected error in parse_token: %s", str(e), exc_info=True)

    def _load_zipcode_data(self) -> list[dict]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s", self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []

    def parse_dealers(self, response: Response) -> Generator[dict, None, None]:
        """Parse and yield dealer information."""
        try:
            data = response.json()
            stores = data.get("dealers", [])

            for store in stores:
                dealer_number = store.get("dealerId")
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
            self.logger.error("Failed to parse JSON response for dealers.", exc_info=True)
        except Exception as e:
            self.logger.error("Unexpected error in parse_dealers: %s", str(e), exc_info=True)

    def _is_valid_store(self, store: dict) -> bool:
        """Check if the store has all required fields."""
        required_fields = ["address", "location", "url", "raw"]
        return all(store.get(field) for field in required_fields)

    def _parse_store(self, store_info: dict) -> dict:
        """Parse store information into a structured format."""
        try:
            parsed_store = {
                "number": store_info.get("dealerId"),
                "name": store_info.get("name"),
                "phone_number": store_info.get("contact", {}).get("phone"),
                "address": self._get_address(store_info.get("address", {})),
                "location": self._get_location(store_info.get("geolocation", {})),
                "hours": self._get_hours(store_info),
                "services": self._get_services(store_info),
                "url": "https://www.nissanusa.com/dealer-locator.html",
                "raw": store_info
            }
            return parsed_store
        except Exception as e:
            self.logger.error("Error parsing store: %s", str(e), exc_info=True)
            return {}

    def _get_address(self, address_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("addressLine1", ""),
                address_info.get("addressLine2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("city", "")
            state = address_info.get("stateCode", "")
            zipcode = address_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information for store: %s", address_info)
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", str(e), exc_info=True)
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

            self.logger.warning("Missing latitude or longitude for store: %s", loc_info)
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", str(error), exc_info=True)
        return {}

    def _get_services(self, store_info: dict) -> list[str]:
        """Extract and parse services from custom fields."""
        services_map = {
            'us_en_nissan_S_DSP': 'SERVICE@HOME',
            'us_en_nissan_S_CS': 'BUY@HOME',
            'us_en_nissan_S_DTDP': 'DRIVE@HOME',
            'us_en_nissan_S_ES': 'Nissan Express Service Participating Dealer',
            'us_en_nissan_S_SVC': 'Service Center',
            'us_en_nissan_S_OS': 'Schedule a Service',
            'us_en_nissan_S_LF': 'EV Certified',
            'us_en_nissan_S_GTR': 'GT-R Dealer',
            'us_en_nissan_S_BC': 'Business Certified ',
            'us_en_nissan_S_RNTL': 'Rental Car Dealer',
            'us_en_nissan_P_O2O': 'MyNissan Rewards',
            'us_en_nissan_S_CPO': 'Certified Pre-Owned Dealer',
            'us_en_nissan_S_GP': 'Genuine Nissan Parts & Accessories',
            'us_en_nissan_S_CLSN': 'Certified Collision Repair Shop'
        }

        service_codes = [service['id'] for service in store_info.get("services", [])]
        available_services = [services_map.get(service) for service in service_codes if service in services_map]

        if 'Nissan Express Service Participating Dealer' in available_services:
            available_services.append('Express Service')

        unknown_services = set(service_codes) - set(services_map.keys())
        if unknown_services:
            self.logger.debug("Unknown service types found: %s", ", ".join(unknown_services))

        return available_services

    def _get_hours(self, store_info: dict) -> dict[str, dict[str, Optional[str]]]:
        """Extract and parse store hours."""
        try:
            hours: dict[str, dict[str, Optional[str]]] = {}
            hours_list = store_info.get("openingHours", {}).get("regularOpeningHours", [])

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("code"))
                return {}

            days_map = {
                1: "monday", 2: "tuesday", 3: "wednesday", 4: "thursday",
                5: "friday", 6: "saturday", 7: "sunday"
            }

            for day_hours in hours_list:
                day_index = day_hours.get("weekDay")
                day_name = days_map.get(day_index)
                intervals = day_hours.get("openIntervals", [])

                if not day_name:
                    self.logger.warning("Invalid day index: %s", day_index)
                    continue

                if not intervals:
                    hours[day_name] = {"open": None, "close": None}
                    self.logger.warning("No hours found for day: %s", day_name)
                    continue

                if len(intervals) != 1:
                    self.logger.warning("Invalid hours data for day: %s", day_hours)
                    continue

                interval_dict = intervals[0]
                open_time = interval_dict.get("openFrom")
                close_time = interval_dict.get("openUntil")
                hours[day_name] = {
                    "open": self._convert_to_12h_format(open_time),
                    "close": self._convert_to_12h_format(close_time)
                }

            return hours
        except Exception as e:
            self.logger.error("Error getting store hours: %s", str(e), exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: Optional[str]) -> Optional[str]:
        """Convert time to 12-hour format."""
        if not time_str:
            return None
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return time_str