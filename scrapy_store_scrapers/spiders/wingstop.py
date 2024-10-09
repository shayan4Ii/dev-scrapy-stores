import json
from datetime import datetime
from typing import Any, Optional, Generator

import scrapy


class WingstopSpider(scrapy.Spider):
    """Spider for scraping Wingstop store information."""

    name = "wingstop"
    allowed_domains = ["ecomm.wingstop.com"]
    zipcode_file_path = "data/tacobell_zipcode_data.json"
    location_api_url = "https://ecomm.wingstop.com/location-worker?type=carryout"

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "clientid": "wingstop",
            "content-type": "application/json",
            "nomnom-platform": "web",
            "priority": "u=1, i",
            "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "Referer": "https://www.wingstop.com/",
            "Referrer-Policy": "strict-origin-when-cross-origin"
        }
    }

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers: set = set()

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Generate initial requests based on zipcode data."""
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            payload = {
                "latitude": zipcode["latitude"],
                "longitude": zipcode["longitude"],
                "radius": 20,
                "radiusUnits": "mi"
            }

            yield scrapy.Request(
                self.location_api_url,
                method='POST',
                body=json.dumps(payload),
                callback=self.parse,
            )

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

    def parse(self, response: scrapy.http.Response) -> Generator[dict[str, Any], None, None]:
        """Parse the API response and yield store data."""
        try:
            stores = response.json().get("data", {}).get("locations", [])
            for store in stores:
                dealer_number = store.get("number")
                if dealer_number not in self.processed_dealer_numbers:
                    self.processed_dealer_numbers.add(dealer_number)
                    parsed_store = self._parse_store(store)
                    if parsed_store:
                        yield parsed_store
                else:
                    self.logger.debug("Duplicate store found: %s", dealer_number)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response: %s", response.text)
        except Exception as e:
            self.logger.error("Error in parse method: %s", str(e), exc_info=True)

    def _parse_store(self, store: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Parse individual store data."""
        try:
            address = self._get_address(store)
            location = self._get_location(store)
            url = "https://www.wingstop.com/order"

            if not all([address, location, url]):
                self.logger.warning("Missing required fields for store: %s", store.get("number"))
                return None

            return {
                "number": store.get("number"),
                "name": store.get("name"),
                "phone_number": store.get("phoneNumber"),
                "address": address,
                "location": location,
                "hours": self._get_hours(store),
                "services": self._get_services(store),
                "url": url,
                "raw": store,
            }
        except Exception as e:
            self.logger.error("Error parsing store data: %s", str(e), exc_info=True)
            return None

    def _get_address(self, store_info: dict[str, Any]) -> str:
        """Format store address."""
        try:
            street = store_info.get("streetAddress", "").strip()
            city = store_info.get("locality", "")
            state = store_info.get("region", "")
            zipcode = store_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()
            full_address = ", ".join(filter(None, [street, city_state_zip]))

            if not full_address:
                self.logger.warning("Missing address information for store: %s", store_info.get("number"))
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", str(e), exc_info=True)
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

            self.logger.warning("Missing latitude or longitude for store: %s", loc_info.get("number"))
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", str(error))
        except Exception as error:
            self.logger.error("Error extracting location: %s", str(error), exc_info=True)
        return {}

    def _get_services(self, store_info: dict[str, Any]) -> list[str]:
        """Extract services offered by the store."""
        return [service.get("description") for service in store_info.get("amenities", [])]

    def _get_hours(self, store_info: dict[str, Any]) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}
            calendar = next((cal for cal in store_info.get("businessHours", {}).get("calendar", [])
                             if cal.get("type") == "Normal"), {})
            hours_list = calendar.get("ranges", [])

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("number"))
                return {}

            for raw_hours_dict in hours_list:
                day = raw_hours_dict["startDay"].lower()
                open_time = raw_hours_dict.get("startTime")
                close_time = raw_hours_dict.get("endTime")

                hours[day] = {
                    "open": self._convert_to_12h_format(open_time),
                    "close": self._convert_to_12h_format(close_time)
                }

            return hours
        except Exception as e:
            self.logger.error("Error getting store hours: %s", str(e), exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: Optional[str]) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return ""
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            return time_str
