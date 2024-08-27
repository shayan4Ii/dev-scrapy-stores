import json
import re
from datetime import datetime
from typing import Generator, Optional, Union

import scrapy
from scrapy.http import Response

class AttSpider(scrapy.Spider):
    """Spider for scraping AT&T store information."""

    name = "att"
    allowed_domains = ["att.com"]
    start_urls = ["https://www.att.com/stores/"]

    store_client_keys: list[str] = []

    APP_KEY_RE = re.compile(r"^\s*appkey:\s*'([^']+)'", flags=re.MULTILINE)

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the initial response and generate requests for store data."""
        app_key = self._extract_app_key(response.text)
        if not app_key:
            return

        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            headers = self._get_headers()
            payload = self._get_payload(zipcode["zipcode"], zipcode["latitude"], zipcode["longitude"], app_key)
            yield scrapy.Request(
                url="https://www.att.com/stores/rest/locatorsearch",
                method="POST",
                headers=headers,
                body=json.dumps(payload),
                callback=self.parse_stores
            )

    def parse_stores(self, response: Response) -> Generator[dict[str, Union[str, dict]], None, None]:
        """Parse store information from the response."""
        stores = response.json()
        for store in stores["response"]["collection"]:
            if store["clientkey"] in self.store_client_keys:
                continue
            
            self.store_client_keys.append(store["clientkey"])

            yield {
                "name": self._get_name(store),
                "phone_number": self._get_phone(store),
                "address": self._get_address(store),
                "location": self._get_location(store),
                "hours": self._get_hours(store),
                "raw_dict": store
            }

    def _extract_app_key(self, text: str) -> Optional[str]:
        """Extract the app key from the response text."""
        app_key_match = self.APP_KEY_RE.search(text)
        if app_key_match:
            return app_key_match.group(1)
        self.logger.error("Failed to find app key")
        return None

    def _load_zipcode_data(self) -> list[dict[str, Union[str, float]]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(r"data\tacobell_zipcode_data.json") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found")
            return []
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file")
            return []

    @staticmethod
    def _get_name(store: dict[str, str]) -> str:
        """Get the store name."""
        return store["name"]
    
    @staticmethod
    def _get_phone(store: dict[str, str]) -> str:
        """Get the store phone number."""
        return store["phone"]
    
    def _get_address(self, store: dict[str, Optional[str]]) -> str:
        """Get the formatted store address."""
        addr1 = self._get_none_as_empty_string(store.get("address1"))
        addr2 = self._get_none_as_empty_string(store.get("address2"))
        city = self._get_none_as_empty_string(store.get("city"))
        state = self._get_none_as_empty_string(store.get("state"))
        zipcode = self._get_none_as_empty_string(store.get("postalcode"))

        street_address = f"{addr1}, {addr2}".strip(", ")
        return f"{street_address}, {city}, {state} {zipcode}"

    @staticmethod
    def _get_none_as_empty_string(value: Optional[str]) -> str:
        """Convert None to an empty string."""
        return value if value is not None else ""

    def _get_location(self, store: dict[str, Union[str, float]]) -> Optional[dict[str, Union[str, list[float]]]]:
        """Get the store location in GeoJSON Point format."""
        try:
            latitude = store.get('latitude')
            longitude = store.get('longitude')
            
            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude")
            return None
        except ValueError:
            self.logger.warning(f"Invalid latitude or longitude values: {latitude}, {longitude}")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting location: {str(e)}")
            return None

    def _get_hours(self, store: dict[str, str]) -> Optional[dict[str, dict[str, Optional[str]]]]:
        """Get the store hours."""
        hours = store.get("bho")
        if not hours:
            return None

        try:
            hours_list = json.loads(hours)
            if len(hours_list) != 7:
                raise ValueError("Invalid hours list length")

            days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
            return {
                day: {
                    "open": self._convert_to_12h_format(hours_list[n][0]),
                    "close": self._convert_to_12h_format(hours_list[n][1])
                } for n, day in enumerate(days)
            }
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in store hours")
        except ValueError as e:
            self.logger.error(f"Error processing store hours: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error processing store hours: {str(e)}")
        return None

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> Optional[str]:
        """Convert time from 24-hour to 12-hour format."""
        if not time_str or time_str == "9999":
            return None
        try:
            time_obj = datetime.strptime(time_str, '%H%M').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return None

    @staticmethod
    def _get_headers() -> dict[str, str]:
        """Get the headers for the request."""
        return {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "x-requested-with": "XMLHttpRequest"
        }

    @staticmethod
    def _get_payload(zipcode: str, latitude: float, longitude: float, app_key: str) -> dict:
        """Get the payload for the request."""
        return {
            "request": {
                "appkey": app_key,
                "formdata": {
                    "geoip": False,
                    "dataview": "store_default",
                    "google_autocomplete": "true",
                    "limit": 15,
                    "geolocs": {
                        "geoloc": [{
                            "addressline": zipcode,
                            "country": "US",
                            "latitude": latitude,
                            "longitude": longitude,
                            "state": "",
                            "province": "",
                            "city": "",
                            "address1": "",
                            "postalcode": zipcode
                        }]
                    },
                    "searchradius": "40|50",
                    "where": {
                        "opening_status": {"distinctfrom": "permanently_closed"},
                        "virtualstore": {"distinctfrom": "1"},
                        "and": {
                            "company_owned_stores": {"eq": ""},
                            "cash_payments_accepted": {"eq": ""},
                            "offers_spanish_support": {"eq": ""},
                            "pay_station_inside": {"eq": ""},
                            "small_business_solutions": {"eq": ""}
                        }
                    },
                    "false": "0"
                }
            }
        }