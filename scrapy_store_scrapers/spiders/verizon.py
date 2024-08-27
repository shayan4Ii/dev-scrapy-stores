import json
from typing import Generator, Union, Optional
import re

import scrapy


class VerizonSpider(scrapy.Spider):
    name = "verizon"
    allowed_domains = ["www.verizon.com"]

    store_numbers = []

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes[:20]:
            payload = self._get_payload(zipcode["latitude"], zipcode["longitude"])
            yield scrapy.Request(
                url=f"https://www.verizon.com/digital/nsa/nos/gw/retail/searchresultsdata",
                method="POST",
                body=json.dumps(payload),
                headers=self._get_headers(),
                callback=self.parse_stores
            )

    def parse_stores(self, response: scrapy.http.Response) -> Generator[dict[str, Union[str, dict]], None, None]:
        stores = response.json()["body"]["data"]["stores"]
        for store in stores:
            store_info = {}

            store_no = self._get_number(store)

            if store_no in self.store_numbers:
                continue

            self.store_numbers.append(store_no)

            store_info["name"] = self._get_name(store)
            store_info["number"] = store_no
            store_info["phone_number"] = self._phone_number(store)
            store_info["location"] = self._get_location(store)
            store_info["hours"] = self._get_hours(store)

            yield store_info

    @staticmethod
    def _get_number(store: dict) -> str:
        return store["storeNumber"]
    
    @staticmethod
    def _get_name(store: dict) -> str:
        return store["storeName"]

    @staticmethod
    def _phone_number(store: dict) -> str:
        return store["phoneNumber"]

    def _get_location(self, store: dict) -> Optional[dict[str, Union[str, list[float]]]]:
        """Get the store location in GeoJSON Point format."""
        try:
            location = store.get('location')
            latitude = location.get('latitude')
            longitude = location.get('longitude')
            
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
        
    def _get_hours(self, store: dict) -> Optional[dict[str, str]]:
        """Get the store hours."""

        hours_info = {}

        day_key_map = {
            "monday": "hoursMon",
            "tuesday": "hoursTue",
            "wednesday": "hoursWed",
            "thursday": "hoursThu",
            "friday": "hoursFri",
            "saturday": "hoursSat",
            "sunday": "hoursSun"
        }
        
        for day, day_key in day_key_map.items():
            hours = store.get(day_key)

            if not hours:
                self.logger.warning(f"Missing hours for {day}")
                continue

            open_close = self._get_open_close(hours)
            if open_close:
                open_time, close_time = open_close
                hours_info[day] = {
                    "open": open_time,
                    "close": close_time
                }
            else:
                hours_info[day] = {
                    "open": None,
                    "close": None
                }
            
        return hours_info

    def _get_open_close(self, hours: str) -> Optional[tuple[str, str]]:
        """Get the open and close times from a string."""
        try:
            hours = hours.strip().lower()

            if hours == "closed closed":
                return None, None

            pattern = r"(\d{1,2}:\d{2}\s?(?:AM|PM))\s+(\d{1,2}:\d{2}\s?(?:AM|PM))"
            match = re.match(pattern, hours, re.IGNORECASE)
            if match:
                open_time, close_time = match.groups()
                return open_time, close_time
            
            self.logger.warning(f"Invalid hours format: {hours}")
            return None
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error extracting open and close times: {str(e)}")
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
    def _get_headers() -> dict[str, str]:
        """Get the headers for the request."""
        return {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "x-requested-with": "XMLHttpRequest"
        }
    
    @staticmethod
    def _get_payload(latitude: float, longitude: float) -> dict:
        return {
            "locationCodes": [],
            "longitude": longitude,
            "latitude": latitude,
            "filterPromoStores": False,
            "range": 20,
            "noOfStores": 25,
            "excludeIndirect": False,
            "retrieveBy": "GEO"
        }
