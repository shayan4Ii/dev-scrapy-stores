from datetime import datetime
import logging
from typing import Optional, Generator

import scrapy
from scrapy.http import Response


class ArbysSpider(scrapy.Spider):
    """Spider for scraping Arby's restaurant data."""

    name = "arbys"
    allowed_domains = ["api.arbys.com"]
    start_urls = ["https://api.arbys.com/arb/web-exp-api/v1/location/list/details?countryCode=US"]

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the initial response and yield requests for each state."""
        try:
            data = response.json()
            for location in data.get("locationsByCountry", []):
                if location.get("countryCode") != "US":
                    continue
                
                for state_dict in location.get('statesOrProvinces', []):
                    state_code = state_dict.get('code', '').upper()
                    if state_code:
                        url = f'https://api.arbys.com/arb/web-exp-api/v1/location/list/details?countryCode=US&stateOrProvinceCode={state_code}'
                        yield scrapy.Request(url, callback=self.parse_state_locations)
        except Exception as e:
            self.logger.error(f"Error in parse method: {e}", exc_info=True)
    
    def parse_state_locations(self, response: Response) -> Generator[dict, None, None]:
        """Parse locations for a specific state."""
        try:
            data = response.json()
            for city_dict in data.get("stateOrProvince", {}).get("cities", []):
                for loc_dict in city_dict.get("locations", []):
                    store = self.parse_store(loc_dict)
                    if store:
                        yield store
        except Exception as e:
            self.logger.error(f"Error in parse_state_locations method: {e}", exc_info=True)

    def parse_store(self, store: dict) -> Optional[dict]:
        """Parse individual store data."""
        try:
            parsed_store = {
                "number": store.get("id"),
                "name": store.get("displayName"),
                "phone_number": store.get("contactDetails", {}).get("phone"),
                "address": self._get_address(store.get("contactDetails", {}).get("address", {})),
                "location": self._get_location(store.get("details", {})),
                "hours": self._get_hours(store.get("hoursByDay", {})),
                "services": store.get("services"),
                "url": f"https://www.arbys.com/locations/{store.get('url', '')}",
                "raw": store
            }

            # Check for missing required fields
            required_fields = ["address", "location", "url", "raw"]
            if not all(parsed_store.get(field) for field in required_fields):
                self.logger.warning(f"Discarding item due to missing required fields: {parsed_store}")
                return None

            for key, value in parsed_store.items():
                if value is None or (isinstance(value, (list, dict)) and not value):
                    self.logger.warning(f"Missing or empty data for {key}")

            return parsed_store
        except Exception as e:
            self.logger.error(f"Error in parse_store method: {e}", exc_info=True)
            return None
    
    def _get_address(self, address_info: dict) -> str:
        """Format the store address from store information."""
        try:
            street = address_info.get("line", "").strip()
            city = address_info.get("cityName", "").strip()
            state = address_info.get("stateProvinceCode", "").strip()
            zipcode = address_info.get("postalCode", "").strip()

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}

    def _get_hours(self, raw_hours: dict) -> dict:
        """Extract and parse store hours."""
        try:
            hours = {}
            day_abbr_map = {
                "Mon": "monday", "Tue": "tuesday", "Wed": "wednesday",
                "Thu": "thursday", "Fri": "friday", "Sat": "saturday", "Sun": "sunday"
            }

            for day, hours_info in raw_hours.items():
                open_time = hours_info.get("start", "")
                close_time = hours_info.get("end", "")
                day_name = day_abbr_map.get(day, "")

                if open_time and close_time and day_name:
                    hours[day_name] = {
                        "open": self._convert_to_12h_format(open_time),
                        "close": self._convert_to_12h_format(close_time)
                    }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return time_str