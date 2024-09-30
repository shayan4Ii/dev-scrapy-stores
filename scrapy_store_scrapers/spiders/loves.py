import logging
from typing import Optional, Generator, Dict, List, Any

import scrapy
from scrapy.http import Request, Response


class LovesSpider(scrapy.Spider):
    """Spider for scraping Loves store data."""

    name = "loves"
    allowed_domains = ["www.loves.com"]
    api_format_url = 'https://www.loves.com/api/sitecore/StoreSearch/SearchStoresWithDetail?pageNumber={}&top=50&lat=38.130353038797594&lng=-97.81370249999999'

    def start_requests(self) -> Generator[Request, None, None]:
        """Generate initial requests to start scraping."""
        yield scrapy.Request(
            url=self.api_format_url.format(0),
            callback=self.parse,
            cb_kwargs=dict(page=0)
        )

    def parse(self, response: Response, page: int) -> Generator[Dict[str, Any], None, None]:
        """Parse the API response and yield store data."""
        try:
            data = response.json()
            for store in data:
                parsed_store = self._parse_store(store)
                if self._validate_store(parsed_store):
                    yield parsed_store
                else:
                    self.logger.warning(f"Discarded invalid store: {store.get('Number', 'Unknown')}")

            if data:
                yield scrapy.Request(
                    url=self.api_format_url.format(page + 1),
                    callback=self.parse,
                    cb_kwargs=dict(page=page + 1)
                )
        except Exception as e:
            self.logger.error(f"Error parsing response: {e}", exc_info=True)

    def _parse_store(self, store: dict) -> dict:
        """Parse individual store data."""
        try:
            parsed_store = {
                'number': str(store.get('Number', '')),
                'name': store.get('PreferredName', ''),
                'phone_number': store.get('MainPhone', ''),
                'address': self._get_address(store),
                'location': self._get_location(store),
                'services': self._get_services(store),
                'hours': self._get_hours(store),
                'url': "https://www.loves.com/en/location-and-fuel-price-search/locationsearchresults#?state=All&city=All&highway=All",
                'raw': store
            }
            return parsed_store
        except Exception as e:
            self.logger.error(f"Error parsing store: {e}", exc_info=True)
            return {}

    def _validate_store(self, store: dict) -> bool:
        """Validate if the store has all required fields."""
        required_fields = ['address', 'location', 'url', 'raw']
        return all(store.get(field) for field in required_fields)

    def _get_hours(self, store_info: dict) -> dict:
        """Extract and format store hours."""
        try:
            days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            business_hours = store_info.get("BusinessHours", [])
            loc_business_hours = store_info.get("LocationBusinessHours", [])

            hours = {}
            if business_hours:
                for day in business_hours:
                    day_name = day["SmaFieldName"].lower()
                    hours[day_name] = self._get_open_close(day["FieldValue"])
            elif loc_business_hours:
                for hours_info in loc_business_hours:
                    if hours_info["SmaFieldName"] == "storehours":
                        for day_name in days:
                            hours[day_name] = self._get_open_close(hours_info["FieldValue"])

            if not hours:
                self.logger.warning(f"No hours found for store: {store_info.get('Number')}")

            return hours
        except Exception as e:
            self.logger.error(f"Error extracting hours: {e}", exc_info=True)
            return {}

    def _get_open_close(self, hours_text: str) -> dict:
        """Extract open and close times from hours text."""
        try:
            hours_text = hours_text.strip().lower().replace('.', '')
            if hours_text == "open 24-hours":
                return {"open": "12:00 am", "close": "11:59 pm"}

            separator = "–" if "–" in hours_text else "-"
            open_time, close_time = hours_text.split(separator)

            return {
                "open": self._convert_time_format(open_time.strip()),
                "close": self._convert_time_format(close_time.strip())
            }
        except ValueError as error:
            self.logger.warning(f"Invalid open/close times: {hours_text}, {error}")
        except Exception as error:
            self.logger.error(f"Error extracting open/close times: {error}", exc_info=True)
        return {}

    def _get_services(self, store_info: dict) -> List[str]:
        """Extract and format services."""
        try:
            return [service['FieldName'] for service in store_info.get("Amenities", [])]
        except Exception as e:
            self.logger.error(f"Error extracting services: {e}", exc_info=True)
            return []

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            street = store_info.get("Address", "").strip()
            city = store_info.get("City", "").strip()
            state = store_info.get("State", "").strip()
            zipcode = store_info.get("Zip", "").strip()

            city_state_zip = f"{city}, {state} {zipcode}".strip()
            full_address = ", ".join(filter(None, [street, city_state_zip]))

            if not full_address:
                self.logger.warning(f"Missing address information for store: {store_info.get('Number', 'Unknown')}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address for store {store_info.get('Number', 'Unknown')}: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> Optional[dict]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('Latitude')
            longitude = store_info.get('Longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store: {store_info.get('Number')}")
            return None
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return None

    @staticmethod
    def _convert_time_format(time_str: str) -> str:
        """Convert time string to a standardized format."""
        if ':' in time_str:
            return time_str
        
        parts = time_str.split()
        
        if len(parts) != 2:
            raise ValueError("Invalid time format. Please use 'X am' or 'X pm'.")
        
        hours, period = parts
        
        try:
            hours = int(hours)
        except ValueError:
            raise ValueError("Invalid hour. Please use a number.")
        
        if hours < 1 or hours > 12:
            raise ValueError("Hours must be between 1 and 12.")
        
        if period.lower() not in ['am', 'pm']:
            raise ValueError("Period must be 'am' or 'pm'.")
        
        return f"{hours:d}:00 {period.lower()}"