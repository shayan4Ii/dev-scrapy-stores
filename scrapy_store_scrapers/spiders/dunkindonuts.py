import json
from datetime import datetime
from typing import Any, Generator

import scrapy
from scrapy.http import Response


class DunkinDonutsSpider(scrapy.Spider):
    """Spider for scraping Dunkin' Donuts store locations."""

    name = "dunkindonuts"
    allowed_domains = ["locations.dunkindonuts.com"]
    start_urls = ["http://locations.dunkindonuts.com/en"]

    URLS_SCRIPT_TEXT_XPATH = '//script[contains(text(), "window.__INITIAL__DATA__")]/text()'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and yield requests for individual store pages."""
        try:
            script_text = response.xpath(self.URLS_SCRIPT_TEXT_XPATH).re_first(r'window.__INITIAL__DATA__ = (.*)')
            
            if not script_text:
                self.logger.error("Failed to extract script text from main page")
                return

            data = json.loads(script_text)
            stores_url_data = data['document']['dm_directoryChildren']
            store_urls = self.extract_store_urls(stores_url_data)

            for store_url in store_urls:
                yield response.follow(store_url, self.parse_store)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON data from main page")
        except KeyError as e:
            self.logger.error(f"Missing key in JSON data: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse method: {e}", exc_info=True)

    def parse_store(self, response: Response) -> dict:
        """Parse individual store page and extract store information."""
        try:
            script_text = response.xpath(self.URLS_SCRIPT_TEXT_XPATH).re_first(r'window.__INITIAL__DATA__ = (.*)')
            if not script_text:
                self.logger.error(f"Failed to extract script text from store page: {response.url}")
                return {}
            
            data = json.loads(script_text)['document']

            parsed_store = {
                'number': str(data.get('id', '')),
                'phone_number': data.get('mainPhone', ''),
                'address': self._get_address(data.get('address', {})),
                'location': self._get_location(data.get('geocodedCoordinate', {})),
                'hours': self._get_hours(data.get('hours', {})),
                'services': data.get('c_storeFeatures', []),
                'url': response.url,
                'raw': data
            }

            self._log_missing_data(parsed_store)

            return parsed_store
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON data from store page: {response.url}")
        except KeyError as e:
            self.logger.error(f"Missing key in JSON data for store page: {response.url}, error: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse_store method: {e}", exc_info=True)
        return {}

    def _get_hours(self, hours_dict: dict) -> dict:
        """Extract and format store hours."""
        formatted_hours = {}
        for day, hours_info in hours_dict.items():
            parsed_hours = self._parse_hours(hours_info)
            if parsed_hours:
                formatted_hours[day] = parsed_hours
        return formatted_hours
    
    def _parse_hours(self, hours_info: dict) -> dict:
        """Parse hours information for a single day."""
        try:
            open_intervals = hours_info.get('openIntervals', [])

            if len(open_intervals) > 1:
                self.logger.warning(f"Multiple intervals found: {open_intervals}")
                return {}
            elif not open_intervals:
                self.logger.warning(f"No intervals found: {hours_info}")
                return {}
            
            open_interval = open_intervals[0]
            open_time = open_interval.get("start")
            close_time = open_interval.get("end")

            if open_time and close_time:
                return {
                    "open": self._convert_to_12h_format(open_time),
                    "close": self._convert_to_12h_format(close_time)
                }
            self.logger.warning(f"Missing open or close time: {hours_info}")
        except Exception as e:
            self.logger.error(f"Error parsing hours info: {e}", exc_info=True)
        return {}
    
    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return ""
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            return time_str

    def _get_address(self, address_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("line1", ""),
                address_info.get("line2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("city", "")
            state = address_info.get("region", "")
            zipcode = address_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information: {address_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, location_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = location_info.get('latitude')
            longitude = location_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude: {location_info}")
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}

    @staticmethod
    def extract_store_urls(data: Any) -> list:
        """Recursively extract store URLs from the data structure."""
        urls = []
        
        def recursive_extract(item: Any) -> None:
            if isinstance(item, dict):
                if 'slug' in item and 'dm_directoryChildren' not in item:
                    urls.append(item['slug'])
                for value in item.values():
                    recursive_extract(value)
            elif isinstance(item, list):
                for element in item:
                    recursive_extract(element)
        
        recursive_extract(data)
        return list(dict.fromkeys(urls))

    def _log_missing_data(self, parsed_store: dict) -> None:
        """Log warnings for missing data in parsed store information."""
        for key, value in parsed_store.items():
            if key != 'raw' and not value:
                self.logger.warning(f"Missing data for {key}")