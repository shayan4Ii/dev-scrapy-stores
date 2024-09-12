from datetime import datetime
import re
import scrapy
import json
from typing import Dict, List, Optional, Iterator, Any

class HomesenseSpider(scrapy.Spider):
    name = "homesense"
    allowed_domains = ["us.homesense.com"]
    start_urls = ["https://us.homesense.com/locator#"]

    required_fields = ['address', 'location', 'url', 'raw']

    STORE_URLS_XPATH = '//ul[@class="states-list"]/li//a[@class="store-details-link"]/@href'
    STORE_DATA_RE = re.compile(r"pageProps: (.*),")

    def parse(self, response):
        for store_url in response.xpath(self.STORE_URLS_XPATH).getall():
            yield scrapy.Request(response.urljoin(store_url), callback=self.parse_store)

    def parse_store(self, response: scrapy.http.Response) -> Optional[Dict[str, Any]]:
        """Parse individual store page and extract store information."""
        try:
            store_json = self.STORE_DATA_RE.search(response.text).group(1)
            store_data = json.loads(store_json)
            store = store_data['document']

            parsed_store = {
                'number': str(store.get('id', '')),
                'phone_number': store.get('mainPhone', ''),
                'address': self._get_address(store.get('address', {})),
                'location': self._get_location(store.get('yextDisplayCoordinate', {})),
                'hours': self._get_hours(store.get('hours', {})),
                'services': self._get_services(store),
                'url': response.url,
                'raw': store
            }

            self._log_missing_data(parsed_store)

            if not all(parsed_store.get(field) for field in self.required_fields):
                self.logger.warning(f"Missing required fields for store {parsed_store.get('number', 'Unknown')}")
                return None

            return parsed_store
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse JSON data from store page: {response.url}")
        except KeyError as e:
            self.logger.error(f"Missing key in JSON data for store page: {response.url}, error: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse_store method: {e}", exc_info=True)
        return None

    def _get_services(self, store: dict) -> list:
        """Extract and format store services."""
        try:
            services_raw = store.get('_site', {}).get('c_servicesLocation', {}).get('servicesLink', [])
            services = [service.get('cTA', {}).get('label', '') for service in services_raw]
            services = [service for service in services if service]
            return services
        except Exception as e:
            self.logger.error(f"Error extracting services: {e}", exc_info=True)
            return []


    def _get_hours(self, hours_dict: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """Extract and format store hours."""
        formatted_hours = {}
        days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']

        for day, hours_info in hours_dict.items():
            if day.lower() not in days:
                self.logger.warning(f"Invalid day: {day}")
                continue
            parsed_hours = self._parse_hours(hours_info)
            if parsed_hours:
                formatted_hours[day] = parsed_hours
        return formatted_hours
    
    def _parse_hours(self, hours_info: Dict[str, Any]) -> Dict[str, str]:
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
            self.logger.error(f"Error parsing hours info: {e}, {hours_info}", exc_info=True)
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

    def _get_address(self, address_info: Dict[str, str]) -> str:
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

    def _get_location(self, location_info: Dict[str, float]) -> Dict[str, Any]:
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
        
    def _log_missing_data(self, parsed_store: Dict[str, Any]) -> None:
        """Log warnings for missing data in parsed store information."""
        for key, value in parsed_store.items():
            if key != 'raw' and not value:
                self.logger.warning(f"Missing data for {key} in store: {parsed_store['number']}")        


        
