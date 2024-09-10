from datetime import datetime
import json
import logging
from typing import Generator

import scrapy
from scrapy.http import Response


class SchnucksSpider(scrapy.Spider):
    """
    A spider to scrape location data from Schnucks' website.
    """
    name = "schnucks"
    allowed_domains = ["locations.schnucks.com"]
    start_urls = ["http://locations.schnucks.com/"]

    SCRIPT_TEXT_XPATH = '//script[@id="__NEXT_DATA__"]/text()'

    def parse(self, response: Response) -> Generator[dict, None, None]:
        """
        Parse the response and extract location data.

        Args:
            response (Response): The response object from the request.

        Yields:
            Generator[dict, None, None]: An Generator of dictionaries containing location data.
        """
        self.logger.info(f"Parsing response from {response.url}")

        try:
            json_text = response.xpath(self.SCRIPT_TEXT_XPATH).get()
            if not json_text:
                self.logger.error("Failed to find script tag with location data")
                return

            json_data = json.loads(json_text)
            locations = json_data["props"]["pageProps"]["locations"]

            self.logger.info(f"Found {len(locations)} locations")

            for store in locations:
                yield self.parse_store(store)

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON data: {str(e)}")
        except KeyError as e:
            self.logger.error(f"Failed to access expected key in JSON data: {str(e)}")
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")


    def parse_store(self, store: dict) -> dict:
        parsed_store = {}

        parsed_store["name"] = store["businessName"]
        parsed_store["number"] = store["storeCode"]

        parsed_store["phone"] = self._get_phone(store)
        parsed_store["address"] = self._get_address(store)
        parsed_store["location"] = self._get_location(store)
        parsed_store["services"] = self._get_services(store)
        parsed_store['hours'] = self._get_hours(store)
        
        parsed_store['url'] = store['websiteURL']
        parsed_store['raw'] = store

        return parsed_store
    
    def _get_hours(self, store_info: dict) -> dict:
        try:
            days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
            hours = store_info.get('businessHours', [])
            
            if len(days) != len(hours):
                self.logger.warning(f"Missing hours for store with info: {store_info}")
                return {}
            
            hours_dict = {}
            for day, hour in zip(days, hours):
                hours_dict[day] = {
                    'open': self._convert_to_12h_format(hour[0]),
                    'close': self._convert_to_12h_format(hour[1])
                }

            return hours_dict

        except Exception as e:
            self.logger.error(f"Error getting hours: {e}", exc_info=True)

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

    def _get_services(self, store_info: dict) -> list[str]:
        """Extract available services from store information."""
        try:
            custom = store_info.get("custom", {})
            depts = custom.get("depts", {})
            dept_names = [dept.title() for dept in depts.keys()]

            services = custom.get("services", [])
            service_names = [service['name'] for service in services]

            all_services = dept_names + service_names
            return all_services
        except Exception as e:
            self.logger.error(f"Error getting services: {e}", exc_info=True)
            return []
        
    def _get_phone(self, store_info: dict) -> str:
        for phone in store_info.get('phoneNumbers', []):
                return phone

    def _get_address(self, store_info: dict) -> str:
        """Get the formatted store address."""
        try:
            address_parts = store_info.get('addressLines', [])
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address for store with info: {store_info}")
            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
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