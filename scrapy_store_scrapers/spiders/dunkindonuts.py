from datetime import datetime
import json

import scrapy



class DunkindonutsSpider(scrapy.Spider):
    name = "dunkindonuts"
    allowed_domains = ["locations.dunkindonuts.com"]
    start_urls = ["http://locations.dunkindonuts.com/en"]

    URLS_SCRIPT_TEXT_XPATH = '//script[contains(text(), "window.__INITIAL__DATA__")]/text()'

    def parse(self, response):
        script_text = response.xpath(self.URLS_SCRIPT_TEXT_XPATH).re_first(r'window.__INITIAL__DATA__ = (.*)')
        
        if not script_text:
            self.logger.error("Failed to extract script text")
            return
            
        data = json.loads(script_text)
        stores_url_data = data['document']['dm_directoryChildren']
        store_urls = self.extract_store_urls(stores_url_data)

        for store_url in store_urls:
            yield response.follow(store_url, self.parse_store)
            break

    def parse_store(self, response):
        
        script_text = response.xpath(self.URLS_SCRIPT_TEXT_XPATH).re_first(r'window.__INITIAL__DATA__ = (.*)')
        if not script_text:
            self.logger.error("Failed to extract script text")
            return
        
        data = json.loads(script_text)['document']

        parsed_store = {}

        parsed_store['number'] = str(data['id'])
        parsed_store['phone_number'] = data['mainPhone']

        parsed_store['address'] = self._get_address(data['address'])
        parsed_store['location'] = self._get_location(data['geocodedCoordinate'])
        parsed_store['hours'] = self._get_hours(data['hours'])

        parsed_store['services'] = data.get('c_storeFeatures', [])

        parsed_store['url'] = response.url
        parsed_store['raw'] = data

        return parsed_store

        
    def _get_hours(self, hours_dict: dict) -> list:

        hours = {}

        for day, hours_info in hours_dict.items():
            parsed_hours = self._parse_hours(hours_info)
            if parsed_hours:
                hours[day] = parsed_hours
    
        return hours
    
    def _parse_hours(self, hours_info: dict) -> dict:
        try:
            open_intervals = hours_info.get('openIntervals')

            if len(open_intervals) > 1:
                self.logger.error(f"Multiple intervals: {open_intervals}")
                return {}
            elif not open_intervals:
                self.logger.warning(f"No intervals found: {open_intervals}")
                return {}
            
            open_interval = open_intervals[0]

            open_time = open_interval.get("start")
            close_time = open_interval.get("end")

            if open_time and close_time:
                return {
                    "open": self._convert_to_12h_format(open_time),
                    "close": self._convert_to_12h_format(close_time)
                }
            self.logger.warning(f"Missing open or close time for hours info: {hours_info}")
        except Exception as error:
            self.logger.error(f"Error parsing hours info: {error}", exc_info=True)
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
        """Get the formatted store address."""
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
                self.logger.warning(f"Missing address for store with address info: {address_info}")
            return full_address
        except Exception as error:
            self.logger.error(f"Error formatting address: {error}", exc_info=True)
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
            self.logger.warning(f"Missing latitude or longitude for store with location info: {location_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}


    @staticmethod
    def extract_store_urls(data):
        urls = []
        
        def recursive_extract(item):
            if isinstance(item, dict):
                if 'slug' in item and 'dm_directoryChildren' not in item:
                    urls.append(item['slug'])
                for value in item.values():
                    recursive_extract(value)
            elif isinstance(item, list):
                for element in item:
                    recursive_extract(element)
        
        recursive_extract(data)

        urls = list(dict.fromkeys(urls))
        return urls
            

