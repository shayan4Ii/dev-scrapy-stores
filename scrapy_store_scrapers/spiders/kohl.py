import json
import re
from datetime import datetime
from typing import Any, Dict, Generator, Optional

import scrapy
from scrapy.http import Response


class KohlSpider(scrapy.Spider):
    """Spider for scraping Kohl's store information."""

    name = "kohl"
    allowed_domains = ["kohls.com"]
    start_urls = ["https://www.kohls.com/stores.shtml"]

    location_urls_xpath = '//div[@class="browse-wrapper"]/ul/li/div/a/@href'
    store_urls_xpath = '//div[@class="city-list-wrapper"]/ul/li//a[text()="Store Info"]/@href'

    STORES_INFO_JSON_RE = re.compile(r'\$config.defaultListData = \'(.*)\';')

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to location pages."""
        location_urls = response.xpath(self.location_urls_xpath).getall()

        for location_url in location_urls:
            yield response.follow(location_url, self.parse)

        store_urls = response.xpath(self.store_urls_xpath)

        if store_urls:
            yield from self.parse_stores(response)

    def parse_stores(self, response: Response) -> Generator[Dict[str, Any], None, None]:
        """Parse store information from the response."""
        for store in self.get_stores(response):
            parsed_store = self._parse_store_data(store)
            if self._validate_parsed_store(parsed_store):
                yield parsed_store
            else:
                self.logger.warning(f"Discarding incomplete store data: {parsed_store}")

    def get_stores(self, response: Response) -> Generator[Dict[str, Any], None, None]:
        """Extract store data from the response."""
        try:
            store_info_json = self.STORES_INFO_JSON_RE.search(response.text)
            if not store_info_json:
                self.logger.error("Failed to find store information JSON in the response")
                return

            unescaped_text = store_info_json.group(1).encode().decode('unicode_escape')
            stores_data = json.loads(unescaped_text)

            for store in stores_data:
                try:
                    store['hours_sets:primary'] = json.loads(store['hours_sets:primary'])
                    yield store
                except json.JSONDecodeError:
                    self.logger.warning(f"Failed to parse hours for store: {store.get('fid')}")
        except Exception as e:
            self.logger.error(f"Error extracting store data: {e}", exc_info=True)

    def _parse_store_data(self, store_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse the store data into a structured format."""
        return {
            "number": store_data.get("fid"),
            "name": store_data.get("address_1"),
            "phone_number": store_data.get("local_phone"),
            "address": self._get_address(store_data),
            "location": self._get_location(store_data),
            "hours": self._get_hours(store_data),
            "services": store_data.get("Store Services_CS", "").split(","),
            "url": store_data.get("url_native"),
            "raw": store_data
        }

    def _validate_parsed_store(self, parsed_store: Dict[str, Any]) -> bool:
        """Validate the parsed store data."""
        required_fields = ["address", "location", "url", "raw"]
        return all(parsed_store.get(field) for field in required_fields)

    def _get_address(self, store_info: Dict[str, Any]) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address_1", ""),
                store_info.get("address_2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("region", "")
            zipcode = store_info.get("post_code", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information: {store_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('lat')
            longitude = store_info.get('lng')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store: {store_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}

    def _get_hours(self, raw_store_data: Dict[str, Any]) -> Dict[str, Dict[str, Optional[str]]]:
        """Extract and parse store hours."""
        try:
            hours_raw = raw_store_data.get("hours_sets:primary", {}).get("days", {})
            if not hours_raw:
                self.logger.warning(f"No hours found for store {raw_store_data}")
                return {}

            hours: Dict[str, Dict[str, Optional[str]]] = {}

            for day, day_hours_list in hours_raw.items():
                day = day.lower()

                if not isinstance(day_hours_list, list):
                    continue

                if len(day_hours_list) != 1:
                    self.logger.warning(f"Unexpected day hours list for {day}: {day_hours_list}")
                    hours[day] = {"open": None, "close": None}
                    continue

                day_hours = day_hours_list[0]

                open_time = self._convert_to_12h_format(day_hours.get("open", ""))
                close_time = self._convert_to_12h_format(day_hours.get("close", ""))

                if not open_time or not close_time:
                    self.logger.warning(f"Missing open or close time for {day} hours: {day_hours}")
                    hours[day] = {"open": None, "close": None}
                else:
                    hours[day] = {"open": open_time, "close": close_time}

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
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            return time_str