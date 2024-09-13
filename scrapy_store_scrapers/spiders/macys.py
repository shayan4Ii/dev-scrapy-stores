import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Generator, Any

import scrapy
from scrapy.http import Response


class MacysSpider(scrapy.Spider):
    """Spider for scraping Macy's store information."""

    name = "macys"
    allowed_domains = ["www.macys.com"]
    start_urls = ["https://www.macys.com/stores/browse/"]

    LOC_PAGE_URLS_XPATH = '//div[@class="map-list-item is-single"]/a/@href'
    STORE_URLS_XPATH = '//a[@class="ga-link location-details-link"]/@href'

    STORE_INFO_JSON_RE = re.compile(r'"info":"<div class=\\"tlsmap_popup\\">(.*)<\\/div>')

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the initial response and follow location and store URLs."""
        for url in self._get_urls(response, self.LOC_PAGE_URLS_XPATH):
            yield response.follow(url, self.parse)

        for url in self._get_urls(response, self.STORE_URLS_XPATH):
            yield response.follow(url, self.parse_store)

    def _get_urls(self, response: Response, xpath: str) -> List[str]:
        """Extract URLs from the response using the given XPath."""
        urls = response.xpath(xpath).getall()
        if not urls:
            self.logger.debug(f"No URLs found for XPath: {xpath}")
        return urls

    def parse_store(self, response: Response) -> Generator[Dict[str, Any], None, None]:
        """Parse individual store information."""
        try:
            store_escaped_json = self._extract_store_json(response.text)
            unescaped_json_text = self._unescape_json(store_escaped_json)
            store_info_json = self._fix_json_structure(unescaped_json_text)
            store_data = json.loads(store_info_json)

            store_data['hours_sets:primary'] = json.loads(store_data['hours_sets:primary'])

            parsed_store = self._parse_store_data(store_data)

            if self._validate_parsed_store(parsed_store):
                yield parsed_store
            else:
                self.logger.warning(f"Discarding item due to missing required fields: {parsed_store}")
        except Exception as e:
            self.logger.error(f"Error parsing store: {e}", exc_info=True)

    def _extract_store_json(self, response_text: str) -> str:
        """Extract store JSON from the response text."""
        match = self.STORE_INFO_JSON_RE.search(response_text)
        if not match:
            raise ValueError("Store info JSON not found in response")
        return match.group(1)

    @staticmethod
    def _fix_json_structure(json_str: str) -> str:
        """Fix the structure of the JSON string."""
        match = re.search(r'"hours_sets:primary": "({.*?})"', json_str)
        if match:
            nested_json = match.group(1)
            escaped_nested_json = nested_json.replace('"', '\\"')
            json_str = json_str.replace(match.group(0), f'"hours_sets:primary": "{escaped_nested_json}"')
        return json_str

    @staticmethod
    def _unescape_json(escaped_json: str) -> str:
        """Unescape the JSON string."""
        return json.loads(f'"{escaped_json}"')

    def _parse_store_data(self, store_data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse the store data into a structured format."""
        return {
            "number": store_data.get("fid"),
            "name": store_data.get("district"),
            "phone_number": store_data.get("local_phone"),
            "address": self._get_address(store_data),
            "location": self._get_location(store_data),
            "hours": self._get_hours(store_data),
            "services": store_data.get("services_cs", "").split(","),
            "url": store_data.get("url"),
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