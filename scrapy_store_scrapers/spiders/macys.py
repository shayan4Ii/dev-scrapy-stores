from datetime import datetime
import json
import re
import scrapy


class MacysSpider(scrapy.Spider):
    name = "macys"
    allowed_domains = ["www.macys.com"]
    start_urls = ["https://www.macys.com/stores/browse/"]

    LOC_PAGE_URLS_XPATH = '//div[@class="map-list-item is-single"]/a/@href'
    STORE_URLS_XPATH = '//a[@class="ga-link location-details-link"]/@href'

    STORE_INFO_JSON_RE = re.compile(r'"info":"<div class=\\"tlsmap_popup\\">(.*)<\\/div>')

    def parse(self, response):
        
        location_urls = response.xpath(self.LOC_PAGE_URLS_XPATH).getall()
        if location_urls:
            for url in location_urls:
                yield response.follow(url, self.parse)


        store_urls = response.xpath(self.STORE_URLS_XPATH).getall()
        if store_urls:
            for url in store_urls:
                yield response.follow(url, self.parse_store)

    def parse_store(self, response):
        store_escaped_json = self.STORE_INFO_JSON_RE.search(response.text).group(1)
        unescaped_json_text = self.unescape_json(store_escaped_json)
        store_info_json = self.fix_json_structure(unescaped_json_text)

        store_data = json.loads(store_info_json)

        store_data['hours_sets:primary'] = json.loads(store_data['hours_sets:primary'])

        parsed_store = {}

        parsed_store["number"] = store_data.get("fid")
        parsed_store["name"] = store_data.get("location_name")
        parsed_store["phone_number"] = store_data.get("local_phone")

        parsed_store["address"] = self._get_address(store_data)
        parsed_store["location"] = self._get_location(store_data)
        parsed_store["hours"] = self._get_hours(store_data)

        parsed_store["services"] = store_data.get("services_cs").split(",")

        parsed_store["url"] = store_data.get("url")
        parsed_store["raw"] = store_data


        yield parsed_store
    
    @staticmethod
    def fix_json_structure(json_str):
        # Find the problematic nested JSON
        match = re.search(r'"hours_sets:primary": "({.*?})"', json_str)
        if match:
            nested_json = match.group(1)
            # Escape quotes within the nested JSON
            escaped_nested_json = nested_json.replace('"', '\\"')
            # Replace the original nested JSON with the escaped version
            json_str = json_str.replace(match.group(0), f'"hours_sets:primary": "{escaped_nested_json}"')
        return json_str

    @staticmethod
    def unescape_json(escaped_json):
        # Use json.loads to properly unescape the string
        return json.loads(f'"{escaped_json}"')
    
    def _get_address(self, store_info: dict) -> str:
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

    def _get_location(self, store_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('lat')
            longitude = store_info.get('lng')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store with location info: {store_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}
    
    def _get_hours(self, raw_store_data: dict) -> dict:
        """Extract and parse store hours."""
        try:
            hours_raw = raw_store_data.get("hours_sets:primary", {}).get("days", {})
            if not hours_raw:
                self.logger.warning(f"No hours found for store {raw_store_data.get('StoreName', 'Unknown')}")
                return {}

            hours = {}

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

