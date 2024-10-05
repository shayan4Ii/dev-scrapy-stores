import json
import logging
from datetime import datetime
from typing import Any, Generator, Optional
from urllib.parse import urlencode

import scrapy
from scrapy.http import Request, Response


class StewartsShopsSpider(scrapy.Spider):
    """Spider for scraping Stewart's Shops store information."""

    name = "stewartsshops"
    start_urls = [
        "https://uberall.com/api/mf-lp-adapter/v2/llp/sitemap?auth_token=ZGRQTRLWHXDMDNUO&country=US&multi_account=false"
    ]
    listing_detail_url = "https://uberall.com/api/mf-lp-adapter/llp.json"

    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "authorization": "ZGRQTRLWHXDMDNUO",
        "priority": "u=1, i",
        "sec-ch-ua": '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "cross-site",
        "Referer": "https://locations.stewartsshops.com/",
        "Referrer-Policy": "strict-origin-when-cross-origin"
    }

    def parse(self, response: Response) -> Generator[Request, None, None]:
        """Parse the initial response and yield requests for individual locations."""
        try:
            locations = response.json()["locations"]
        except KeyError:
            self.logger.error(
                "Failed to extract locations from response", exc_info=True)
            return

        for location in locations:
            store_info = location.get("store_info", {})
            params = {
                "address": store_info.get("address"),
                "country": store_info.get("country"),
                "locality": store_info.get("locality"),
                "multi_account": "false",
                "pageSize": "30",
                "region": store_info.get("region"),
            }
            url = f"{self.listing_detail_url}?{urlencode(params)}"
            yield Request(
                url=url,
                method="GET",
                headers=self.headers,
                callback=self.parse_location,
                errback=self.handle_error
            )

    def parse_location(self, response: Response) -> Generator[dict, None, None]:
        """Parse individual location data."""
        try:
            results = response.json()
        except json.JSONDecodeError:
            self.logger.error("Failed to decode JSON response", exc_info=True)
            return

        if not results:
            self.logger.warning("No data found for location: %s", response.url)
        elif len(results) > 1:
            self.logger.warning(
                "Multiple results found for location: %s", response.url)
        else:
            parsed_data = self._parse_location_data(results[0])
            if all(parsed_data.get(field) for field in ['address', 'location', 'url', 'raw']):
                yield parsed_data
            else:
                self.logger.warning(
                    "Discarding item due to missing required fields")

    def _parse_location_data(self, data: dict) -> dict:
        """Parse and structure location data."""
        store_info = data.get("store_info", {})
        custom_fields = data.get("custom_fields", [])

        parsed_store = {
            "number": store_info.get("corporate_id"),
            "name": self._get_store_name(store_info, custom_fields),
            "phone_number": store_info.get("phone"),
            "address": self._get_address(store_info),
            "location": self._get_location(store_info),
            "services": self._get_services(custom_fields),
            "hours": self._get_hours(store_info),
            "url": f"https://locations.stewartsshops.com{data.get('llp_url')}",
            "raw": data
        }

        return parsed_store

    def _get_store_name(self, store_info: dict, custom_fields: list) -> str:
        """Extract store name from custom fields or store info."""
        # Check custom fields first
        store_name = self._get_custom_field(custom_fields, "store_name")
        if store_name:
            return store_name.strip()

        # Check LLP name if store name is not found
        llp_name = self._get_custom_field(custom_fields, "llp_name")
        if llp_name:
            return llp_name.split("#")[0].strip().strip('-').strip()

        # Fall back to store_info name if custom fields don't have the name
        return store_info.get("name", "").strip()

    def _get_custom_field(self, custom_fields: list, field: str) -> Optional[str]:
        """Extract a specific custom field from the list of custom fields."""
        for custom_field in custom_fields:
            if custom_field["name"] == field:
                return custom_field["data"]
        return None

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address", ""),
                store_info.get("address_extended", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("locality", "")
            state = store_info.get("region", "")
            zipcode = store_info.get("postcode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information for store: %s", store_info.get(
                    'storeNumber', 'Unknown'))
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address for store %s: %s", store_info.get(
                'storeNumber', 'Unknown'), e, exc_info=True)
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

            self.logger.warning("Missing latitude or longitude for store: %s", store_info.get(
                'storeNumber', 'Unknown'))
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values for store %s: %s", store_info.get(
                'storeNumber', 'Unknown'), error)
        except Exception as error:
            self.logger.error("Error extracting location for store %s: %s", store_info.get(
                'storeNumber', 'Unknown'), error, exc_info=True)
        return {}

    def _get_services(self, custom_fields: list) -> list:
        """Extract and parse services from custom fields."""
        services_html = self._get_custom_field(custom_fields, "amenities_text")
        if services_html:
            sel = scrapy.Selector(text=services_html)
            services_text = sel.xpath("normalize-space(.)").get()
            if services_text:
                services_text = services_text.replace(
                    "Shop Features:", "").strip()
                return [service.strip() for service in services_text.split(";")]
        return []

    def _get_hours(self, store_info: dict) -> dict:
        """Extract and parse store hours."""
        try:
            hours_text = store_info.get("store_hours")
            if not hours_text:
                self.logger.warning(
                    "No hours found for store: %s", store_info.get('storeNumber', 'Unknown'))
                return {}

            hours = {}
            days = {
                "1": "monday", "2": "tuesday", "3": "wednesday", "4": "thursday",
                "5": "friday", "6": "saturday", "7": "sunday"
            }

            for day_hour in hours_text.split(";"):
                day_index, open_time, close_time = day_hour.split(",")
                day = days.get(day_index)
                if day:
                    hours[day] = {
                        "open": self._convert_to_12h_format(open_time),
                        "close": self._convert_to_12h_format(close_time)
                    }
            return hours
        except Exception as e:
            self.logger.error(
                "Error getting store hours: %s", e, exc_info=True)
            return {}

    def _convert_to_12h_format(self, time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            if time_str == "2400":
                return "12:00 am"
            time_obj = datetime.strptime(time_str, '%H%M').time()
            return time_obj.strftime('%I:%M %p').lower().lstrip('0')
        except ValueError:
            self.logger.error("Invalid time value: %s", time_str)
            return time_str

    def handle_error(self, failure: Any) -> None:
        """Handle request failures."""
        self.logger.error("Request failed: %s", failure)
