import json
import re
from typing import Generator, Any

import scrapy
from scrapy.http import Response


class PigglyWigglySpider(scrapy.Spider):
    """Spider for scraping Piggly Wiggly store locations."""

    name = "pigglywiggly"
    allowed_domains = ["www.pigglywiggly.com"]
    start_urls = ["https://www.pigglywiggly.com/store-locations/"]

    STATE_PAGE_URLS_XPATH = '//ul[@id="menu-main-menu-1"]/li/a/@href'
    STORES_JSON_RE = re.compile(r'var locations = (.*);')

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and yield requests for state pages."""
        for url in response.xpath(self.STATE_PAGE_URLS_XPATH).getall():
            yield scrapy.Request(url, callback=self.parse_stores)

    def parse_stores(self, response: Response) -> Generator[dict, None, None]:
        """Parse store information from state pages."""
        stores_info = self.get_stores(response)
        for store_info in stores_info:
            parsed_store = self.parse_store(store_info, response.url)
            if self.is_valid_store(parsed_store):
                yield parsed_store
            else:
                self.logger.warning(f"Discarding invalid store: {store_info.get('storeNumber', 'Unknown')}")

    def parse_store(self, store_info: dict, state_page_url: str) -> dict:
        """Parse individual store information."""
        parsed_store = {
            "number": store_info.get("storeNumber"),
            "name": store_info.get("name"),
            "phone_number": store_info.get("phone"),
            "address": self._get_address(store_info),
            "location": self._get_location(store_info),
            "url": state_page_url,
            "raw": store_info
        }
        return parsed_store

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address1", ""),
                store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information for store: {store_info.get('storeNumber', 'Unknown')}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address for store {store_info.get('storeNumber', 'Unknown')}: {e}", exc_info=True)
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

            self.logger.warning(f"Missing latitude or longitude for store: {store_info.get('storeNumber', 'Unknown')}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values for store {store_info.get('storeNumber', 'Unknown')}: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location for store {store_info.get('storeNumber', 'Unknown')}: {error}", exc_info=True)
        return {}
    
    def get_stores(self, response: Response) -> list:
        """Extract store data from the page's JavaScript."""
        try:
            stores_json = self.STORES_JSON_RE.search(response.text)
            if stores_json:
                data = json.loads(stores_json.group(1))
                return list(data.values()) if isinstance(data, dict) else data
            else:
                self.logger.error("Failed to find store data in the page.")
                return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse store JSON data: {e}", exc_info=True)
            return []
        except Exception as e:
            self.logger.error(f"Unexpected error while getting stores: {e}", exc_info=True)
            return []

    def is_valid_store(self, store: dict) -> bool:
        """Check if a store has all required fields."""
        required_fields = ['address', 'location', 'url', 'raw']
        return all(store.get(field) for field in required_fields)