from datetime import datetime
from typing import Any, Generator, Optional

import scrapy
from scrapy.http import Response


class OreillyAutoSpider(scrapy.Spider):
    """Spider for scraping O'Reilly Auto Parts store information."""

    name = "oreillyauto"
    allowed_domains = ["locations.oreillyauto.com"]
    start_urls = ["http://locations.oreillyauto.com/"]

    # XPath selectors
    LOCATION_URLS_XPATH = '//ul[@id="browse-content"]/li/div/a/@href'
    STORE_URLS_XPATH = '//ul[@id="browse-content"]//a[contains(text(), "Store Details")]'
    NAME_XPATH = '//span[@class="location-name"]/text()'
    STORE_NUMBER_XPATH = 'normalize-space(//span[@class="store-number"])'
    PHONE_XPATH = '//span[contains(@class, "phone")]/div/text()'
    ADDRESS_PARTS_XPATH = '//h2/span[@class="address"]'
    LAT_LONG_SCRIPT_XPATH = '//script[@type="application/ld+json"]'
    HOURS_ROWS_XPATH = '//div[contains(@class, "store-hours ")]/div/div[contains(@class, "day-hour-row")]'
    DAY_PART_XPATH = './span[@class="daypart"]/@data-daypart'
    OPEN_TIME_XPATH = './/span[@class="time-open"]/text()'
    CLOSE_TIME_XPATH = './/span[@class="time-close"]/text()'
    SERVICES_XPATH = '//ul[contains(@class, "location-specialties")]/li/@data-specialty-name'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and follow links to location pages."""
        location_urls = response.xpath(self.LOCATION_URLS_XPATH).getall()
        store_urls = response.xpath(self.STORE_URLS_XPATH)

        for location_url in location_urls:
            yield response.follow(location_url, self.parse)

        for store_url in store_urls:
            yield response.follow(store_url, self.parse_store)

        if not store_urls and not location_urls:
            yield self.parse_store(response)

    def parse_store(self, response: Response) -> dict[str, Any]:
        """Parse individual store pages."""
        try:
            parsed_store = {
                "number": self._extract_store_number(response),
                "name": response.xpath(self.NAME_XPATH).get('').strip(),
                "phone_number": response.xpath(self.PHONE_XPATH).get('').strip(),
                "address": self._get_address(response),
                "location": self._get_location(response),
                "hours": self._get_hours(response),
                "services": response.xpath(self.SERVICES_XPATH).getall(),
                "url": response.url,
            }

            if self._validate_parsed_store(parsed_store):
                return parsed_store
            else:
                self.logger.warning(f"Discarded item due to missing required fields: {response.url}")
        except Exception as e:
            self.logger.error(f"Error parsing store {response.url}: {e}", exc_info=True)

        return None

    def _validate_parsed_store(self, parsed_store: dict[str, Any]) -> bool:
        """Validate the parsed store data."""
        required_fields = ["address", "location", "url"]
        return all(parsed_store.get(field) for field in required_fields)

    def _extract_store_number(self, response: Response) -> Optional[str]:
        """Extract store number from the response."""
        try:
            return response.xpath(self.STORE_NUMBER_XPATH).re_first(r'#\s(\d+)')
        except Exception as e:
            self.logger.error(f"Error extracting store number: {e}", exc_info=True)
            return None

    def _get_address(self, response: Response) -> str:
        """Format store address."""
        try:
            address_parts = [adr.xpath('normalize-space(.)').get('') for adr in response.xpath(self.ADDRESS_PARTS_XPATH)]
            if not address_parts:
                self.logger.warning(f"No address parts found for store: {response.url}")
                return ""
            
            return ", ".join(address_parts)
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, response: Response) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = response.xpath(self.LAT_LONG_SCRIPT_XPATH).re_first(r'"latitude": "([^"]+)"')
            longitude = response.xpath(self.LAT_LONG_SCRIPT_XPATH).re_first(r'"longitude": "([^"]+)"')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store: {response.url}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}

    def _get_hours(self, response: Response) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}
            for row in response.xpath(self.HOURS_ROWS_XPATH):
                day = row.xpath(self.DAY_PART_XPATH).get('').lower()
                open_time = row.xpath(self.OPEN_TIME_XPATH).get('').lower()
                close_time = row.xpath(self.CLOSE_TIME_XPATH).get('').lower()
                hours[day] = {
                    "open": open_time,
                    "close": close_time
                }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}
