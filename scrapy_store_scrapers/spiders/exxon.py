from datetime import datetime
import json
from typing import Any, Dict, Optional
import scrapy


class ExxonSpider(scrapy.Spider):
    name = "exxon"
    allowed_domains = ["www.exxon.com"]
    start_urls = ["https://www.exxon.com/en/find-station/alabama"]

    LINKS_XPATH = '//div[@id="content"]//ul/li/a/@href'

    STORE_JSON_XPATH = '//script[@type="application/ld+json" and contains(text(), "LocalBusiness")]/text()'
    SERVICES_XPATH = '//ul[contains(@class, "station-details-featuredItem")]/li/text()'
    
    def parse(self, response):

        if response.xpath(self.STORE_JSON_XPATH):
            yield self._parse_store(response)

        all_links = response.xpath(self.LINKS_XPATH).getall()
        for link in all_links:
            yield response.follow(link, callback=self.parse)
            break

    def _parse_store(self, response):
        store_json = response.xpath(self.STORE_JSON_XPATH).get()
        store_data = json.loads(store_json)

        parsed_store = {}

        parsed_store["name"] = store_data["name"]
        parsed_store["phone_number"] = store_data["telephone"]

        parsed_store["address"] = self._get_address(store_data["address"])
        parsed_store["location"] = self._get_location(store_data["geo"])
        parsed_store["hours"] = self._get_hours(store_data)
        parsed_store["services"] = response.xpath(self.SERVICES_XPATH).getall()

        parsed_store["url"] = response.url
        parsed_store["raw"] = store_data

        return parsed_store

    def _get_address(self, address_info: Dict[str, Any]) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("streetAddress", ""),
                # address_info.get("address_2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("addressLocality", "")
            state = address_info.get("addressCountry", "")
            zipcode = address_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(
                    f"Missing address information: {address_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, location_info: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = location_info.get('latitude')
            longitude = location_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(
                f"Missing latitude or longitude for store: {location_info}")
            return {}
        except ValueError as error:
            self.logger.warning(
                f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(
                f"Error extracting location: {error}", exc_info=True)
        return {}

    def _get_hours(self, raw_store_data: Dict[str, Any]) -> Dict[str, Dict[str, Optional[str]]]:
        """Extract and parse store hours."""
        try:
            hours_raw = raw_store_data.get("openingHoursSpecification", {})
            if not hours_raw:
                self.logger.warning(
                    f"No hours found for store {raw_store_data}")
                return {}

            hours = {}

            for day in hours_raw["@dayOfWeek"]:
                day = day.lower()

                open_time = self._convert_to_12h_format(hours_raw["@opens"])
                close_time = self._convert_to_12h_format(hours_raw["@closes"])

                hours[day] = {"open": open_time, "close": close_time}

            if not hours:
                self.logger.warning(
                    f"No hours found for store {raw_store_data}")

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
