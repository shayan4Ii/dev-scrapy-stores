import json
import logging
from typing import Any, Generator, Optional, Union

import scrapy
from scrapy.http import Response


class ChevronSpider(scrapy.Spider):
    """Spider for scraping Chevron gas station data."""

    name = "chevron"
    zipcode_file_path = "data/tacobell_zipcode_data.json"
    start_urls = ["https://www.chevronwithtechron.com/en_us/home/gas-station-near-me.html"]
    API_FORMAT_URL = "https://apis.chevron.com/api/StationFinder/nearby?clientid={client_id}&lat={latitude}&lng={longitude}&oLat={latitude}&oLng={longitude}&brand=chevronTexaco&radius=35"
    CLIENT_ID_XPATH = '//div[@class="cwtFindAStation__section"]/@data-clientid'

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the spider."""
        super().__init__(*args, **kwargs)
        self.processed_store_ids: set[str] = set()

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the initial response and generate API requests."""
        client_id = self._get_client_id(response)
        if not client_id:
            self.logger.error("Failed to extract client ID")
            return

        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            api_url = self.API_FORMAT_URL.format(
                client_id=client_id,
                latitude=zipcode["latitude"],
                longitude=zipcode["longitude"]
            )
            yield scrapy.Request(api_url, callback=self.parse_stores)

    def _load_zipcode_data(self) -> list[dict[str, Union[str, float]]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s", self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []

    def _get_client_id(self, response: Response) -> Optional[str]:
        """Extract the client ID from the response."""
        return response.xpath(self.CLIENT_ID_XPATH).get()

    def parse_stores(self, response: Response) -> Generator[dict[str, Any], None, None]:
        """Parse store data from the API response."""
        try:
            stores = response.json().get("stations", [])
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in API response")
            return

        for store in stores:
            store_id = store.get("id")
            if not store_id:
                self.logger.warning("Store missing ID, skipping")
                continue

            if store_id in self.processed_store_ids:
                self.logger.info("Skipping duplicate store with ID: %s", store_id)
                continue

            self.processed_store_ids.add(store_id)
            parsed_store = self._parse_store(store)
            if parsed_store:
                yield parsed_store

    def _parse_store(self, store: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Parse individual store data."""
        try:
            address = self._get_address(store)
            location = self._get_location(store)
            if not (address and location):
                self.logger.warning("Store missing required fields, discarding: %s", store.get("id"))
                return None

            return {
                "number": store.get("id"),
                "name": store.get("name"),
                "phone_number": store.get("phone"),
                "address": address,
                "location": location,
                "url": "https://www.chevronwithtechron.com/en_us/home/gas-station-near-me.html",
                "raw": store,
            }
        except Exception as e:
            self.logger.error("Error parsing store data: %s", e, exc_info=True)
            return None

    def _get_address(self, store_info: dict[str, Any]) -> str:
        """Format store address."""
        try:
            street = store_info.get("address", "").strip()
            city = store_info.get("city", "").strip()
            state = store_info.get("state", "").strip()
            zipcode = store_info.get("zip", "").strip()

            city_state_zip = ", ".join(filter(None, [city, f"{state} {zipcode}".strip()]))
            full_address = ", ".join(filter(None, [street, city_state_zip]))

            if not full_address:
                self.logger.warning("Missing address information: %s", store_info.get("id"))
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, store_info: dict[str, Any]) -> dict[str, Any]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('lat')
            longitude = store_info.get('lng')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning("Missing latitude or longitude for store: %s", store_info.get("id"))
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

    def _get_services(self, store_info: dict[str, Any]) -> list[str]:
        """Extract and format services."""
        try:
            return [key for key, value in store_info.items() if value == "1"]
        except Exception as e:
            self.logger.error("Error extracting services: %s", e, exc_info=True)
            return []