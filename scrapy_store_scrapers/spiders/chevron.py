from typing import Union
import json

import scrapy


class ChevronSpider(scrapy.Spider):
    name = "chevron"

    zipcode_file_path = r"data\tacobell_zipcode_data.json"

    start_urls = ["https://www.chevronwithtechron.com/en_us/home/gas-station-near-me.html"]

    API_FORMAT_URL = "https://apis.chevron.com/api/StationFinder/nearby?clientid={client_id}&lat={latitude}&lng={longitude}&oLat={latitude}&oLng={longitude}&brand=chevronTexaco&radius=35"
    
    CLIENT_ID_XPATH = '//div[@class="cwtFindAStation__section"]/@data-clientid'

    def parse(self, response):

        client_id = self._get_client_id(response)
        zipcodes = self._load_zipcode_data()

        for zipcode in zipcodes:
            api_url = self.API_FORMAT_URL.format(client_id=client_id, latitude=zipcode["latitude"], longitude=zipcode["longitude"])
            yield scrapy.Request(api_url, callback=self.parse_stores)

    def _load_zipcode_data(self) -> list[dict[str, Union[str, float]]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path) as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found")
            return []
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file")
            return []

    def _get_client_id(self, response):
        return response.xpath(self.CLIENT_ID_XPATH).get()
    
    def parse_stores(self, response):
        
        stores = response.json()

        for store in stores["stations"]:
            parsed_store = {}

            parsed_store["number"] = store["id"]
            parsed_store["name"] = store["name"]
            parsed_store["phone_number"] = store["phone"]
            parsed_store["address"] = self._get_address(store)
            parsed_store["location"] = self._get_location(store)
            parsed_store["url"] = f"https://www.chevronwithtechron.com/en_us/home/gas-station-near-me.html"
            parsed_store["raw"] = store

            yield parsed_store

    def _get_address(self, store_info) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address", ""),
                # store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information: {store_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info):
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

    def _get_services(self, store_info):
        """Extract and format services."""
        try:
            services = []
            for key, value in store_info.items():
                if value == "1":
                    services.append(key)
            return services
        except Exception as e:
            self.logger.error(f"Error extracting services: {e}", exc_info=True)
            return []