from typing import Optional, Generator

import scrapy


class VolkswagenusaSpider(scrapy.Spider):
    """Spider for scraping Volkswagen USA dealer information."""

    name = "volkswagenusa"
    allowed_domains = ["prod-cached-ds.dcc.feature-app.io"]
    start_urls = [
        "https://prod-cached-ds.dcc.feature-app.io/v2-4-1/bff-search/dealers?serviceConfigEndpoint=%7B%22endpoint%22%3A+%7B%22type%22%3A+%22publish%22%2C+%22country%22%3A+%22us%22%2C+%22language%22%3A+%22en%22%2C+%22content%22%3A+%22onehub_pkw%22%2C+%22envName%22%3A+%22prod%22%2C+%22testScenarioId%22%3A+null%7D%7D&query=%7B%22type%22%3A+%22DEALER%22%2C+%22language%22%3A+%22en-US%22%2C+%22countryCode%22%3A+%22US%22%2C+%22dealerServiceFilter%22%3A+%5B%5D%2C+%22contentDealerServiceFilter%22%3A+%5B%5D%2C+%22dealerId%22%3A+%22401015%22%2C+%22dealerBrand%22%3A+%22V%22%2C+%22name%22%3A+%22+%22%2C+%22usePrimaryTenant%22%3A+true%7D"
    ]

    def parse(self, response: scrapy.http.Response) -> Generator[dict, None, None]:
        """Parse the response and yield store items."""
        try:
            stores = response.json()['dealers']
            for store in stores:
                item = self._parse_store(store)
                if item:
                    yield item
        except Exception as e:
            self.logger.error("Error parsing response: %s", e, exc_info=True)

    def _parse_store(self, store: dict) -> Optional[dict]:
        """Parse individual store data and return a store item."""
        try:
            required_fields = ['address', 'location', 'url', 'raw']
            item = {
                "number": store.get("id"),
                "name": store.get("name"),
                "phone_number": store.get("contact", {}).get("phoneNumber"),
                "address": self._get_address(store.get("address", {})),
                "location": self._get_location(store),
                "hours": self._get_hours(store),
                "url": "https://www.vw.com/en/dealer-search.html?---=%7B%22dealer-search_featureappsection%22%3A%22%2F%22%7D",
                "raw": store
            }

            # Check for missing required fields
            if not all(item.get(field) for field in required_fields):
                missing_fields = [field for field in required_fields if not item.get(field)]
                self.logger.warning("Store missing required fields: %s. Store data: %s", missing_fields, store)
                return None

            return item
        except Exception as e:
            self.logger.error("Error parsing store: %s", e, exc_info=True)
            return None

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("street", "").strip(),
                store_info.get("streetSupplementary", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("province", "")
            zipcode = store_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information for store: %s", store_info)
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            coordinates = loc_info.get("coordinates", [])
            if len(coordinates) == 2:
                latitude, longitude = coordinates
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning("Missing or invalid coordinates for store: %s", loc_info)
            return {}
        except Exception as e:
            self.logger.error("Error extracting location: %s", e, exc_info=True)
            return {}

    def _get_hours(self, store_info: dict) -> dict:
        """Extract and parse store hours."""
        try:
            hours = {}
            hours_list = store_info.get("businessHours", [])

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("id"))
                return {}

            for raw_hours_dict in hours_list:
                hours_day = raw_hours_dict["label"].lower()
                hour_range = raw_hours_dict["displayTimes"]

                if len(hour_range) != 1:
                    self.logger.warning("Multiple hours info found for %s: %s", hours_day, hour_range)
                    continue

                hours_info = hour_range[0]
                open_time = hours_info.get("from", "").lower()
                close_time = hours_info.get("till", "").lower()

                hours[hours_day] = {
                    "open": open_time,
                    "close": close_time
                }

            return hours
        except Exception as e:
            self.logger.error("Error getting store hours: %s", e, exc_info=True)
            return {}