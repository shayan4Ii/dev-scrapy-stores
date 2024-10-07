import logging
from typing import Generator, Optional

import scrapy
from scrapy.http import Response


class MercedesBenzSpider(scrapy.Spider):
    """Spider for scraping Mercedes-Benz dealer information."""

    name = "mercedes-benz"
    allowed_domains = ["nafta-service.mbusa.com"]
    start_urls = ["https://nafta-service.mbusa.com/api/dlrsrv/v1/us/states?lang=en"]
    state_api_format_url = "https://nafta-service.mbusa.com/api/dlrsrv/v1/us/search?count=1000&filter=mbdealer&state={state}"

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the initial response and yield requests for each state."""
        try:
            states = response.json()
            for state in states:
                yield scrapy.Request(
                    url=self.state_api_format_url.format(state=state['code']),
                    callback=self.parse_stores,
                )
        except Exception as e:
            self.logger.error("Error parsing states: %s", e, exc_info=True)

    def parse_stores(self, response: Response) -> Generator[dict, None, None]:
        """Parse store data from the response."""
        try:
            data = response.json()
            stores = data.get("results", [])
            for store in stores:
                parsed_store = self._parse_store(store)
                if self._validate_store(parsed_store):
                    yield parsed_store
                else:
                    self.logger.warning("Discarded invalid store: %s", store.get("id"))
        except Exception as e:
            self.logger.error("Error parsing stores: %s", e, exc_info=True)

    def _parse_store(self, store: dict) -> dict:
        """Parse individual store data."""
        try:
            parsed_store = {
                "number": store.get("id"),
                "name": store.get("name"),
                "phone_number": self._get_phone(store),
                "services": self._get_services(store),
                "url": "https://www.mbusa.com/en/dealers",
            }

            store_addresses = store.get("address", [])
            if store_addresses:
                parsed_store["address"] = self._get_address(store_addresses[0])
                parsed_store["location"] = self._get_location(store_addresses[0].get("location", {}))

            parsed_store["raw"] = store
            return parsed_store
        except Exception as e:
            self.logger.error("Error parsing store %s: %s", store.get("id"), e, exc_info=True)
            return {}

    def _get_phone(self, store: dict) -> Optional[str]:
        """Extract phone number from store data."""
        try:
            contacts = store.get("contact", [])
            for contact in contacts:
                if contact.get("type") == "phone":
                    return contact.get("value")
            self.logger.warning("No phone number found for store: %s", store.get("id"))
            return None
        except Exception as e:
            self.logger.error("Error getting phone for store %s: %s", store.get("id"), e, exc_info=True)
            return None

    def _get_address(self, address_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("line1", ""),
                address_info.get("line2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("city", "")
            state = address_info.get("state", "")
            zipcode = address_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning("Missing address information for store: %s", address_info)
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("lat")
            longitude = loc_info.get("lng")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning("Missing latitude or longitude for store: %s", loc_info)
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

    def _get_services(self, store_info: dict) -> list:
        """Extract and parse services from custom fields."""
        services_map = {
            "all": "All Locations",
            "mbdealer": "Dealership",
            "amg": "AMG Performance Center",
            "amgelite": "AMG Performance Center Elite",
            "collisioncenter": "Collision Center",
            "elitecollisioncenter": "Elite Collision Center (Aluminum Welding)",
            "prmrexp": "Express Service by MB",
            "maybach": "Maybach Dealership",
            "service": "Service and Parts",
            "eqready": "EQ Certified",
            "zevst": "Zero Emission Vehicle State",
            "cpo": "Certified Pre-Owned",
            "ccdropoffonly": "Collision Center Drop-off Only",
            "mobileservice": "Mobile Service by Mercedes-Benz",
            "evcollisioncenter": "Electric Vehicle Collision Center"
        }
        services = store_info.get("badges", [])
        available_services = [services_map.get(service) for service in services if service in services_map]
        
        unknown_services = set(services) - set(services_map.keys())
        if unknown_services:
            self.logger.warning("Unknown service types found: %s", ", ".join(unknown_services))
        
        return available_services

    def _validate_store(self, store: dict) -> bool:
        """Validate if the store has all required fields."""
        required_fields = ["address", "location", "url", "raw"]
        return all(store.get(field) for field in required_fields)
