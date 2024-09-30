import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Generator

import scrapy
from scrapy.http import Response


class ShellSpider(scrapy.Spider):
    """Spider for scraping Shell gas station data."""

    name = "shell"
    allowed_domains = ["find.shell.com"]
    start_urls = ["https://find.shell.com/us/fuel/locations"]

    LOCATIONS_JSON_XPATH = '//div[@data-react-class="pages/GeographicDirectoryPage"]/@data-react-props'
    STORES_JSON_XPATH = '//div[@data-react-class="pages/StationListDirectoryPage"]/@data-react-props'
    STORE_JSON_XPATH = '//div[@data-react-class="pages/LocationPage"]/@data-react-props'

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the main page and yield requests for each state."""
        try:
            states_json = response.xpath(self.LOCATIONS_JSON_XPATH).get()
            states_data = json.loads(states_json)
            states = states_data["geographicListProps"]["locations"]

            for state in states:
                yield response.follow(state["link"], self.parse_state)
        except Exception as e:
            self.logger.error("Error parsing main page: %s", e, exc_info=True)

    def parse_state(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the state page and yield requests for each city."""
        try:
            cities_json = response.xpath(self.LOCATIONS_JSON_XPATH).get()
            cities_data = json.loads(cities_json)
            cities = cities_data["geographicListProps"]["locations"]

            for city in cities:
                yield response.follow(city["link"], self.parse_city)
        except Exception as e:
            self.logger.error("Error parsing state page: %s", e, exc_info=True)

    def parse_city(self, response: Response) -> Generator[scrapy.Request, None, None]:
        """Parse the city page and yield requests for each store."""
        try:
            stores_json = response.xpath(self.STORES_JSON_XPATH).get()
            stores_data = json.loads(stores_json)
            stores = stores_data["stationListProps"]["locations"]

            for store in stores:
                yield response.follow(store["link"], self.parse_store)
        except Exception as e:
            self.logger.error("Error parsing city page: %s", e, exc_info=True)

    def parse_store(self, response: Response) -> Generator[Dict[str, Any], None, None]:
        """Parse the store page and yield the store data."""
        try:
            store_json = response.xpath(self.STORE_JSON_XPATH).get()
            store_data = json.loads(store_json)

            services_name_map = store_data["config"]["intlData"]["messages"]["amenities"]
            fuel_name_map = store_data["config"]["intlData"]["messages"]["fuels"]
            raw_store_info = store_data["location"]

            parsed_store = {
                "number": raw_store_info["location_id"],
                "name": raw_store_info["name"],
                "address": self._get_address(store_data),
                "phone_number": raw_store_info["telephone"],
                "location": self._get_location(raw_store_info),
                "hours": self._get_hours(raw_store_info),
                "services": self._get_services(raw_store_info, services_name_map, fuel_name_map),
                "url": response.url,
                "raw": store_data
            }

            # Discard items missing required fields
            required_fields = ["address", "location", "url", "raw"]
            if all(parsed_store.get(field) for field in required_fields):
                yield parsed_store
            else:
                self.logger.warning("Discarding store due to missing required fields: %s", parsed_store["number"])
        except Exception as e:
            self.logger.error("Error parsing store page: %s", e, exc_info=True)

    def _get_address(self, store_data: dict) -> Optional[str]:
        """Extract and format the store address."""
        try:
            breadcrumbs = [b_info.get('text') for b_info in store_data.get("breadcrumbs", [])]
            formatted_address = store_data.get('location', {}).get("formatted_address")

            if len(breadcrumbs) != 5:
                self.logger.warning("Unable to extract address info from breadcrumbs: %s", store_data["location"]["location_id"])
                return None

            state, city, street = breadcrumbs[-3:]
            zipcode = self._extract_zipcode_from_address(formatted_address)

            city_state_zip = f"{city}, {state} {zipcode}".strip()
            full_address = ", ".join(filter(None, [street, city_state_zip]))

            if not full_address:
                self.logger.warning("Missing address for store: %s", store_data["location"]["location_id"])
                return None

            return full_address
        except Exception as e:
            self.logger.error("Error extracting address: %s", e, exc_info=True)
            return None

    def _extract_zipcode_from_address(self, address: str) -> str:
        """Extract zipcode from address string using regex."""
        try:
            zipcode = re.search(r'\d{5}', address)
            return zipcode.group() if zipcode else ""
        except Exception as e:
            self.logger.error("Error extracting zipcode from address: %s", e, exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> Optional[dict]:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('lat')
            longitude = store_info.get('lng')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning("Missing latitude or longitude for store: %s", store_info.get("location_id"))
            return None
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return None

    def _get_services(self, store_info: dict, services_name_map: dict, fuel_name_map:dict) -> List[str]:
        """Extract and format services."""
        try:
            services = [services_name_map[service] for service in store_info["amenities"]]
            fuels = [fuel_name_map[fuel] for fuel in store_info["fuels"]]
            services.extend(fuels)

            if store_info.get("ev_charging", {}).get("charging_points", 0) > 0:
                services.append("Electric Vehicle Charging")
            
            if store_info.get("hydrogen_offering"):
                services.append("Hydrogen Fueling")
            
            return services
        except Exception as e:
            self.logger.error("Error extracting services: %s", e, exc_info=True)
            return []

    def _get_hours(self, store_info: dict) -> Dict[str, Dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}
            hours_list = store_info.get("shop_opening_hours", {})

            if not hours_list:
                hours_list = store_info.get("forecourt_opening_hours", {})

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("location_id"))
                return {}

            for raw_hours_dict in hours_list:
                hour_days = raw_hours_dict["days"]

                if len(hour_days) == 1:
                    self._process_single_day_hours(hours, raw_hours_dict, store_info)
                elif len(hour_days) == 2:
                    self._process_multi_day_hours(hours, raw_hours_dict, store_info)
                else:
                    self.logger.warning("Invalid days found for store: %s", store_info.get("location_id"))
                    return {}

            return hours
        except Exception as e:
            self.logger.error("Error getting store hours: %s", e, exc_info=True)
            return {}

    def _process_single_day_hours(self, hours: dict, raw_hours_dict: dict, store_info: dict) -> None:
        """Process hours for a single day."""
        day_name = self._get_days(raw_hours_dict["days"][0], raw_hours_dict["days"][0])[0]
        open_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][0])
        close_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][1])

        if day_name in hours:
            self.logger.warning("Duplicate hours found for store: %s", store_info.get("location_id"))
        else:
            hours[day_name] = {"open": open_time, "close": close_time}

    def _process_multi_day_hours(self, hours: dict, raw_hours_dict: dict, store_info: dict) -> None:
        """Process hours for multiple days."""
        start_day, end_day = raw_hours_dict["days"]
        day_names = self._get_days(start_day, end_day)
        open_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][0])
        close_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][1])

        for day in day_names:
            if day in hours:
                self.logger.warning("Duplicate hours found for store: %s", store_info.get("location_id"))
            else:
                hours[day] = {"open": open_time, "close": close_time}

    @staticmethod
    def _get_days(start_day: str, end_day: str) -> List[str]:
        """Get list of days between start and end day."""
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        days_map = {
            "mon": "monday", "tue": "tuesday", "wed": "wednesday",
            "thu": "thursday", "fri": "friday", "sat": "saturday", "sun": "sunday"
        }

        start_index = days.index(days_map.get(start_day.lower()))
        end_index = days.index(days_map.get(end_day.lower()))

        if start_index > end_index:
            return days[start_index:] + days[:end_index + 1]
        return days[start_index:end_index + 1]

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