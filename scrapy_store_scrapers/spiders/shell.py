from datetime import datetime
import re
from typing import Any

import json
import scrapy


class ShellSpider(scrapy.Spider):
    name = "shell"
    allowed_domains = ["find.shell.com"]
    start_urls = ["https://find.shell.com/us/fuel/locations"]

    LOCATIONS_JSON_XPATH = '//div[@data-react-class="pages/GeographicDirectoryPage"]/@data-react-props'
    STORES_JSON_XPATH = '//div[@data-react-class="pages/StationListDirectoryPage"]/@data-react-props'
    STORE_JSON_XPATH = '//div[@data-react-class="pages/LocationPage"]/@data-react-props'

    def parse(self, response):
        states_json = response.xpath(self.LOCATIONS_JSON_XPATH).get()
        states_data = json.loads(states_json)

        states = states_data["geographicListProps"]["locations"]

        for state in states:
            yield response.follow(state["link"], self.parse_state)

    def parse_state(self, response):
        cities_json = response.xpath(self.LOCATIONS_JSON_XPATH).get()
        cities_data = json.loads(cities_json)

        cities = cities_data["geographicListProps"]["locations"]

        for city in cities:
            yield response.follow(city["link"], self.parse_city)            

    def parse_city(self, response):
        stores_json = response.xpath(self.STORES_JSON_XPATH).get()
        stores_data = json.loads(stores_json)

        stores = stores_data["stationListProps"]["locations"]

        for store in stores:
            yield response.follow(store["link"], self.parse_store)

    def parse_store(self, response):
        store_json = response.xpath(self.STORE_JSON_XPATH).get()
        store_data = json.loads(store_json)

        services_name_map = store_data["config"]["intlData"]["messages"]["amenities"]

        raw_store_info = store_data["location"]

        parsed_store = {}

        parsed_store["number"] = raw_store_info["location_id"]
        parsed_store["name"] = raw_store_info["name"]
        parsed_store["address"] = self._get_address(store_data)
        parsed_store["phone_number"] = raw_store_info["telephone"]
        parsed_store["location"] = self._get_location(raw_store_info)
        parsed_store["hours"] = self._get_hours(raw_store_info)
        parsed_store["services"] = self._get_services(raw_store_info, services_name_map)
        parsed_store["url"] = response.url
        parsed_store["raw"] = store_data

        yield parsed_store

    def _get_address(self, store_data: dict[str, Any]) -> dict[str, str]:
        breadcumbs = [b_info.get('text') for b_info in store_data.get("breadcrumbs", [])]
        formatted_address = store_data.get('location', {}).get("formatted_address")

        if len(breadcumbs) != 5:
            self.logger.warning(f"Unable to extract address info from breadcrumbs as there are more than 5 breadcrumbs: {store_data}")
            return ''
        
        state, city, street = breadcumbs[-3:]
        zipcode = self._extract_zipcode_from_address(formatted_address)

        city_state_zip = f"{city}, {state} {zipcode}".strip()

        full_address = ", ".join(filter(None, [street, city_state_zip]))
        if not full_address:
            self.logger.warning(f"Missing address for store: {store_data}")
        return full_address



    def _extract_zipcode_from_address(self, address: str) -> str:
        """Extract zipcode from address string using regex."""
        try:
            zipcode = re.search(r'\d{5}', address)
            return zipcode.group() if zipcode else ""            
        except Exception as e:
            self.logger.error("Error extracting zipcode from address: %s", e, exc_info=True)
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

    def _get_services(self, store_info, services_name_map) -> list[str]:
        """Extract and format services."""
        try:
            return [services_name_map[service] for service in store_info["amenities"]]
        except Exception as e:
            self.logger.error("Error extracting services: %s", e, exc_info=True)
            return []
        
    def _get_hours(self, store_info) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}

            hours_list = store_info.get("shop_opening_hours", {})

            if not hours_list:
                self.logger.warning(f"No hours found for store: {store_info}")
                return {}
            
            for raw_hours_dict in hours_list:
                hour_days = raw_hours_dict["days"]

                [
                    {'days': ['Mon', 'Sat'], 'hours': [['06:00', '21:00']]}, 
                    {'days': ['Sun'], 'hours': [['07:00', '21:00']]}
                    ]

                if len(hour_days) == 1:
                    day_name = self._get_days(hour_days[0], hour_days[0])[0]
                    open_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][0])
                    close_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][1])

                    if day_name in hours:
                        self.logger.warning(f"Duplicate hours found for store: {store_info}")
                        continue

                    hours[day_name] = {
                        "open": open_time,
                        "close": close_time
                    }
                elif len(hour_days) == 2:
                    start_day, end_day = hour_days
                    day_names = self._get_days(start_day, end_day)
                    open_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][0])
                    close_time = self._convert_to_12h_format(raw_hours_dict["hours"][0][1])

                    for day in day_names:
                        if day in hours:
                            self.logger.warning(f"Duplicate hours found for store: {store_info}")
                            continue
                        hours[day] = {
                            "open": open_time,
                            "close": close_time
                        }
                else:
                    self.logger.warning(f"Invalid days found for store: {store_info}")
                    return {}

            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    def _get_days(self, start_day, end_day):
        """Get list of days between start and end day."""
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

        days_map = {
            "mon": "monday",
            "tue": "tuesday",
            "wed": "wednesday",
            "thu": "thursday",
            "fri": "friday",
            "sat": "saturday",
            "sun": "sunday"
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