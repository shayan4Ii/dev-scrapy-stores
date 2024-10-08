import re
import json
import scrapy

from typing import Union


class HondausaSpider(scrapy.Spider):
    name = "hondausa"
    allowed_domains = ["automobiles.honda.com"]
    zipcode_file_path = "data/tacobell_zipcode_data.json"

    zipcode_api_format_url = "https://automobiles.honda.com/platform/api/v2/dealer?productDivisionCode=A&excludeServiceCenters=true&zip={zipcode}&maxResults=16"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers = set()

    def start_requests(self):
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            url = self.zipcode_api_format_url.format(
                zipcode=zipcode["zipcode"])
            yield scrapy.Request(url, callback=self.parse)

    def _load_zipcode_data(self) -> list[dict[str, Union[str, float]]]:
        """Load zipcode data from a JSON file."""
        try:
            with open(self.zipcode_file_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found: %s",
                              self.zipcode_file_path)
        except json.JSONDecodeError:
            self.logger.error(
                "Invalid JSON in zipcode data file: %s", self.zipcode_file_path)
        return []

    def parse(self, response):
        data = response.json()
        stores = data["Dealers"]

        for store in stores:
            dealer_number = store.get("DealerNumber")
            if dealer_number not in self.processed_dealer_numbers:
                self.processed_dealer_numbers.add(dealer_number)
                yield self._parse_store(store)
            else:
                self.logger.debug(f"Duplicate store found: {dealer_number}")

    def _parse_store(self, store: dict[str, Union[str, float]]) -> dict[str, Union[str, float]]:
        """Parse individual store data."""
        return {
            "number": store.get("DealerNumber"),
            "name": store.get("Name"),
            "phone_number": store.get("Phone"),
            "address": self._get_address(store),
            "location": self._get_location(store),
            "hours": self._get_hours(store),
            "services": self._get_services(store),
            "url": "https://automobiles.honda.com/tools/dealership-locator",
            "raw": store,
        }

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("Address", "").strip(),
                # store_info.get("line2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("City", "")
            state = store_info.get("State", "")
            zipcode = store_info.get("ZipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(
                    "Missing address information for store: %s", store_info)
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("Latitude")
            longitude = loc_info.get("Longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(
                "Missing latitude or longitude for store: %s", loc_info)
            return {}
        except ValueError as error:
            self.logger.warning(
                "Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s",
                              error, exc_info=True)
        return {}

    def _get_services(self, store_info: dict) -> list:
        """Extract and parse services from custom fields."""
        services_map = {
            'HC': 'Council of Excellence',
            'GM': 'Battery Electric Vehicle Authorized Dealer',
            '09': 'Express Service',
            'FC': 'Fuel Cell Electric Vehicle Dealer',
            'GA': 'Honda Environmental Leadership Award',
            'GG': 'Honda Environmental Leadership Award - Gold Level',
            'GP': 'Honda Environmental Leadership Award',
            'GS': 'Honda Environmental Leadership Award',
            'CM': 'Honda Service Pass',
            'MC': 'Masters Circle',
            'MO': 'Motocompacto Authorized Dealer',
            '02': "President's Award",
            'PE': "President's Award Elite"
        }
        service_codes = [service['Code'] for service in store_info.get("Attributes", [])]
        available_services = [services_map.get(
            service) for service in service_codes if service in services_map]

        unknown_services = set(service_codes) - set(services_map.keys())
        if unknown_services:
            self.logger.debug(
                "Unknown service types found: %s", ", ".join(unknown_services))

        return available_services

    def _get_hours(self, store_info: dict):
        """Extract and parse store hours."""
        try:
            hours = {}
            hours_list = store_info.get("SalesHours", {})

            if not hours_list:
                self.logger.warning("No hours found for store: %s", store_info.get("location_id"))
                return {}

            for raw_hours_dict in hours_list:
                hour_days = raw_hours_dict["Days"].lower()
                hours_text = raw_hours_dict["Hours"].lower()
                

                if '-' in hour_days:
                    start_day, end_day = hour_days.split('-')
                    hour_days = self._get_days(start_day, end_day)
                else:
                    hour_days = self._get_days(hour_days, hour_days)

                if '-' in hours_text:
                    open_time, close_time = hours_text.split('-')

                    open_time = self.format_time(open_time)
                    close_time = self.format_time(close_time)
                elif hours_text == "closed":
                    open_time = close_time = None
                else:
                    self.logger.warning("Invalid hours format: %s", hours_text)
                    open_time = close_time = None

                for day in hour_days:
                    hours[day] = {
                        "open": open_time,
                        "close": close_time
                    }


            return hours
        except Exception as e:
            self.logger.error("Error getting store hours: %s", e, exc_info=True)
            return {}
        
    @staticmethod
    def format_time(time_str: str) -> str:
        """Add a space before 'am' or 'pm' if not present."""
        return re.sub(r'(\d+)([ap]m)', r'\1 \2', time_str)

    @staticmethod
    def _get_days(start_day: str, end_day: str):
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
