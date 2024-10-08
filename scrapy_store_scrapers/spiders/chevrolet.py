import json
import re
from typing import Generator, Union

import scrapy


class ChevroletSpider(scrapy.Spider):
    """Spider to scrape Chevrolet dealership data."""
    name = "chevrolet"
    allowed_domains = ["www.chevrolet.com"]
    API_FORMAT_URL = (
        "https://www.chevrolet.com/bypass/pcf/quantum-dealer-locator/v1/getDealers?"
        "desiredCount=25&distance=500&makeCodes=001&serviceCodes=&latitude={latitude}"
        "&longitude={longitude}&searchType=latLongSearch"
    )
    zipcode_file_path = "data/tacobell_zipcode_data.json"

    custom_settings = {
        'DEFAULT_REQUEST_HEADERS': {
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Clientapplicationid": "quantum",
            "Content-Type": "application/json; charset=utf-8",
            "Locale": "en-US",
            "Referer": "https://www.chevrolet.com/dealer-locator",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36"
            ),
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers = set()

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        """Generate initial requests based on zipcode data."""
        zipcodes = self._load_zipcode_data()
        self.logger.info("Loaded %d zipcodes.", len(zipcodes))
        for zipcode in zipcodes:
            url = self.API_FORMAT_URL.format(
                latitude=zipcode["latitude"],
                longitude=zipcode["longitude"]
            )
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response: scrapy.http.Response) -> Generator[dict, None, None]:
        """Parse the API response and yield store items."""
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            self.logger.error("Error parsing JSON response: %s", e, exc_info=True)
            return

        stores = data.get("payload", {}).get("dealers", [])
        if not stores:
            self.logger.warning("No dealers found in response.")

        for store in stores:
            dealer_number = store.get("bac")
            if dealer_number not in self.processed_dealer_numbers:
                self.processed_dealer_numbers.add(dealer_number)
                item = self._parse_store(store)
                if item:
                    yield item
            else:
                self.logger.debug(f"Duplicate store found: {dealer_number}")


    def _parse_store(self, store: dict) -> Union[dict, None]:
        """Parse individual store data."""
        sales_department = self._get_department_by_name("Sales", store)
        address = self._get_address(store.get("address", {}))
        location = self._get_location(store.get("geolocation", {}))
        url = "https://www.chevrolet.com/dealer-locator"
        raw = store
        phone_number = sales_department.get("phoneNumber") or store.get("generalContact", {}).get('phone1')
        required_fields = [address, location, url, raw]
        if not all(required_fields):
            self.logger.warning(
                "Missing required fields for store: %s",
                store.get("dealerName", "Unknown")
            )
            return None

        item = {
            "number": store.get("bac"),
            "name": store.get("dealerName"),
            "phone_number": phone_number,
            "address": address,
            "location": location,
            "services": self._get_services(store),
            "hours": self._get_hours(sales_department),
            "url": url,
            "raw": raw,
        }
        return item

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

    def _get_department_by_name(self, name: str, store: dict) -> dict:
        """Retrieve department information by name."""
        for department in store.get("departments", []):
            if department.get("name") == name:
                return department
        self.logger.warning(
            "Department %s not found in store: %s",
            name,
            store.get("dealerName", "Unknown")
        )
        return {}

    def _get_address(self, address_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                address_info.get("addressLine1", ""),
                address_info.get("addressLine2", ""),
                address_info.get("addressLine3", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_info.get("cityName", "")
            state = address_info.get("countrySubdivisionCode", "")
            zipcode = address_info.get("postalCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(
                    "Missing address information for store: %s", address_info
                )
            return full_address
        except Exception as e:
            self.logger.error("Error formatting address: %s", e, exc_info=True)
            return ""

    def _get_location(self, loc_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = loc_info.get("latitude")
            longitude = loc_info.get("longitude")

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(
                "Missing latitude or longitude for location: %s", loc_info
            )
            return {}
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

    def _get_services(self, store: dict) -> list:
        """Extract and parse services from store data."""
        services_map = {
            "247": "Bolt EV/EUV Recall Certified",
            "019": "Business Elite",
            "139": "BuyPower Rewards Card",
            "189": "Certified Service Employee Discount Participating Dealer",
            "046": "Chevrolet Performance Parts",
            "113": "Collision/Body Shop",
            "182": "Corvette Certified Dealer",
            "246": "Courtesy Delivery Dealer",
            "187": "EV Sales & Service",
            "124": "Extended Protection Plan",
            "012": "GM Certified Pre-Owned",
            "240": "GM FleetTrac Program",
            "200": "Low Cab Forward Sales and Service",
            "282": "MobileService +",
            "202": "My Chevrolet Rewards Dealer",
            "047": "National Promotions – Participating Dealers",
            "237": "Shop. Click. Drive",
            "201": "Silverado 45/55/6500HD Sales & Service",
            "990": "Tire Sales & Service"
        }

        service_codes = [
            service.get('code', '') for service in store.get("services", [])
        ]
        available_services = [
            services_map.get(code) for code in service_codes if code in services_map
        ]

        unknown_services = set(service_codes) - set(services_map.keys())
        if unknown_services:
            self.logger.debug(
                "Unknown service types found: %s", ", ".join(unknown_services)
            )

        return available_services

    def _get_hours(self, department: dict) -> dict:
        """Extract and parse store hours."""
        try:
            hours = {}
            hours_list = department.get("departmentHours", [])

            day_map = {
                1: "monday",
                2: "tuesday",
                3: "wednesday",
                4: "thursday",
                5: "friday",
                6: "saturday",
                7: "sunday"
            }

            if not hours_list:
                self.logger.warning(
                    "No hours found for department: %s", department.get("name", "Unknown")
                )
                return {}

            for raw_hours_dict in hours_list:
                day_indexes = raw_hours_dict.get("dayOfWeek", [])
                for day_index in day_indexes:
                    day_name = day_map.get(day_index)
                    if not day_name:
                        self.logger.warning("Invalid day index: %s", day_index)
                        continue

                    open_time = self._format_time(raw_hours_dict.get("openFrom", ""))
                    close_time = self._format_time(raw_hours_dict.get("openTo", ""))

                    if not open_time or not close_time:
                        self.logger.warning(
                            "Invalid time for day %s in department %s",
                            day_name,
                            department.get("name", "Unknown")
                        )
                        continue

                    if day_name in hours:
                        self.logger.warning(
                            "Duplicate hours found for day %s in department %s",
                            day_name,
                            department.get("name", "Unknown")
                        )
                    else:
                        hours[day_name] = {"open": open_time, "close": close_time}

            return hours
        except Exception as e:
            self.logger.error("Error getting store hours: %s", e, exc_info=True)
            return {}

    @staticmethod
    def _format_time(time_str: str) -> str:
        """Format time to include minutes if missing and add a space before 'am' or 'pm'."""
        if not time_str:
            return ""
        # If there's no colon, add ':00' before 'am' or 'pm'
        if ':' not in time_str:
            time_str = re.sub(r'(\d+)([ap]m)', r'\1:00\2', time_str)
        # Add a space before 'am' or 'pm'
        time_str = re.sub(r'([ap]m)', r' \1', time_str)
        return time_str
