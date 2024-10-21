import json
import scrapy
from datetime import datetime, timedelta


class ZaxbysSpider(scrapy.Spider):
    name = "zaxbys"
    allowed_domains = ["zapi.zaxbys.com"]

    zipcode_file_path = "data/tacobell_zipcode_data.json"
    STORES_API_URL = "https://zapi.zaxbys.com/v1/stores/near?latitude={latitude}&longitude={longitude}"
    HOURS_API_URL = "https://zapi.zaxbys.com/v1/stores/{store_id}/calendars?from={from_date}&to={to_date}"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.processed_dealer_numbers = set()

    def start_requests(self):
        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            # hardcode specific latitude and longitude for testing
            zipcode["latitude"] = 33.9519347
            zipcode["longitude"] = -83.357567

            url = self.STORES_API_URL.format(
                latitude=zipcode["latitude"], longitude=zipcode["longitude"])
            yield scrapy.Request(url, callback=self.parse)
            break

    def _load_zipcode_data(self):
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
        stores = response.json()

        for store in stores:
            dealer_number = store.get("storeId")
            if dealer_number not in self.processed_dealer_numbers:
                self.processed_dealer_numbers.add(dealer_number)
                yield scrapy.Request(
                    url=self._get_hours_url(dealer_number),
                    callback=self._parse_store_with_hours,
                    meta={'store': store}
                )
            else:
                self.logger.debug(f"Duplicate store found: {dealer_number}")

    def _get_hours_url(self, store_id):
        today = datetime.now().date()
        end_date = today + timedelta(days=6)
        return self.HOURS_API_URL.format(
            store_id=store_id,
            from_date=today.strftime('%Y-%m-%d'),
            to_date=end_date.strftime('%Y-%m-%d')
        )

    def _parse_store_with_hours(self, response):
        store = response.meta['store']
        hours_data = response.json()
        store_data = self._parse_store(store, hours_data)
        yield store_data

    def _parse_store(self, store, hours_data):
        """Parse individual store data."""
        store['hours_data'] = hours_data
        return {
            "number": store.get("storeId"),
            "name": store.get("storeName"),
            "phone_number": store.get("phone"),
            "address": self._get_address(store),
            "location": self._get_location(store),
            'hours': self._get_hours(hours_data),
            "url": self._get_url(store),
            "raw": store
        }

    def _get_hours(self, hours_data):
        """Parse store hours from the API response."""
        parsed_hours = {
            'monday': {'open': None, 'close': None},
            'tuesday': {'open': None, 'close': None},
            'wednesday': {'open': None, 'close': None},
            'thursday': {'open': None, 'close': None},
            'friday': {'open': None, 'close': None},
            'saturday': {'open': None, 'close': None},
            'sunday': {'open': None, 'close': None}
        }

        day_mapping = {
            'Mon': 'monday', 'Tue': 'tuesday', 'Wed': 'wednesday',
            'Thu': 'thursday', 'Fri': 'friday', 'Sat': 'saturday', 'Sun': 'sunday'
        }

        for day_data in hours_data:
            if day_data['type'] == 'business':
                for range_data in day_data['ranges']:
                    day_of_week = day_mapping[range_data['day']]

                    open_time = datetime.strptime(range_data['opensAt'].split(
                    )[1], '%H:%M').strftime('%I:%M %p').lower().lstrip('0')
                    close_time = datetime.strptime(range_data['closeAt'].split()[
                                                   1], '%H:%M').strftime('%I:%M %p').lower().lstrip('0')

                    parsed_hours[day_of_week] = {
                        "open": open_time,
                        "close": close_time
                    }

        return parsed_hours

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address", "").strip(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")

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
            latitude = loc_info.get("latitude")
            longitude = loc_info.get("longitude")

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

    def _get_url(self, store_info: dict) -> str:
        """Get store URL."""
        state = store_info.get("state").lower()
        city = store_info.get("city").lower()
        slug = store_info.get("slug").lower()
        return f"https://www.zaxbys.com/locations/{state}/{city}/{slug}"