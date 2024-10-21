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
        store_data = self._parse_store(store)
        store_data['hours'] = hours_data
        # store_data['hours'] = self._get_hours(hours_data)
        yield store_data

    def _parse_store(self, store):
        """Parse individual store data."""
        return {
            "number": store.get("storeId"),
            "name": store.get("storeName"),
            "phone_number": store.get("phone"),
            "address": self._get_address(store),
            "location": self._get_location(store),
            "url": self._get_url(store),
            "raw": store
        }

    def _get_hours(self, hours_data):
        """Parse store hours from the API response."""
        parsed_hours = {}
        days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        
        for day_data in hours_data:
            date = day_data.get('date')
            day_of_week = datetime.strptime(date, '%Y-%m-%d').strftime('%A')
            
            open_time = day_data.get('open')
            close_time = day_data.get('close')
            
            if open_time and close_time:
                parsed_hours[day_of_week] = f"{open_time} - {close_time}"
            else:
                parsed_hours[day_of_week] = "Closed"
        
        # Ensure all days of the week are included, even if missing from the API response
        for day in days_of_week:
            if day not in parsed_hours:
                parsed_hours[day] = "Hours not available"
        
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