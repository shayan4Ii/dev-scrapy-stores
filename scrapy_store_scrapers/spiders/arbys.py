from datetime import datetime
import scrapy


class ArbysSpider(scrapy.Spider):
    name = "arbys"
    allowed_domains = ["api.arbys.com"]
    start_urls = ["https://api.arbys.com/arb/web-exp-api/v1/location/list/details?countryCode=US"]

    def parse(self, response):
        data = response.json()

        for location in data["locationsByCountry"]:
            if location["countryCode"] != "US":
                continue
            
            for state_dict in location['statesOrProvinces']:
                url = 'https://api.arbys.com/arb/web-exp-api/v1/location/list/details?countryCode=US&stateOrProvinceCode=' + state_dict['code'].upper()
                yield scrapy.Request(url, callback=self.parse_state_locations)
                break
    
    def parse_state_locations(self, response):
        data = response.json()

        for city_dict in data["stateOrProvince"]["cities"]:
            for loc_dict in city_dict["locations"]:
                store = self.parse_store(loc_dict)
                yield store
            break

    def parse_store(self, store):
        parsed_store = {}

        parsed_store["number"] = store["id"]
        parsed_store["name"] = store["displayName"]
        parsed_store["phone_number"] = store["contactDetails"]["phone"]

        parsed_store["address"] = self._get_address(store["contactDetails"]["address"])
        parsed_store["location"] = self._get_location(store["details"])
        parsed_store["hours"] = self._get_hours(store["hoursByDay"])
        parsed_store["services"] = store["services"]
        parsed_store["url"] = "https://www.arbys.com/locations/" + store["url"]
        parsed_store["raw"] = store

        for key, value in parsed_store.items():
            if value is None or (isinstance(value, (list, dict)) and not value):
                self.logger.warning(f"Missing or empty data for {key}")
            
        return parsed_store
    
    def _get_address(self, address_info: dict) -> str:
        """Format the store address from store information."""
        try:
            street = address_info.get("line", "").strip()
            city = address_info.get("cityName", "").strip()
            state = address_info.get("stateProvinceCode", "").strip()
            zipcode = address_info.get("postalCode", "").strip()

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}

    def _get_hours(self, raw_hours: dict) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:

            hours = {}

            day_abbr_map = {
                "Mon": "monday",
                "Tue": "tuesday",
                "Wed": "wednesday",
                "Thu": "thursday",
                "Fri": "friday",
                "Sat": "saturday",
                "Sun": "sunday"
            }

            for day, hours_info in raw_hours.items():
                open_time = hours_info.get("start", "")
                close_time = hours_info.get("end", "")

                day_name = day_abbr_map.get(day, "")

                if open_time and close_time:
                    hours[day_name] = {
                        "open": self._convert_to_12h_format(open_time),
                        "close": self._convert_to_12h_format(close_time)
                    }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    @staticmethod
    def _convert_to_12h_format(time_str: str) -> str:
        """Convert time to 12-hour format."""
        if not time_str:
            return time_str
        try:
            time_obj = datetime.strptime(time_str, '%H:%M').time()
            return time_obj.strftime('%I:%M %p').lower()
        except ValueError:
            return time_str

