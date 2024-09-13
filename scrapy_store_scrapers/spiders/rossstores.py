import json
import re
import scrapy


class RossstoresSpider(scrapy.Spider):
    name = "rossstores"
    start_urls = ["https://hosted.where2getit.com/rossdressforless/locator.html"]

    APP_KEY_RE = re.compile(r"^\s*appkey:\s*'([^']+)'", flags=re.MULTILINE)
    store_client_keys = []

    def parse(self, response) :
        """Parse the initial response and generate requests for store data."""
        app_key = self._extract_app_key(response.text)
        if not app_key:
            return

        zipcodes = self._load_zipcode_data()
        for zipcode in zipcodes:
            headers = self._get_headers()
            payload = self._get_payload(zipcode["zipcode"], zipcode["latitude"], zipcode["longitude"], app_key)
            yield scrapy.Request(
                url="https://hosted.where2getit.com/rossdressforless/rest/locatorsearch",
                method="POST",
                headers=headers,
                body=json.dumps(payload),
                callback=self.parse_stores
            )


    def parse_stores(self, response):
        """Parse store information from the response."""
        stores = response.json()

        for store in stores["response"]["collection"]:
            if store["clientkey"] in self.store_client_keys:
                continue
            
            self.store_client_keys.append(store["clientkey"])

            parsed_store = {}

            parsed_store["number"] = store["clientkey"]
            parsed_store["name"] = store["name"]
            parsed_store["phone_number"] = store["phone"]

            parsed_store["address"] = self._get_address(store)
            parsed_store["location"] = self._get_location(store)
            parsed_store["hours"] = self._get_hours(store)

            parsed_store["url"] = "https://www.rossstores.com/store-locator/"
            parsed_store["raw"] = store

            yield parsed_store

    def _get_hours(self, raw_store_data: dict) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}

            days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            for day in days:
                hours_text = raw_store_data.get(day, "")
                if not hours_text:
                    self.logger.warning(f"Missing hours for {day}: {raw_store_data}")
                    hours[day] = {
                        "open": None,
                        "close": None
                    }
                    continue

                open, close = hours_text.split("-")
                open = open.strip().lower()
                close = close.strip().lower()

                hours[day] = {
                    "open": open,
                    "close": close
                }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    def _get_address(self, store_info) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address1", ""),
                store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("postalcode", "")

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
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

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

    def _extract_app_key(self, text: str):
        """Extract the app key from the response text."""
        app_key_match = self.APP_KEY_RE.search(text)
        if app_key_match:
            return app_key_match.group(1)
        self.logger.error("Failed to find app key")
        return None

    def _load_zipcode_data(self):
        """Load zipcode data from a JSON file."""
        try:
            with open(r"data\tacobell_zipcode_data.json") as f:
                return json.load(f)
        except FileNotFoundError:
            self.logger.error("Zipcode data file not found")
            return []
        except json.JSONDecodeError:
            self.logger.error("Invalid JSON in zipcode data file")
            return []

    @staticmethod
    def _get_headers() -> dict[str, str]:
        """Get the headers for the request."""
        return {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "x-requested-with": "XMLHttpRequest"
        }

    @staticmethod
    def _get_payload(zipcode: str, latitude: float, longitude: float, app_key: str) -> dict:
        """Get the payload for the request."""
        return {
    "request": {
        "appkey": "097D3C64-7006-11E8-9405-6974C403F339",
        "formdata": {
            "geoip": False,
            "dataview": "store_default",
            "stateonly": 1,
            "limit": 20,
            "geolocs": {
                "geoloc": [
                    {
                        "addressline": "",
                        "country": "US",
                        "latitude": latitude,
                        "longitude": longitude,
                        "state": "",
                        "province": "",
                        "city": "",
                        "address1": "",
                        "postalcode": zipcode
                    }
                ]
            },
            "searchradius": "100|250",
            "where": {
                "clientkey": {"eq": ""},
                "opendate": {"eq": ""},
                "Shopping_Spree": {"eq": ""}
            },
            "false": "0"
        }
    }
}