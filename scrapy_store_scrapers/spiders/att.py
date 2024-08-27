from datetime import datetime
import json
import re
from typing import Generator

import scrapy
from scrapy.http import Response


class AttSpider(scrapy.Spider):
    name = "att"
    allowed_domains = ["att.com"]
    start_urls = ["https://www.att.com/stores/"]

    store_client_keys = []

    APP_KEY_RE = re.compile(r"^\s*appkey:\s*'([^']+)'", flags=re.MULTILINE)

    def parse(self, response: Response) -> Generator[scrapy.Request, None, None]:
        app_key_search = self.APP_KEY_RE.search(response.text)

        if app_key_search:
            app_key = app_key_search.group(1)
        else:
            self.logger.error("Failed to find app key")
            return

        with open(r"data\tacobell_zipcode_data.json") as f:
            zipcodes = json.load(f)

        for zipcode in zipcodes[:5]:
            headers = self.get_headers()
            payload = self.get_payload(zipcode["zipcode"], zipcode["latitude"], zipcode["longitude"], app_key)
            yield scrapy.Request(
                url="https://www.att.com/stores/rest/locatorsearch",
                method="POST",
                headers=headers,
                body=json.dumps(payload),
                callback=self.parse_stores
            )
            

    def parse_stores(self, response: Response) -> Generator[dict, None, None]:
        stores = response.json()
        for store in stores["response"]["collection"]:

            if store["clientkey"] in self.store_client_keys:
                continue
            
            self.store_client_keys.append(store["clientkey"])

            store_info = {}

            store_info["name"] = self.get_name(store)
            store_info["phone"] = self.get_phone(store)
            store_info["address"] = self.get_address(store)
            store_info["location"] = self.get_location(store)
            store_info["hours"] = self.get_hours(store)

            yield store_info

    def get_name(self, store: dict) -> str:
        return store["name"]
    
    def get_phone(self, store: dict) -> str:
        return store["phone"]
    
    def get_address(self, store: dict) -> str:
        addr1 = self.get_none_as_empty_string(store["address1"])
        addr2 = self.get_none_as_empty_string(store["address2"])
        city = self.get_none_as_empty_string(store["city"])
        state = self.get_none_as_empty_string(store["state"])
        zipcode = self.get_none_as_empty_string(store["postalcode"])

        street_address = f"{addr1}, {addr2}".strip(", ")
        formatted_address = f"{street_address}, {city}, {state} {zipcode}"
        return formatted_address

    def get_none_as_empty_string(self, value: str) -> str:
        return value if value is not None else ""

    def get_location(self, store: dict) -> float:
        """
        Extract the location (longitude and latitude) from the raw store information
        and return it in GeoJSON Point format.
        """
        try:
            
            latitude = store.get('latitude')
            longitude = store.get('longitude')
            
            # Convert latitude and longitude to float if they exist
            if latitude is not None and longitude is not None:
                try:
                    longitude = float(longitude)
                    latitude = float(latitude)
                    return {
                        "type": "Point",
                        "coordinates": [longitude, latitude]  # GeoJSON uses [longitude, latitude] order
                    }
                except ValueError:
                    self.logger.warning(f"Invalid latitude or longitude values: {latitude}, {longitude}")
                    return None
            else:
                self.logger.warning("Missing latitude or longitude")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting location: {str(e)}")
            return None

    def get_hours(self, store: dict) -> str:

        hours_info = {}

        hours = store.get("bho")
        if hours:
            hours_list = json.loads(hours)
            
            days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]

            if len(hours_list) != 7:
                self.logger.error(f"Invalid hours list: {hours_list}")
                return None

            for n, hours_list in enumerate(hours_list):
                day = days[n]
                hours_info[day] = {
                    "open": self.convert_to_12h_format(hours_list[0]),
                    "close": self.convert_to_12h_format(hours_list[1])
                }
            return hours_info
        return None

    @staticmethod
    def convert_to_12h_format(time_str: str) -> str:
        if not time_str:
            return time_str
        elif time_str == "9999":
            return None
        time_obj = datetime.strptime(time_str, '%H%M').time()
        return time_obj.strftime('%I:%M %p').lower()            

    @staticmethod
    def get_headers() -> dict:
        return {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "x-requested-with": "XMLHttpRequest"
        }

    @staticmethod
    def get_payload(zipcode: str, latitude: float, longitude: float, app_key: str) -> dict:
        return {
        "request": {
            "appkey": app_key,
            "formdata": {
                "geoip": False,
                "dataview": "store_default",
                "google_autocomplete": "true",
                "limit": 15,
                "geolocs": {
                    "geoloc": [{
                        "addressline": zipcode,
                        "country": "US",
                        "latitude": latitude,
                        "longitude": longitude,
                        "state": "",
                        "province": "",
                        "city": "",
                        "address1": "",
                        "postalcode": zipcode
                    }]
                },
                "searchradius": "40|50",
                "where": {
                    "opening_status": {"distinctfrom": "permanently_closed"},
                    "virtualstore": {"distinctfrom": "1"},
                    "and": {
                        "company_owned_stores": {"eq": ""},
                        "cash_payments_accepted": {"eq": ""},
                        "offers_spanish_support": {"eq": ""},
                        "pay_station_inside": {"eq": ""},
                        "small_business_solutions": {"eq": ""}
                    }
                },
                "false": "0"
            }
        }
    }
