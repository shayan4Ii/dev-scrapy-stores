from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *




class MisterCarwash(scrapy.Spider):
    name = "mistercarwash"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        USER_AGENT = None,
        CONCURRENT_REQUESTS = 4,
        DOWNLOAD_DELAY = 0.7
    )


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            yield scrapy.Request(
                url=f"https://mistercarwash.com/api/v1/locations/getbydistance?cLat={zipcode['latitude']}&cLng={zipcode['longitude']}&radius=100&cityName=&stateName=&allServices=true", 
                callback=self.parse,
                meta={"impersonate": "chrome"}
            )

    
    def parse(self, response: Response):
        stores = json.loads(response.text)['data']['body']
        if isinstance(stores, str):
            return
        for store in stores:
            yield {
                "number": store['storeNumber'],
                "name": store["name"],
                "phone_number": store["contacts"][0]['phoneNumber'],
                "address": self._get_address(store),
                "location": {
                    "type": "Point",
                    "coordinates": [
                        store["longitude"],
                        store["latitude"]
                    ]
                },
                "services": list(set([service['name'] for service in store['services'] if service['name']])),
                "hours": self._get_hours(store),
                "url": f"https://mistercarwash.com/store/{store['name'].replace(' ','-')}/",
                "raw": store
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store['address'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = store['city']
            state = store['state']
            zipcode = store['zipcode']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
    

    def _get_hours(self, store: Dict) -> dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            for hour_range in store['hours']:
                for day in days:
                    if hour_range['dayOfWeek'].lower() in day:
                        if hour_range.get("localStartTime") is None:
                            break
                        hours[day.lower()] = {
                            "open": convert_to_12h_format(hour_range.get('localStartTime').replace(":00", "", 1)),
                            "close": convert_to_12h_format(hour_range.get('localEndTime').replace(":00", "", 1))
                        }
                        break
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}