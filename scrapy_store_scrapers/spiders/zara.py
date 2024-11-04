import scrapy
from scrapy_store_scrapers.utils import *



class Zara(scrapy.Spider):
    name = "zara"
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "USER_AGENT": None,
        "CONCURRENT_REQUESTS": 8,
    }


    def start_requests(self) -> Iterable[scrapy.Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://www.zara.com/us/en/stores-locator/extended/search?lat={zipcode['latitude']}&lng={zipcode['longitude']}&isDonationOnly=false&skipRestrictions=true&ajax=true"
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response) -> Iterable[Dict]:
        stores = json.loads(response.text)
        for store in stores:
            yield {
                "number":f"{store['id']}",
                "name": store.get('name'),
                "address": self._get_address(store),
                "phone_number": store.get("phones")[0] if store.get("phones") else None,
                "location": {
                    "type": "Point",
                    "coordinates": [store['longitude'], store['latitude']]
                },
                "hours": self._get_hours(store),
                "url": store['url'],
                "raw": store
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store['addressLines'][0],
            ]
            street = ", ".join(filter(None, address_parts))

            city = store.get("city", "")
            state = store.get("stateCode", "")
            zipcode = store.get("zipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, store: Dict) -> Dict:
        hours = {}
        days = {
            1: "monday",
            2: "tuesday",
            3: "wednesday",
            4: "thursday",
            5: "friday",
            6: "saturday",
            7: "sunday"
        }
        try:
            for idx, day in days.items():
                for hour in store['openingHours']:
                    if hour['weekDay'] == idx:
                        hours[day] = {
                            "open": convert_to_12h_format(hour['openingHoursInterval'][0]['openTime']),
                            "close": convert_to_12h_format(hour['openingHoursInterval'][0]['closeTime'])
                        }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
        


