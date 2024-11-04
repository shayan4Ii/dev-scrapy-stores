import scrapy
from scrapy.http import Response
from scrapy_store_scrapers.utils import *



class FirestoneCompleteAutoCare(scrapy.Spider):
    name = "firestonecompleteautocare"


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://www.firestonecompleteautocare.com/bsro/services/store/location/get-list-by-zip?zipCode={zipcode['zipcode']}"
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response) -> Iterable[Dict]:
        obj = json.loads(response.text)
        if obj.get("success"):  
            stores = obj['data']['stores']
            for store in stores:
                yield {
                    "number": store.get("storeNumber"),
                    "name": store.get("storeName"),
                    "address": self._get_address(store),
                    "phone_number": store.get("phone"),
                    "location": {
                        "type": "Point",
                        "coordinates": [float(store.get("longitude")), float(store.get("latitude"))]
                    },
                    "hours": self._get_hours(store),
                    "url": f"https://www.firestonecompleteautocare.com{store['localPageURL']}",
                    "raw": store
                }
        else:
            self.logger.error("Store not found for: %s", response.url)

    
    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store['address'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = store.get("city", "")
            state = store.get("state", "")
            zipcode = store.get("zip", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""



    def _get_hours(self, store: Dict) -> List[Dict]:
        hours = {}
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        try:
            for day in days:
                for hour in store['hours']:
                    if hour['weekDay'].lower() in day:
                        hours[day] = {
                            "open": convert_to_12h_format(hour['openTime']),
                            "close": convert_to_12h_format(hour['closeTime'])
                        }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting hours for store {store.get('storeNumber')}: {e}")
            return []

