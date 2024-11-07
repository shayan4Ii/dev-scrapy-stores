import scrapy
from scrapy_store_scrapers.utils import *



class Hm(scrapy.Spider):
    name = "hm"


    def start_requests(self) -> Iterable[scrapy.Request]:
        url = f"https://api.storelocator.hmgroup.tech/v2/brand/hm/stores/locale/en_us/country/US?_type=json&campaigns=true&departments=true&openinghours=true&maxnumberofstores=100"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response) -> Iterable[Dict]:
        stores = json.loads(response.text)['stores']
        for store in stores:
            yield {
                "number": store['storeCode'],
                "name": store['name'],
                "address": self._get_address(store),
                "phone_number": store.get("phone"),
                "location": {
                    "type": "Point",
                    "coordinates": [float(store['longitude']), float(store['latitude'])]
                },
                "hours": self._get_hours(store),
                "raw": store
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store['address']['streetName1'],
                store['address']['streetName2']
            ]
            street = ", ".join(filter(None, address_parts))

            city = store.get("city", "")
            state = store['address'].get("state", "")
            zipcode = store['address'].get("postCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).strip(" US")
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, store: Dict) -> Dict:
        hours = {}
        days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
        try:
            for day in days:
                for business_day in store['openingHours']:
                    if business_day['name'].lower() in day:
                        hours[day] = {
                            "open": convert_to_12h_format(business_day['opens']),
                            "close": convert_to_12h_format(business_day['closes'])
                        }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e)
            return {}
