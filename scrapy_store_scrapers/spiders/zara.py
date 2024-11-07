import scrapy
from scrapy_store_scrapers.utils import *
import chompjs



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
        url = "https://www.zara.com/us/en/z-stores-st1404.html?v1=11108"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response) -> Iterable[Dict]:
        stores = response.xpath("//li[@class='store-sub-accordions__city-stores-item']/a/@href").getall()
        for store in stores:
            yield scrapy.Request(store, callback=self.parse_store)


    def parse_store(self, response: Response) -> Iterable[Dict]:
        store = list(chompjs.parse_js_objects(response.xpath("//script[contains(text(), 'appConfig')]/text()").get().strip(";").strip("window.zara.appConfig = ")))[-1]['physicalStoreExtendedDetails']
        yield {
            "number": store['id'],
            "name": store.get('commercialName'),
            "address": self._get_address(store),
            "phone_number": store.get("phone"),
            "location": {
                "type": "Point",
                "coordinates": [store['coordinates']['longitude'], store['coordinates']['latitude']]
            },
            "hours": self._get_hours(store.get('openingHours', {}).get('schedule', [])),
            "url": store['url'],
            "services": [attr.get('title') for attr in store.get('attributes', [])],
            "raw": store
        }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store['address'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = store.get("city", "")
            state = store.get("province", "")
            zipcode = store.get("zipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, opening_hours: List[Dict]) -> Dict:
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
                for opening_hour in opening_hours:
                    if opening_hour['weekDay'] == idx:
                        if not opening_hour['hours']:
                            continue
                        hours[day] = {
                            "open": convert_to_12h_format(opening_hour['hours'][0]),
                            "close": convert_to_12h_format(opening_hour['hours'][-1])
                        }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
        


