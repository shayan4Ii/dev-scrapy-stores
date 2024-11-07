import scrapy
from scrapy_store_scrapers.utils import *



class Uniqlo(scrapy.Spider):
    name = "uniqlo"
    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "USER_AGENT": None,
        "DOWNLOAD_DELAY": 0.2,
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
    }


    def start_requests(self) -> Iterable[Request]:
        url = "https://map.uniqlo.com/us/api/storelocator/v1/en/stores?limit=100&RESET=true&lang=english&offset=0&r=storelocator"
        yield scrapy.Request(url, callback=self.parse, meta={"impersonate": "chrome"})


    def parse(self, response: Response) -> Iterable[Dict]:
        obj = json.loads(response.text)
        if obj.get("status", "") == "ok":
            stores = obj['result'].get('stores', [])
            for store in stores:    
                url = f"https://map.uniqlo.com/us/api/storelocator/v1/en/stores/{store['id']}"
                yield scrapy.Request(url, callback=self.parse_store, meta={"impersonate": "chrome"})
        else:
            self.logger.error("Error fetching stores: %s", response.url)


    def parse_store(self, response: Response) -> Iterable[Dict]:
        obj = json.loads(response.text)
        if obj.get("status", "") == "ok":
            store = obj['result']
            yield {
                "number": store['id'],
                "name": store['name'],
                "address": store['address'],
                "phone_number": store['phone'],
                "location": {
                    "type": "Point",
                    "coordinates": [float(store['longitude']), float(store['latitude'])]
                },
                "hours": self._get_hours(store),
                "services": self._get_services(store),
                "url": f"https://map.uniqlo.com/us/en/detail/{store['id']}",
                "raw": store
            }
        else:
            self.logger.error("Error fetching store: %s", response.url)


    def _get_hours(self, store: Dict) -> Dict:
        hours = {}
        days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
        try:
            for day in days:
                for key, value in store.items():
                    if 'OpenAt'.lower() in key.lower() or 'CloseAt'.lower() in key.lower():
                        abbr_day = key.replace("OpenAt", "").replace("CloseAt", "").lower()
                        if abbr_day in day:
                            hours[day] = {
                                "open": convert_to_12h_format(store.get(abbr_day+"OpenAt")),
                                "close": convert_to_12h_format(store.get(abbr_day+"CloseAt"))
                            }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e)
            return {}

    
    def _get_services(self, store: Dict) -> Dict:
        services = []
        services_mapping = {
            "orderAndPickFlag": "Order & Pick",
            "clickAndCollectFlag": "Click & Collect",
            "parkingFlag": "Parking",
            "payAtStoreFlag": "Pay at Store",
        }
        for service, name in services_mapping.items():
            if store.get(service):
                services.append(name)
        return services