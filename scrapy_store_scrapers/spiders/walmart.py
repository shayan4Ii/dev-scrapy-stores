import scrapy
from typing import Dict, Iterator, List, Any
import json
from datetime import datetime
from scrapy_store_scrapers.items import WalmartStoreItem

class WalmartSpider(scrapy.Spider):
    name: str = "walmart"
    allowed_domains: List[str] = ["www.walmart.com"]
    start_urls: List[str] = ["https://www.walmart.com/store-directory"]

    @staticmethod
    def get_default_headers() -> Dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "TE": "Trailers",
        }

    def start_requests(self) -> Iterator[scrapy.Request]:
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=self.get_default_headers(), callback=self.parse_store_directory)

    def extract_store_ids(self, stores_by_location: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        store_ids = []

        for state, cities in stores_by_location.items():
            for city_data in cities:
                stores = city_data.get('stores', [city_data])
                if not isinstance(stores, list):
                    self.logger.error(f"Stores data is not a list for city in state {state}: {city_data}")
                    continue

                for store in stores:
                    store_id = store.get('storeId') or store.get('storeid')
                    if store_id:
                        store_ids.append(str(store_id))
                    else:
                        self.logger.warning(f"No store ID found for store in state {state}: {store}")

        return store_ids

    def parse_store_directory(self, response: scrapy.http.Response) -> Iterator[scrapy.Request]:
        script_content = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        json_data = json.loads(script_content)

        stores_by_location_json = json_data["props"]["pageProps"]["bootstrapData"]["cv"]["storepages"]["_all_"]["sdStoresPerCityPerState"]
        stores_by_location = json.loads(stores_by_location_json.strip('"'))

        store_ids = self.extract_store_ids(stores_by_location)
        self.logger.info(f"Found {len(store_ids)} store IDs")

        for store_id in store_ids:
            store_url = f"https://www.walmart.com/store/{store_id}"
            yield scrapy.Request(url=store_url, headers=self.get_default_headers(), callback=self.parse_store)

    def parse_store(self, response: scrapy.http.Response) -> WalmartStoreItem:
        script_content = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        
        json_data = json.loads(script_content)
        store_data = json_data['props']['pageProps']['initialData']['initialDataNodeDetail']['data']['nodeDetail']

        store_item = WalmartStoreItem(
            name=store_data['displayName'],
            address=self.format_address(store_data['address']),
            city=store_data['address']['city'],
            state=store_data['address']['state'],
            phone_number=store_data['phoneNumber'],
            hours=self.format_hours(store_data['operationalHours']),
            services=[service['displayName'] for service in store_data['services']],
            url=response.url
        )

        return store_item

    @staticmethod
    def format_address(address: Dict[str, str]) -> str:
        return f"{address['addressLineOne']}, {address['city']}, {address['state']} {address['postalCode']}"

    def format_hours(self, operational_hours: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        formatted_hours = {}
        for day_hours in operational_hours:
            formatted_hours[day_hours['day']] = {
                "open": self.convert_to_12h_format(day_hours['start']),
                "close": self.convert_to_12h_format(day_hours['end'])
            }
        return formatted_hours

    @staticmethod
    def convert_to_12h_format(time_str: str) -> str:
        time_obj = datetime.strptime(time_str, '%H:%M').time()
        return time_obj.strftime('%I:%M %p').lower()
