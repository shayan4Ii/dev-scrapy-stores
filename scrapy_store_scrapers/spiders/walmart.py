import scrapy
from typing import Dict, Iterator
import json
from datetime import datetime

class WalmartSpider(scrapy.Spider):
    name = "walmart"
    allowed_domains = ["www.walmart.com"]
    start_urls = ["https://www.walmart.com/store-directory"]

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
            yield scrapy.Request(url=url, headers=self.get_default_headers(), callback=self.parse)

    def extract_store_ids(self, stores_per_city_dict):
        store_ids = []

        for state, cities in stores_per_city_dict.items():
            for city_data in cities:
                if 'stores' in city_data:
                    stores = city_data['stores']
                    if not isinstance(stores, list):
                        self.logger.error(f"Stores data is not a list for city in state {state}: {city_data}")
                        continue
                else:
                    stores = [city_data]  # Treat the city_data itself as a store

                for store in stores:
                    store_id = store.get('storeId') or store.get('storeid')
                    if store_id:
                        store_ids.append(store_id)
                    else:
                        self.logger.warning(f"No store ID found for store in state {state}: {store}")

        return store_ids

    def parse(self, response):
        script_text = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        json_data = json.loads(script_text)
        # props.pageProps.bootstrapData.cv.storepages._all_.sdStoresPerCityPerState
        stores_per_city = json_data["props"]["pageProps"]["bootstrapData"]["cv"]["storepages"]["_all_"]["sdStoresPerCityPerState"]

        stores_per_city_dict = json.loads(stores_per_city.strip('"'))

        # store_ids = []

        # for state, cities in stores_per_city_dict.items():
        #     for city_data in cities:
        #         if 'stores' in city_data:
        #             if not isinstance(city_data['stores'], list):
        #                 # self.logger.error(f"City data is not a list: {city_data}")
        #                 continue
        #             for store in city_data['stores']:
        #                 store_id = store.get('storeId') or store.get('storeid')
        #                 store_ids.append(store_id)
        #         elif 'storeId' in city_data:
        #             store_ids.append(city_data['storeId'])

        store_ids = self.extract_store_ids(stores_per_city_dict)
        self.logger.info(f"Found {len(store_ids)} store IDs")

        for store_id in store_ids:
            store_url = f"https://www.walmart.com/store/{store_id}"
            yield scrapy.Request(url=store_url, headers=self.get_default_headers(), callback=self.parse_store)
            break

    def parse_store(self, response):
        script_text = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
        
        json_data = json.loads(script_text)
        store_data = json_data['props']['pageProps']['initialData']['initialDataNodeDetail']['data']['nodeDetail']

        item = {
            'name': store_data['displayName'],
            'address': self.format_address(store_data['address']),
            'city': store_data['address']['city'],
            'state': store_data['address']['state'],
            'phone_number': store_data['phoneNumber'],
            'hours': self.format_hours(store_data['operationalHours']),
            'services': [service['displayName'] for service in store_data['services']],
            'url': response.url
        }

        yield item

        
    def format_address(self, address):
        return f"{address['addressLineOne']}, {address['city']}, {address['state']} {address['postalCode']}"

    def format_hours(self, operational_hours):
        formatted_hours = {}
        for day in operational_hours:
            formatted_hours[day['day']] = {
                "open": self.convert_to_12h_format(day['start']),
                "close": self.convert_to_12h_format(day['end'])
            }
        return formatted_hours

    @staticmethod
    def convert_to_12h_format(time_str):
        t = datetime.strptime(time_str, '%H:%M').time()
        return t.strftime('%I:%M %p').lower()
