from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *




class Menards(scrapy.Spider):
    name = "menards1"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "https": "scrapy_store_scrapers.utils.MuxDownloadHandler",
        },
        USER_AGENT = None,
        CONCURRENT_REQUESTS = 1,
        DOWNLOAD_DELAY = 5,
    )


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            yield scrapy.Request(f"https://www.menards.com/store-details/locate-stores-by-address.ajx?postalCode={zipcode['zipcode']}", callback=self.parse, meta={"impersonate": "edge99"})
            break


    def parse(self, response: Response):
        stores = json.loads(response.text)['storeResults']
        for store in stores:
            partial_item = {
                "number": store["number"],
                "name": store["name"],
                "address": self._get_address(store),
                "location": {
                    "type": "Point",
                    "coordinates": [
                        store["longitude"],
                        store["latitude"]
                    ]
                },
                "url": f"https://www.menards.com/store-details/store.html?store={store['number']}",
                "services": [service['displayName'] for service in store['services']],
                "raw": store
            }
            yield scrapy.Request(
                url=partial_item["url"],
                callback=self.parse_store,
                cb_kwargs={"partial_item": partial_item},
                meta={"playwright": True}
            )

    
    def parse_store(self, response: Response, partial_item: Dict):
        phone = response.xpath("//a[@id='store-phone']/@href").get().split(":")[-1]
        partial_item["phone_number"] = phone
        partial_item['hours'] = self._get_hours(response)


    def _get_address(self, store_info) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("street", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""
        
    
    def _get_hours(self, response: Response) -> dict:
        days = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"]
        hours = {}
        try:
            for row in ("//div[@id='storeHoursTitle']/following-sibling::table/tbody/tr"):
                pass
            return hours
        except Exception as e:
            self.logger.error(f"Error getting hours: {e}")
            return {}     
