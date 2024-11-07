from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *



class Pacsun(scrapy.Spider):
    name = "pacsun"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        USER_AGENT = None
    )


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://www.pacsun.com/on/demandware.store/Sites-pacsun-Site/default/Stores-FindStores?showMap=false&radius=300%20Miles&lat={zipcode['latitude']}&long={zipcode['longitude']}&findInStore=false"
            yield scrapy.Request(url, callback=self.parse, meta={"impersonate": "chrome"})


    def parse(self, response: Response) -> Iterable[Dict]:
        stores = json.loads(response.text)['stores']
        for store in stores:
            yield {
                "number": store['ID'],
                "name": store['name'],
                "address": self._get_address(store),
                "phone_number": store['phone'],
                "location": {
                    "type": "Point",
                    "coordinates": [store['longitude'], store['latitude']]
                },
                "hours": self._get_hours(store['storeHours']),
                "url": store['seoURL'],
                "raw": store
            }

    
    def _get_address(self, location: Dict) -> str:
        try:
            address_parts = [
                location['address1'],
                location['address2'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = location['city']
            state = location['stateCode']
            zipcode = location['postalCode']

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, store_hours: str) -> dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            sel = scrapy.Selector(text=store_hours)
            for hour_range in sel.xpath("//div[@class='hours']/text()").getall():
                if "monday - friday" in hour_range.lower():
                    mon_fri_open, mon_fri_close = hour_range.replace("Monday - Friday", "").strip().split("-")
                    for day in days[:5]:
                        hours[day] = {
                            "open": convert_to_12h_format(mon_fri_open),
                            "close": convert_to_12h_format(mon_fri_close)
                        }
                elif "saturday" in hour_range.lower():
                    sat_open, sat_close = hour_range.replace("Saturday", "").strip().split("-")
                    for day in days[5:6]:
                        hours[day] = {
                            "open": convert_to_12h_format(sat_open),
                            "close": convert_to_12h_format(sat_close)
                        }
                elif "sunday" in hour_range.lower():
                    sun_open, sun_close = hour_range.replace("Sunday", "").strip().split("-")
                    for day in days[5:6]:
                        hours[day] = {
                            "open": convert_to_12h_format(sun_open),
                            "close": convert_to_12h_format(sun_close)
                        }

            return hours
        except Exception as e:
            self.logger.error(f"Error getting hours: {e}")
            return {}