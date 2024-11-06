import scrapy
from scrapy_store_scrapers.utils import *



class BananaRepublicSpider(scrapy.Spider):
    name = "bananarepublic"


    def start_requests(self) -> Iterable[scrapy.Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://bananarepublic.gap.com/stores/maps/api/getAsyncLocations?template=search&level=search&search={zipcode['zipcode']}"
            yield scrapy.Request(url, callback=self.parse_stores)


    def parse_stores(self, response):
        stores = json.loads(response.text)
        if stores.get("markers") is None:
            return
        maplist = json.loads("[" + scrapy.Selector(text=stores['maplist']).xpath("//div/text()").get().strip(",") + "]")
        for store in stores.get("markers", []):
            info = json.loads(scrapy.Selector(text=store['info']).xpath("//div/text()").get())
            hours_data = next((json.loads(item['hours_sets:primary'])['days'] for item in maplist if item["fid"] == info["fid"]), None)
            yield {
                "number": info["fid"],
                "name": info["location_name"],
                "address": self._get_address(info),
                "phone_number": info["local_phone"],
                "location": {
                    "type": "Point",
                    "coordinates": [float(info["lng"]), float(info["lat"])],
                },
                "services": [service['name'] for service in store.get('specialties', [])],
                "hours": self._get_hours(hours_data),
                "url": info['url'],
                "raw": info
            }


    def _get_address(self, info: Dict) -> str:
        try:
            address_parts = [
                info.get("address_1", ""),
                info.get("address_2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = info.get("city", "")
            state = info.get("region", "")
            zipcode = info.get("post_code", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]


            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, hours_data: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        new_item = {}
        try:
            for day in days:
                for d, hours in hours_data.items():
                    if d.lower() == day:
                        new_item[day] = {
                            "open": convert_to_12h_format(hours[0]['open']),
                            "close": convert_to_12h_format(hours[0]['close'])
                        }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
