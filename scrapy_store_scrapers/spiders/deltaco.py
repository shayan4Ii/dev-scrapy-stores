import scrapy
from scrapy_store_scrapers.utils import *
import chompjs



class DelTaco(scrapy.Spider):
    name = "deltaco"


    def start_requests(self) -> Iterable[Request]:
        url = "https://locations.deltaco.com/us"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        states = response.xpath("//a[@class='state']/@href").getall()
        yield from response.follow_all(urls=states, callback=self.parse_state)


    def parse_state(self, response: Response):
        cities = response.xpath("//div[@class='city-name']/a/@href").getall()
        yield from response.follow_all(urls=cities, callback=self.parse_city)


    def parse_city(self, response: Response):
        stores = response.xpath("//a[@class='name']/@href").getall()
        yield from response.follow_all(urls=stores, callback=self.parse_store)


    def parse_store(self, response: Response):
        obj = chompjs.parse_js_object(response.xpath("//script[contains(text(), 'Restaurant')]/text()").get().strip())
        item = {
            "number": response.xpath("//script[contains(text(), 'dimensionLocationNumber')]/text()").re_first(r"(?:dimensionLocationNumber\'\:\s\')(.*?)(?:\')"),
            "name": obj.get("name"),
            "phone_number": obj.get("telephone"),
            "location": {
                "type": "Point",
                "coordinates": [float(obj.get("geo", {}).get("longitude")), float(obj.get("geo", {}).get("latitude"))]
            },
            "address": self._get_address(obj.get("address")),
            "hours": self._get_hours(obj.get("openingHoursSpecification")),
            "raw": obj,
            "url": response.url,
            "coming_soon": bool(response.xpath("//span[@class='comingSoon']")),
        }
        if not item['hours'] and not item['coming_soon']:
            item["is_permanently_closed"] = True
        return item


    def _get_address(self, address: Dict) -> str:
        try:
            address_parts = [
                address['streetAddress'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = address['addressLocality']
            state = address['addressRegion']
            zipcode = address['postalCode']

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,",",")
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, hours_data: Dict) -> dict:
        hours = {}
        try:
            for hour_range in hours_data:
                day_of_week = hour_range.get("dayOfWeek").split("/")[-1].lower()
                opens_at = hour_range.get("opens").replace(".","").lower().strip()
                closes_at = hour_range.get("closes").replace(".","").lower().strip()
                if "24hs" in opens_at or "24hs" in closes_at:
                    hours[day_of_week] = {
                        "open": "12:00 am",
                        "close": "11:59 pm"
                    }
                    continue
                if "closed" in opens_at or "closed" in closes_at:
                    continue
                hours[day_of_week] = {
                    "open": opens_at,
                    "close": closes_at
                }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
    

    def is_store_closed(self, day: Dict) -> bool:
        opens_at = day.get("open")
        closes_at = day.get("close")
        if opens_at == "closed" and closes_at == "closed":
            return True
        return False
