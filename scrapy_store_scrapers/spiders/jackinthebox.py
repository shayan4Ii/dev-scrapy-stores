import scrapy
from scrapy_store_scrapers.utils import *
import chompjs



class JackInTheBoxSpider(scrapy.Spider):
    name = "jackinthebox"
    start_urls = ["https://locations.jackinthebox.com/us"]


    def parse(self, response: Response):
        states = response.xpath("//a[@class='state']/@href").getall()
        for state in states:
            yield Request(response.urljoin(state), callback=self.parse_state)


    def parse_state(self, response: Response):
        cities = response.xpath("//div[contains(@class, 'city-name')]/a/@href").getall()
        for city in cities:
            yield Request(response.urljoin(city), callback=self.parse_city)


    def parse_city(self, response: Response):
        stores = response.xpath("//div[@data-location-address]/a[@class='name']/@href").getall()
        for store in stores:
            yield Request(response.urljoin(store), callback=self.parse_store)

    
    def parse_store(self, response: Response):
        obj = chompjs.parse_js_object(response.xpath("//script[contains(text(), 'openingHoursSpecification')]/text()").get())
        item = {
            "number": response.xpath("//script[contains(text(), 'dimensionLocationNumber')]/text()").re_first(r"dimensionLocationNumber\'\:\s\'(\d+)\'"),
            "name": obj['name'],
            "address": self._get_address(obj['address']),
            "location": {
                "type": "Point",
                "coordinates": [float(obj['geo']['longitude']), float(obj['geo']['latitude'])]
            },
            "phone_number": obj['telephone'],
            "hours": self._get_hours(obj['openingHoursSpecification']),
            "url": response.url,
            "raw": obj,
            "coming_soon": False
        }
        if item['hours'].get('monday', {}).get('open', '') == "coming soon":
            item['hours'] = {}
            item['coming_soon'] = True
        yield item


    def _get_address(self, node: Dict) -> str:
        try:
            address_parts = [
                node.get("streetAddress", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = node.get("addressLocality", "")
            state = node.get("addressRegion", "")
            zipcode = node.get("postalCode", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, hours_data: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            for day in days:
                for hour_range in hours_data:
                    if day in hour_range['dayOfWeek'].lower():
                        if hour_range['opens'].lower().replace(".", "") == "closed":
                            continue
                        hours[day] = {
                            "open": hour_range['opens'].lower().replace(".", ""),
                            "close": hour_range['closes'].lower().replace(".", "")
                        }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}