import scrapy
import chompjs
from scrapy_store_scrapers.utils import *
from w3lib.html import remove_tags_with_content



class TimHortons(scrapy.Spider):
    name = "timhortons"


    def start_requests(self) -> Iterable[Request]:
        url = "https://locations.timhortons.com/en/locations-list"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        sel = scrapy.Selector(text=remove_tags_with_content(response.text, which_ones=["noscript","iframe"]))
        states = sel.xpath("//a[@class='directory_links']/@href").getall()
        yield from response.follow_all(urls=states, callback=self.parse_state)


    def parse_state(self, response: Response):
        sel = scrapy.Selector(text=remove_tags_with_content(response.text, which_ones=["noscript","iframe"]))
        cities = sel.xpath("//a[@class='directory_links']/@href").getall()
        yield from response.follow_all(urls=cities, callback=self.parse_city)


    def parse_city(self, response: Response):
        sel = scrapy.Selector(text=remove_tags_with_content(response.text, which_ones=["noscript","iframe"]))
        stores = sel.xpath("//a[@class='directory_links']/@href").getall()
        yield from response.follow_all(urls=stores, callback=self.parse_store)


    def parse_store(self, response: Response):
        obj = chompjs.parse_js_object(response.xpath("//script[@data-schema-type='localbusiness']/text()").get())
        item = {
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
            "services": [txt.strip() for txt in response.xpath("//div[@class='lp-banner-features']/ul/li/text()").getall() if txt.strip()],
        }
        if not item['hours']:
            item['is_permanently_closed'] = True
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
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0].strip()

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
                opens_at = hour_range.get("opens").replace(":00","", 1).lower().strip()
                closes_at = hour_range.get("closes").replace(":00","", 1).lower().strip()
                if "24hs" in opens_at or "24hs" in closes_at:
                    hours[day_of_week] = {
                        "open": "12:00 am",
                        "close": "11:59 pm"
                    }
                    continue
                if "closed" in opens_at or "closed" in closes_at:
                    continue
                if "24:00" in closes_at:
                    closes_at = "23:59"
                hours[day_of_week] = {
                    "open": convert_to_12h_format(opens_at),
                    "close": convert_to_12h_format(closes_at)
                }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
    
    
