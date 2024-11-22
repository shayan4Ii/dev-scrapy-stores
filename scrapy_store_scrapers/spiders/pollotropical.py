import scrapy
from scrapy_store_scrapers.utils import *
from urllib.parse import urlencode
from scrapy.exceptions import CloseSpider



class PolloTropical(scrapy.Spider):
    name = "pollotropical"
    

    def start_requests(self) -> Iterable[Request]:
        url = "https://locations.pollotropical.com/"
        yield scrapy.Request(url, callback=self.parse)
        

    
    def parse(self, response: Response):
        states = response.xpath("//ul/li/a/@href").getall()
        yield from response.follow_all(urls=states, callback=self.parse_state)


    def parse_state(self, response: Response):
        cities = response.xpath("//ul/li/a/@href").getall()
        yield from response.follow_all(urls=cities, callback=self.parse_city)


    def parse_city(self, response: Response):
        stores = response.xpath("//div[@class='Core-nearbyLocTitle']/a/@href").getall()
        yield from response.follow_all(urls=stores, callback=self.parse_store)


    def parse_store(self, response: Response):
        obj = json.loads(response.xpath("//script[contains(text(), 'LocalBusiness')]/text()").get())
        return {
            "name": response.xpath("//span[@class='LocationName-brand']/text()").get(),
            "location": {
                "type": "Point",
                "coordinates": [float(response.xpath("//meta[@itemprop='longitude']/@content").get()), float(response.xpath("//meta[@itemprop='latitude']/@content").get())]
            },
            "url": response.url,
            "phone_number": response.xpath("//a[@itemprop='telephone']/text()").get(),
            "address": self._get_address(response),
            "hours": self._get_hours(obj),
            "raw": obj
        }


    def _get_address(self, response: Response) -> str:
        try:
            address_parts = [
                response.xpath("//meta[@itemprop='streetAddress']/@content").get(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("//meta[@itemprop='addressLocality']/@content").get()
            state = response.xpath("//abbr[@itemprop='addressRegion']/text()").get()
            zipcode = response.xpath("//span[@itemprop='postalCode']/text()").get()
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, obj: Dict):
        hours = {}
        # Mo,Tu,We,Th,Fr,Sa,Su 10:30-00:00
        days = {
            "mo": "monday",
            "tu": "tuesday",
            "we": "wednesday",
            "th": "thursday",
            "fr": "friday",
            "sa": "saturday",
            "su": "sunday",
        }
        try:
            opening_hours = obj.get("@graph", [{}])[0].get("openingHours", [None])
            if not opening_hours or not list(filter(None, opening_hours)):
                return {}
            for open_hour in opening_hours:
                days_string, hours_string = open_hour.lower().split(" ")
                for key, day in days.items():
                    if key in days_string:
                        hours[day] = {
                            "open": convert_to_12h_format(hours_string.split("-")[0]),
                            "close": convert_to_12h_format(hours_string.split("-")[-1])
                        }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
