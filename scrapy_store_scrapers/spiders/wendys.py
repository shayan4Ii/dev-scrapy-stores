import scrapy
from scrapy_store_scrapers.utils import *



class Wendys(scrapy.Spider):
    name = "wendys"
    start_urls = ["https://locations.wendys.com/united-states"]


    def parse(self, response: Response):
        states = response.xpath("//a[@class='Directory-listLink']/@href").getall()
        yield from response.follow_all(states, self.parse_state)


    def parse_state(self, response: Response):
        cities = response.xpath("//a[@class='Directory-listLink']/@href").getall()
        yield from response.follow_all(cities, self.parse_city)

    
    def parse_city(self, response: Response):
        stores = response.xpath("//a[contains(@class,'Teaser-titleLink')]/@href").getall()
        yield from response.follow_all(stores, self.parse_store)


    def parse_store(self, response: Response):
        yield {
            "number": response.xpath("//div[@id='LocationInfo-operating']/@data-corporatecode").get(),
            "name": response.xpath("//h1/text()").get(),
            "phone_number": response.xpath("//a[@data-ya-track='mainphone']/text()").get(),
            "address": self._get_address(response),
            "location": {
                "type": "Point",
                "coordinates": [
                    float(response.xpath("//meta[@itemprop='longitude']/@content").get()),
                    float(response.xpath("//meta[@itemprop='latitude']/@content").get())
                ]
            },
            "hours": self._get_hours(response),
            "services": response.xpath("//span[@itemprop='amenityFeature']/text()").getall(),
            "url": response.url
        }


    def _get_address(self, response: Response) -> str:
        try:
            address_parts = [
                response.xpath("//address[@itemprop='address']//span[@class='c-address-street-1']/text()").get(),
            ]
            street = ", ".join(filter(None, address_parts))

            city = response.xpath("//meta[@itemprop='addressLocality']/@content").get()
            state = response.xpath("//abbr[@itemprop='addressRegion']/text()").get()
            zipcode = response.xpath("//span[@itemprop='postalCode']/text()").get()
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, response: Response) -> dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            for row in response.xpath("//h4[contains(text(), 'Rest')]/following-sibling::div/table[@class='c-location-hours-details']/tbody/tr"):
                for day in days:
                    content = row.xpath("./@content").get().lower()
                    if "closed" in content:
                        break
                    if "all day" in content:
                        d = content.split("all day")[0].strip()
                        if day.startswith(d):   
                            hours[day] = {
                                "open": "12:00 am",
                                "close": "11:59 pm"
                            }
                            break
                    elif ":" in content:
                        d, hour_range = content.split(" ")
                        if day.startswith(d.strip()):
                            open_time, close_time = hour_range.split("-")
                            hours[day] = {
                                "open": convert_to_12h_format(open_time),
                                "close": convert_to_12h_format(close_time)
                            }
                            break
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}