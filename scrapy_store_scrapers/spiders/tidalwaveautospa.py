import scrapy
from scrapy_store_scrapers.utils import *



class Tidalwaveautospa(scrapy.Spider):
    name = "tidalwaveautospa"


    def start_requests(self) -> Iterable[Request]:
        url = "https://www.tidalwaveautospa.com/locations/"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        url = response.xpath("//script[contains(@src, '/embed/')]/@src").get()
        yield scrapy.Request(url, callback=self.parse_locations)

    
    def parse_locations(self, response: Response):
        obj = json.loads(response.text.split("bizDataResp =")[-1].strip().split("var locale")[0].strip().rstrip(";"))
        for location in obj['businessLocations']:
            partial_item = {
                "number": f"{location['businessId']}",
                "name": location['name'],
                "address": self._get_address(location),
                "location": {
                    "type": "Point",
                    "coordinates": [location['longitude'], location['latitude']]
                },
                "phone_number": location['phone'],
                "raw": location
            }
            slug = f"{location['city']} {location['state']}".lower().replace(" ", "-")
            url = f"https://www.tidalwaveautospa.com/location/{slug}/"
            yield scrapy.Request(url, callback=self.parse_location, cb_kwargs={"partial_item": partial_item})


    def parse_location(self, response: Response, partial_item: Dict):
        item = partial_item.copy()
        item["hours"] = self._get_hours(response)
        item['url'] = response.url
        yield item


    # def start_requests(self) -> Iterable[Request]:
    #     url = "https://www.tidalwaveautospa.com/locations/"
    #     yield scrapy.Request(url, callback=self.parse)


    # def parse(self, response: Response):
    #     locations = response.xpath("//a[contains(@href, '/location/') and contains(text(), 'View')]/@href").getall()
    #     yield from response.follow_all(locations, callback=self.parse_location)


    # def parse_location(self, response: Response):
    #     return {
    #         "name": response.xpath("//p[contains(@class, 'text-center h2')]/text()").get(),
    #         "phone_number": response.xpath("//a[contains(@class, 'phone')]/text()").get(),
    #         "location": '', # //a[contains(@class, 'convertDirections')]/@href
    #         "address": self._get_address(response)
    #     }


    def _get_address(self, address: Dict) -> str:
        try:
            address_parts = [
                address['address1'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = address['city']
            state = address['state']
            zipcode = address['zipcode']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    
    def _get_hours(self, response: Response):
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            days_range, hours_range = [part.strip().lower() for part in response.xpath("//span[contains(text(), 'Hours')]/following-sibling::text()").getall() if part.strip()]
            if 'am' in days_range or 'pm' in days_range:
                days_range, hours_range = hours_range, days_range
            hours_range = hours_range.split("exterior")[0].strip() if "exterior" in hours_range else hours_range
            start_day, end_day = [d.strip().lower() for d in days_range.split("-")]
            for day in days[days.index(start_day):days.index(end_day) + 1]:
                hours[day] = {
                    "open": convert_to_12h_format(hours_range.split("-")[0].lower().strip().replace("am", " am").replace("pm", " pm")),
                    "close": convert_to_12h_format(hours_range.split("-")[1].lower().strip().replace("am", " am").replace("pm", " pm"))
                }
            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}