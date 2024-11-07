import scrapy
from scrapy_store_scrapers.utils import *
from urllib.parse import urlparse, parse_qs



class Deloitte(scrapy.Spider):
    name = "deloitte"
    start_urls = ["https://www2.deloitte.com/us/en/footerlinks/office-locator.html"]


    def parse(self, response: Response): # //script[contains(text(), 'GeoCoordinates')]
        for office in response.xpath("//div[@class='offices']"):
            yield response.follow(
                url=office.xpath(".//h3/a/@href").get(),
                callback=self.parse_office
            )

    def parse_office(self, response: Response):
        obj = response.xpath("//script[contains(text(), 'GeoCoordinates')]/text()").get()
        if obj is None:
            return
        office = json.loads(obj)
        yield {
            "name": office['name'],
            "address": self._get_address(office['address']),
            "phone_number": office.get('telephone')[0] if office.get('telephone') else None,
            "location": {
                "type": "Point",
                "coordinates": [office['geo']['longitude'], office['geo']['latitude']]
            },
            "url":  response.url,
            "raw": office
        }


    def _get_address(self, address: Dict) -> str:
        try:
            address_parts = [i for i in address.get("streetAddress") if i.strip()]

            street = ", ".join(filter(None, address_parts))

            city = address.get("addressLocality", "")
            state = address.get("addressRegion", "")
            zipcode = address.get("postalCode", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace("\u200b","").replace(", ,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
