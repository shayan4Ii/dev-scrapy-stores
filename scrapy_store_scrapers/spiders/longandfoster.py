import re

import scrapy
from typing import Dict, Generator, Iterable, Any
from scrapy.http import Response, Request
from scrapy_store_scrapers.utils import *



class LongandFoster(scrapy.Spider):
    name = "longandfoster"


    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(
            url="https://www.longandfoster.com/pages/real-estate-offices",
            callback=self.parse
        )


    def parse(self, response: Response, **kwargs) -> Generator[Request, None, None]:
        pages = response.xpath("//div[@id='Master_dlCity']//a/@href").getall()
        for page in pages:
            yield scrapy.Request(
                url=page,
                callback=self.parse_page
            )


    def parse_page(self, response: Response):
        offices = response.xpath("//div[@id='Master_dlCity']//a/@href").getall()
        for url in offices:
            yield scrapy.Request(
                url=url,
                callback=self.parse_office
            )


    def parse_office(self, response: Response):
        try:
            match = re.search(r"(?:stringify\()(.*?)(?:\);)", response.xpath("//script[contains(text(), 'officeJSONData')]/text()").get(), re.DOTALL)
            data = re.sub(r'\s+', " ", match.group(1)).replace("desc()",'"a"')
        except TypeError:
            self.logger.info("Office not found!")
            return
        office = json.loads(data)
        url = office['url']
        yield {
            "name": office['name'],
            "address": self._get_address(office),
            "location": self._get_location(response),
            "phone_number": office["telephone"],
            "url": 'https://' + url if not url.startswith(('http://', 'https://')) else url,
            "raw": office
        }


    def _get_address(self, office: Dict) -> str:
        try:
            address_parts = [
                office['address']['streetAddress'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = office['address']['addressLocality']
            state = office['address']['addressRegion']
            zipcode = office['address']['postalCode']

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_location(self, response: Response) -> Dict:
        try:
            lat_match = re.search(r'(?:startingMidLat:\s)(.*?)(?:,)', response.text)
            long_match = re.search(r'(?:startingMidLong:\s)(.*?)(?:,)', response.text)
            lat = float(str(lat_match.group(1)))
            lon = float(str(long_match.group(1)))
            return {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}