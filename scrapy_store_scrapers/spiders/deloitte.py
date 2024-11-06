import scrapy
from scrapy_store_scrapers.utils import *
from urllib.parse import urlparse, parse_qs



class Deloitte(scrapy.Spider):
    name = "deloitte"
    start_urls = ["https://www2.deloitte.com/us/en/footerlinks/office-locator.html"]


    def parse(self, response):
        for office in response.xpath("//div[@class='offices']"):
            yield {
                "name": office.xpath(".//h3/a/text()").get().strip(),
                "address": " ".join([re.sub(r'\s+\t+', " ", line) for line in office.xpath("./div[@class='address']/p/text()").getall()[:-1]]).strip(),
                "phone_number": office.xpath("./div[@class='contact']/p/a/text()").get('').strip(),
                "location": self._get_location(office.xpath(".//a[@class='view_map']/@href").get()),
                "url":  response.urljoin(office.xpath(".//h3/a/@href").get().strip()),
            }


    def _get_location(self, map_url: str):
        coordinates = parse_qs(urlparse(map_url).query).get("sll") or parse_qs(urlparse(map_url).query).get("ll")
        if coordinates is None:
            if "/maps/place" in map_url:
                coordinates = map_url.split("/@")[-1].split("/")[0].split(",")[:-1]
                if len(coordinates) != 2:
                    return None
            else:
                return None
        else:
            coordinates = coordinates[0].split(",")
        
        return {
            "type": "Point",
            "coordinates": [float(coordinates[1]), float(coordinates[0])]
        }
