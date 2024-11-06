import scrapy
from scrapy_store_scrapers.utils import *



class Bechtel(scrapy.Spider):
    name = "bechtel"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        USER_AGENT = None
    )


    def start_requests(self) -> Iterable[scrapy.Request]:
        url = "https://www.bechtel.com/about/locations/" 
        yield scrapy.Request(url, callback=self.parse_locations, meta={"impersonate": "chrome"})


    def parse_locations(self, response: Response):
        obj = json.loads(json.loads(response.xpath("//script[contains(text(), 'serializedChunks')]/text()").get().split(";")[0].split("=")[-1].strip())[0])
        for location in obj:
            if location['Country'] != "USA":
                continue
            yield {
                "name": location["OfficeTitle"],
                "address": self._get_address(location),
                "phone_number": location["Telephone"],
                "location": {
                    "type": "Point",
                    "coordinates": [location["Longitude"], location["Latitude"]]
                },
                "raw": location
            }


    def _get_address(self, location: Dict) -> str:
        try:
            address_parts = [
                location['AddressLine1'],
                location['AddressLine2'],
                location['AddressLine3'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = location['City']
            state = location['State']
            zipcode = location['Zip']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()


            address = ", ".join(filter(None, [street, city_state_zip])).strip("-")
            address = re.sub(r"\s+", " ", address).strip()
            return address.strip(",").strip().replace(", ,", ",").replace(",,", ",")
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
