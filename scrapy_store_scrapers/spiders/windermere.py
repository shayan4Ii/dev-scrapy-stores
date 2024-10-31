import scrapy
from typing import Dict, Iterable, Generator
from scrapy.http import Response, Request
from scrapy_store_scrapers.utils import *



class Windermere(scrapy.Spider):
    name = "windermere"
    params = {
        'pgsize': '500',
        'site_type': 'Brokerage Website',
        'from_app': 'aws:https://www.windermere.com',
    }


    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(
            url="https://www.windermere.com/services/cacheproxy/service/profile/v2_insecure/offices/-proxy-search",
            callback=self.parse
        )


    def parse(self, response: Response, **kwargs) -> Generator[Dict, None, None]:
        offices = json.loads(response.text)['data']['result_list']
        for office in offices:
            yield {
                "number": f"{office['uuid']}",
                "name": office['name'],
                "address": self._get_address(office),
                "location": self._get_location(office),
                "phone_number": office.get("phone"),
                "url": self._get_url(office),
                "raw": office
            }

    def _get_url(self, office: Dict) -> str:
        url_slug = office.get('url_slug')
        if url_slug:
            return f"https://www.windermere.com/directory/offices/{url_slug}"

    def _get_address(self, office: Dict) -> str:
        try:
            address_parts = [
                office['location']["address"],
                office['location']["address2"],
            ]
            street = ", ".join(filter(None, address_parts))

            city = office['location'].get("city", "")
            state = office['location'].get("state", "")
            zipcode = office['location'].get("zip", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_location(self, office: Dict) -> Dict:
        try:
            lat = float(str(office['location'].get("latitude", 0)))
            lon = float(str(office['location'].get("longitude", 0)))
            return {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}