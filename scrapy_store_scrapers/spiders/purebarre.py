import scrapy
from scrapy_store_scrapers.utils import *




class PureBarre(scrapy.Spider):
    name = "purebarre"
    custom_settings = dict(
        DOWNLOAD_HANDLERS = {
            "http": "scrapy_impersonate.ImpersonateDownloadHandler",
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        USER_AGENT = None
    )


    def start_requests(self) -> Iterable[scrapy.Request]:
        url = "https://members.purebarre.com/api/brands/purebarre/locations?open_status=external&offer_slug="
        yield scrapy.Request(url, callback=self.parse_locations, meta={"impersonate": "chrome"})


    def parse_locations(self, response):
        data = json.loads(response.text)['locations']
        for location in data:
            if location['country_code'] != "US":
                continue
            partial_item = {
                "number": f"{location['seq']}",
                "name": location['name'],
                "address": self._get_address(location),
                "phone_number": location['phone'],
                "location": {
                    "type": "Point",
                    "coordinates": [location['lng'], location['lat']]
                },
                "hours": {},
                "url": f"https://www.purebarre.com/location/{location['site_slug']}",
                "raw": location,
                "coming_soon": location['coming_soon']
            }
            yield scrapy.Request(
                url=f"https://www.purebarre.com/location/{location['site_slug']}",
                callback=self.parse_location_page,
                cb_kwargs={"partial_item": partial_item},
                meta={"impersonate": "chrome"}
            )
    

    def parse_location_page(self, response: Response, partial_item: Dict):
        hours = response.xpath("//span[@class='location-info-map__info']/@data-hours").get()
        if hours is None:
            return partial_item
        obj = json.loads(hours)
        partial_item["raw"]["hours"] = obj
        for day, hours in obj.items():
            if len(hours) == 1:
                partial_item['hours'][day] = {
                    "open": convert_to_12h_format(hours[0][0]),
                    "close": convert_to_12h_format(hours[0][1]),
                }
            elif len(hours) > 1:
                partial_item['hours'][day] = [{
                    "open": convert_to_12h_format(hour[0]),
                    "close": convert_to_12h_format(hour[1]),
                } for hour in hours]
            else:
                self.logger.error(f"Invalid hours: {hours} for {partial_item['url']}")
                partial_item['hours'][day] = None

        item = partial_item
        return item


    def _get_address(self, location: Dict) -> str:
        try:
            address_parts = [
                location['address'],
                location['address2'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = location['city']
            state = location['state']
            zipcode = location['zip']

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        
