import scrapy
from scrapy_store_scrapers.utils import *



class BananaRepublicSpider(scrapy.Spider):
    name = "bananarepublic"


    def start_requests(self) -> Iterable[scrapy.Request]:
        url = "https://bananarepublic.gap.com/stores"
        yield scrapy.Request(url, callback=self.parse_state)


    def parse_state(self, response: Response):
        states = response.xpath("//li[@role='listitem']//a/@href").getall()
        for state in states:
            yield scrapy.Request(url=response.urljoin(state), callback=self.parse_cities)


    def parse_cities(self, response: Response):
        cities = response.xpath("//a[@data-city-item]/@href").getall()
        for city in cities:
            yield scrapy.Request(url=response.urljoin(city), callback=self.parse_city)


    def parse_city(self, response: Response):
        stores = response.xpath("//a[@class='view-store ga-link']/@href").getall()
        for store in stores:
            yield scrapy.Request(url=response.urljoin(store), callback=self.parse_store)


    def parse_store(self, response: Response):
        data = json.loads(response.xpath("//div[@id='map-data-wrapper']//script[contains(text(), 'RLS.defaultData')]/text()").get().strip(";").strip("RLS.defaultData = "))
        info = json.loads(scrapy.Selector(text=data['markerData'][0]['info']).xpath("//div/text()").get())
        hours_data = json.loads(re.search(r'(?:\["primary"\]\s\=\s)(.*?)(?:\;)', response.text).group(1))
        yield {
            "number": info["fid"],
            "name": info["location_name"],
            "address": self._get_address(info),
            "phone_number": info["local_phone"],
            "location": {
                "type": "Point",
                "coordinates": [float(info["lng"]), float(info["lat"])],
            },
            "services": [service['name'] for service_id, service in data['locationSpecialties'].items()],
            "hours": self._get_hours(hours_data['days']),
            "url": info['url'],
            "raw": {**info, "specialties": data['locationSpecialties']}
        }


    def _get_address(self, info: Dict) -> str:
        try:
            address_parts = [
                info.get("address_1", ""),
                info.get("address_2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = info.get("city", "")
            state = info.get("region", "")
            zipcode = info.get("post_code", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]


            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, hours_data: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        new_item = {}
        try:
            for day in days:
                for d, hours in hours_data.items():
                    if d.lower() == day:
                        if isinstance(hours, str):
                            continue
                        new_item[day] = {
                            "open": convert_to_12h_format(hours[0]['open']),
                            "close": convert_to_12h_format(hours[0]['close'])
                        }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
