import scrapy
from scrapy_store_scrapers.utils import *



class PandaExpress(scrapy.Spider):
    name = "pandaexpress"
    headers = {
        "host": "nomnom-prod-api.pandaexpress.com",
        "clientid": "panda",
        "ui-transformer": "restaurants",
        "nomnom-platform": "web",
        "accept": "application/json",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "origin": "https://www.pandaexpress.com",
        "referer": "https://www.pandaexpress.com/",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9"
    }


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://nomnom-prod-api.pandaexpress.com/restaurants/near?lat={zipcode['latitude']}&long={zipcode['longitude']}&radius=20&limit=100&loyalty_filter=false&nomnom=calendars&nomnom_calendars_from=20241113&nomnom_calendars_to=20241124&nomnom_exclude_extref=99997,99996,99987,99988,99989,99994,11111,8888,99998,99999,0000"
            yield scrapy.Request(url, callback=self.parse, headers=self.headers)



    def parse(self, response: Response):
        restaurants = response.json()['restaurants']
        for restaurant in restaurants:
            yield {
                "number": f"{restaurant['id']}",
                "name": restaurant['name'],
                "address": self._get_address(restaurant),
                "location": {
                    "type": "Point",
                    "coordinates": [
                        restaurant['longitude'],
                        restaurant['latitude']
                    ]
                },
                "hours": self._get_hours(restaurant),
                "phone_number": restaurant['telephone'],
                "raw": restaurant
            }


    def _get_address(self, restaurant: Dict) -> str:
        try:
            address_parts = [
                restaurant.get("streetaddress", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = restaurant.get("city", "")
            state = restaurant.get("state", "")
            zipcode = restaurant.get("zip", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, restaurant: Dict) -> Dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            hours_data = restaurant['calendars']['calendar'][0]['ranges'] if restaurant['calendars']['calendar'] else []
            for day in days:
                for hour_range in hours_data:
                    if hour_range['weekday'].lower() in day:
                        hours[day] = {
                            "open": convert_to_12h_format(hour_range['start'].split(" ")[1]),
                            "close": convert_to_12h_format(hour_range['end'].split(" ")[1])
                        }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}