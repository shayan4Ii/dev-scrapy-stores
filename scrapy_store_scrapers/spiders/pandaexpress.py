import scrapy
from scrapy_store_scrapers.utils import *




class PandaExpress(scrapy.Spider):
    name = "pandaexpress"
    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "https": "scrapy_impersonate.ImpersonateDownloadHandler",
        },
        "USER_AGENT": None
    }
    headers = {
        "host": "nomnom-prod-api.pandaexpress.com",
        "clientid": "panda",
        "ui-transformer": "restaurants",
        "nomnom-platform": "web",
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://www.pandaexpress.com",
        "referer": "https://www.pandaexpress.com/",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9"
    }


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://nomnom-prod-api.pandaexpress.com/restaurants/near?lat={zipcode['latitude']}&long={zipcode['longitude']}&radius=20&limit=100&loyalty_filter=false&nomnom=calendars&nomnom_calendars_from=20241115&nomnom_calendars_to=20241124&nomnom_exclude_extref=99997,99996,99987,99988,99989,99994,11111,8888,99998,99999,0000"
            yield scrapy.Request(url, callback=self.parse, headers=self.headers, meta={"impersonate": "chrome_android"})



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
        days_mapping = {
            "sun": "sunday",
            "mon": "monday",
            "tue": "tuesday",
            "wed": "wednesday",
            "thu": "thursday",
            "fri": "friday",
            "sat": "saturday"
        }
        hours = {}
        try:
            calendar = restaurant['calendars']['calendar']
            if not calendar:
                return {}
            for _range in calendar[0]['ranges']:
                day = days_mapping[_range['weekday'].lower()]
                parsed_date_open = datetime.strptime(_range['start'], "%Y%m%d %H:%M")
                parsed_date_close = datetime.strptime(_range['end'], "%Y%m%d %H:%M")
                hours[day] = {
                    "open": parsed_date_open.strftime("%I:%M %p").lower(),
                    "close": parsed_date_close.strftime("%I:%M %p").lower()
                }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}