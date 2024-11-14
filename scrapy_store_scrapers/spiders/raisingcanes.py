import scrapy
from scrapy_store_scrapers.utils import *
from urllib.parse import urlencode
from scrapy.exceptions import CloseSpider



class RaisingCanes(scrapy.Spider):
    name = "raisingcanes"
    params = {
        'experienceKey': 'locator',
        'v': '20220511',
        'version': 'PRODUCTION',
        'locale': 'en',
        'input': '',
        'verticalKey': 'locations',
        'limit': '50',
        'offset': '0',
        'retrieveFacets': 'true',
        'facetFilters': '{}',
        'skipSpellCheck': 'false',
        'sessionTrackingEnabled': 'false',
        'sortBys': '[]',
        'source': 'STANDARD',
    }

    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(f"https://locations.raisingcanes.com/search", callback=self.get_api_key)
        


    def get_api_key(self, response: Response):
        api_key = response.xpath("//script[contains(text(), 'decodeURIComponent')]").re_first(r'(?:searchExperienceAPIKey)(.*?)(?:searchPage)').split("%22")[2]
        if api_key:
            self.params['api_key'] = api_key
            zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
            for zipcode in zipcodes:
                self.params['location'] = f"{zipcode['latitude']},{zipcode['longitude']}"
                self.params['filters'] = json.dumps({
                    "builtin.location": {
                        "$near": {
                            "lat": zipcode['latitude'],
                            "lng": zipcode['longitude'],
                            "radius": 40233.6
                        }
                    }
                })
                url = f"https://prod-cdn.us.yextapis.com/v2/accounts/me/search/vertical/query?{urlencode(self.params)}"
                yield scrapy.Request(url, callback=self.parse)
        else:
            raise CloseSpider("No API key found")


    def parse(self, response: Response):
        for restaurant in response.json()['response']['results']:
            restaurant_data = restaurant['data']
            yield {
                "number": restaurant_data['id'],
                "name": restaurant_data['name'],
                "address": self._get_address(restaurant_data['address']),
                "location": {
                    "type": "Point",
                    "coordinates": [restaurant_data.get('geocodedCoordinate', {}).get('longitude'), restaurant_data.get('geocodedCoordinate', {}).get('latitude')]
                },
                "hours": self._get_hours(restaurant_data),
                "url": restaurant_data['website'],
                "phone": restaurant_data['mainPhone'],
                "raw": restaurant_data
            }


    def _get_address(self, restaurant: Dict) -> str:
        try:
            address_parts = [
                restaurant.get("line1", ""),
                restaurant.get("line2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = restaurant.get("city", "")
            state = restaurant.get("region", "")
            zipcode = restaurant.get("postalCode", "")
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
            for d, hour_range in restaurant['hours'].items():
                hours[d] = {
                    "open": convert_to_12h_format(hour_range['openIntervals'][0]['start']),
                    "close": convert_to_12h_format(hour_range['openIntervals'][0]['end'])
                }
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
