import scrapy

from scrapy_store_scrapers.utils import *



class Longhornsteakhouse(scrapy.Spider):
    name = "longhornsteakhouse"
    custom_settings = dict(
        CONCURRENT_REQUESTS=8,
        DOWNLOAD_DELAY=0.5,
    )


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            yield scrapy.Request(
                url=f"https://www.longhornsteakhouse.com/api/restaurants?locale=en_US&latitude={zipcode['latitude']}&longitude={zipcode['longitude']}&resultsPerPage=15",
                callback=self.parse,
                headers={
                    "x-source-channel": "WEB",
                    "accept": "application/json, text/plain, */*"
                }
            )


    def parse(self, response: Response) -> Iterable[Dict]:
        restaurants = json.loads(response.text)['restaurants']
        for restaurant in restaurants:
            restaurant_id = restaurant['restaurantNumber']
            partial_item = {
                "number": f"{restaurant_id}",
                "name": restaurant['restaurantName'],
                "address": self._get_address(restaurant),
                "location": self._get_location(restaurant),
                "phone_number": next(iter(restaurant["contactDetail"]['phoneDetail']), [{}]).get("phoneNumber"),
                "url": response.url,
                "services": self._get_services(restaurant),
                "raw": restaurant
            }
            yield scrapy.Request(
                url=f"https://www.longhornsteakhouse.com/api/restaurants/{restaurant_id}?locale=en_US&restaurantNumber={restaurant_id}",
                callback=self.parse_details,
                headers={
                    "x-source-channel": "WEB",
                    "accept": "application/json, text/plain, */*"
                },
                cb_kwargs={"partial_item": partial_item}
            )

    
    def parse_details(self, response: Response, partial_item: Dict) -> Dict:
        restaurant = json.loads(response.text)
        item = {
            **partial_item,
            "hours": self._get_hours(restaurant),
        }
        return item


    def _get_address(self, restaurant: Dict) -> str:
        try:
            address_parts = [
                restaurant['contactDetail']['address']['street1'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = restaurant['contactDetail']['address']['city']
            state = restaurant['contactDetail']['address']['stateCode']
            zipcode = restaurant['contactDetail']['address']['zipCode'][:5]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_location(self, restaurant: Dict) -> Dict:
        try:
            lat = restaurant['contactDetail']['address']['coordinates']['latitude']
            lon = restaurant['contactDetail']['address']['coordinates']['longitude']
            return {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}


    def _get_hours(self, restaurant: Dict) -> Dict:
        new_item = {}
        try:
            for hours in restaurant['restaurant']['restaurantHours']:
                new_item[hours['day'].lower()] = {
                    "open": hours['hoursInfo'][0]['startTime'].lower(),
                    "close": hours['hoursInfo'][0]['endTime'].lower()
                }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
        
    
    def _get_services(self, restaurant: Dict) -> List[str]:
        services = []
        services_mapping = {
            "curbSideTogoEnabled": "curbside",
            "onlineTogoEnabled": "online ordering",
        }
        for feature in restaurant['features']:
            if feature in services_mapping:
                services.append(services_mapping[feature])
        return services