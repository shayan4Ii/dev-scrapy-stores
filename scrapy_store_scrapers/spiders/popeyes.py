from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *
from urllib.parse import urlencode




class Popeyes(scrapy.Spider):
    name = "popeyes"
    custom_settings = dict(
        COOKIES_ENABLED=True,
    )
    headers = {
        'Host': 'use1-prod-plk-gateway.rbictg.com',
        'Sec-Ch-Ua': '"Chromium";v="125", "Not.A/Brand";v="24"',
        'X-Platform-Framework': 'react-dom',
        'X-User-Datetime': '2024-11-12T16:08:31+05:00',
        'X-Client-Name': 'plk-rn-web',
        'Sec-Ch-Ua-Platform': '"Linux"',
        'X-Ui-Region': 'US',
        'X-Ui-Platform': 'web',
        'X-Client-Version': 'no-rv-no-uid-29ce0b9',
        'X-Ui-Language': 'en',
        'Sec-Ch-Ua-Mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.112 Safari/537.36',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Origin': 'https://www.popeyes.com',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Accept-Language': 'en-US,en;q=0.9',
        'Priority': 'u=1, i',
    }
    url = "https://use1-prod-plk-gateway.rbictg.com/graphql"


    def start_requests(self) -> Iterable[Request]:
        headers = {
            'Host': 'use1-prod-plk-gateway.rbictg.com',
            'Accept': '*/*',
            'Access-Control-Request-Method': 'GET',
            'Access-Control-Request-Headers': 'content-type,x-aws-waf-token,x-client-name,x-client-version,x-device-id,x-forter-token,x-platform-framework,x-session-id,x-ui-language,x-ui-platform,x-ui-region,x-user-datetime',
            'Origin': 'https://www.popeyes.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.112 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Priority': 'u=1, i',
            'Connection': 'keep-alive',
        }
        querystring = {
            "operationName": "GetNearbyRestaurants",
            "variables": "{\"input\":{\"pagination\":{\"first\":50},\"coordinates\":{\"userLat\":40.74855,\"userLng\":-73.94964},\"radiusStrictMode\":true}}",
            "extensions": "{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"4f7636962d84eeab7b47b60f6eb2a1e527b8fbc656c881a179cfe4f847a641da\"}}"
        }
        url = self.url + "?" + urlencode(querystring)
        yield Request(url, method="OPTIONS", callback=self.parse, headers=headers)


    def parse(self, response: Response):
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            querystring = {
                "operationName": "GetNearbyRestaurants",
                "variables": f"{{\"input\":{{\"pagination\":{{\"first\":50}},\"coordinates\":{{\"userLat\":{zipcode['latitude']},\"userLng\":{zipcode['longitude']}}},\"radiusStrictMode\":true}}}}",
                "extensions": "{\"persistedQuery\":{\"version\":1,\"sha256Hash\":\"4f7636962d84eeab7b47b60f6eb2a1e527b8fbc656c881a179cfe4f847a641da\"}}"
            }
            url = self.url + "?" + urlencode(querystring)
            yield Request(url, method="GET", callback=self.parse_restaurants, headers=self.headers)


    def parse_restaurants(self, response: Response):
        nodes = json.loads(response.text)['data']['restaurantsV2']['nearby']['nodes']
        for node in nodes:
            yield {
                "number": node['storeId'],
                "location": {
                    "type": "Point",
                    "coordinates": [node['longitude'], node['latitude']]
                },
                "address": self._get_address(node['physicalAddress']),
                "phone_number": node['phoneNumber'],
                "services": self._get_services(node),
                "hours": self._get_hours(node),
                "url": f"https://www.popeyes.com/store-locator/store/restaurant_{node['storeId']}",
                "raw": node
            }


    def _get_address(self, node: Dict) -> str:
        try:
            address_parts = [
                node.get("address1", ""),
                node.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = node.get("city", "")
            state = node.get("stateProvinceShort", "")
            zipcode = node.get("postalCode", "")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, node: Dict) -> Dict:
        key_day_mapping = {
            "mon": "monday",
            "tue": "tuesday",
            "wed": "wednesday",
            "thr": "thursday",
            "fri": "friday",
            "sat": "saturday",
            "sun": "sunday"
        }
        hours = {}
        try:
            def get_hours(hours_data: Dict):
                for day in key_day_mapping:
                    parital_key = day[:3]
                    if hours_data[parital_key+"Open"] is None or hours_data[parital_key+"Close"] is None:
                        continue
                    hours[key_day_mapping[day]] = {
                        "open": convert_to_12h_format(hours_data[parital_key+"Open"].replace(":00", "")),
                        "close": convert_to_12h_format(hours_data[parital_key+"Close"].replace(":00", ""))
                    }
            hours_data = node.get('curbsideHours')
            get_hours(hours_data)
            if not hours:
                hours_data = node.get('diningRoomHours')
                get_hours(hours_data)
            if not hours:
                hours_data = node.get("driveThruHours")
                get_hours(hours_data)
            if not hours:
                hours_data = node.get('deliveryHours')
                get_hours(hours_data)
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}


    def _get_services(self, node: Dict) -> List[str]:
        services_mapping = {
            "hasDriveThru": "Drive Thru",
            "hasMobileOrdering": "Mobile Ordering",
        }
        services = []
        for key, value in services_mapping.items():
            if node.get(key):
                services.append(value)
        return services
