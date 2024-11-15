import scrapy
from scrapy_store_scrapers.utils import *
from urllib.parse import urlencode, quote



class Bk(scrapy.Spider):
    name = "bk"
    custom_settings = dict(
        COOKIES_ENABLED=True,
    )
    headers = {
        'Host': 'use1-prod-bk-gateway.rbictg.com',
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
        'Origin': 'https://www.bk.com',
        'Sec-Fetch-Site': 'cross-site',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Accept-Language': 'en-US,en;q=0.9',
        'Priority': 'u=1, i',
    }
    url = "https://use1-prod-bk-gateway.rbictg.com/graphql"


    def start_requests(self) -> Iterable[Request]:
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'access-control-request-headers': 'apollographql-client-name,apollographql-client-version,content-type,x-forter-token,x-platform-framework,x-session-id,x-ui-language,x-ui-platform,x-ui-region,x-ui-version,x-user-datetime',
            'access-control-request-method': 'GET',
            'connection': 'keep-alive',
            'host': 'use1-prod-bk-gateway.rbictg.com',
            'origin': 'https://www.bk.com',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'cross-site',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        }
        params = {
            'operationName': 'GetNearbyRestaurants',
            'variables': '{"input":{"pagination":{"first":20},"radiusStrictMode":false,"coordinates":{"searchRadius":32000,"userLat":40.754,"userLng":-73.999}}}',
            'extensions': '{"persistedQuery":{"version":1,"sha256Hash":"286ebe4fc947fc3c95c86d6e53ac7bd8a1cc2226ffa142e0b8a24be1f27c70b5"}}',
        }
        url = self.url + "?" + urlencode(params)
        yield Request(url, method="OPTIONS", callback=self.parse, headers=headers)

    
    def parse(self, response: Response):
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            params = {
                'operationName': 'GetNearbyRestaurants',
                'variables': json.dumps({
                    "input": {"pagination": {"first": 20}, "radiusStrictMode": False, "coordinates": {"searchRadius": 32000, "userLat": zipcode['latitude'], "userLng": zipcode['longitude']}}
                }, separators=(',', ':')),
                'extensions': json.dumps({
                    "persistedQuery": {"version": 1, "sha256Hash": "286ebe4fc947fc3c95c86d6e53ac7bd8a1cc2226ffa142e0b8a24be1f27c70b5"}
                }, separators=(',', ':'))
            }
            encoded_params = urlencode(params, quote_via=quote)
            url = f"https://use1-prod-bk-gateway.rbictg.com/graphql?{encoded_params}"
            yield scrapy.Request(url, callback=self.parse_stores, headers=self.headers)


    def parse_stores(self, response: Response):
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
                "hours": self._get_hours(node),
                "url": f"https://www.bk.com/store-locator/store/{node['_id']}",
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
                for key, day in key_day_mapping.items():
                    if hours_data[key+"Open"] is None or hours_data[key+"Close"] is None:
                        continue
                    hours[day] = {
                        "open": convert_to_12h_format(hours_data[key+"Open"].replace(":00", "")),
                        "close": convert_to_12h_format(hours_data[key+"Close"].replace(":00", ""))
                    }
            get_hours(node.get("curbsideHours", {}))
            if not hours:
                get_hours(node.get("deliveryHours", {}))
            if not hours:
                get_hours(node.get("diningRoomHours", {}))
            if not hours:
                get_hours(node.get("driveThruHours", {}))
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
