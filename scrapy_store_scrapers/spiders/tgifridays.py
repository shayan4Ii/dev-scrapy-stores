import copy
import math
import re
import scrapy
from typing import Dict, Iterable
from scrapy.http import Response, Request
import requests
import json
from scrapy.exceptions import CloseSpider
from scrapy_store_scrapers.utils import *
from curl_cffi import requests



class TgiFridays(scrapy.Spider):
    name = "tgifridays"
    endpoint = "https://liveapi.yext.com/v2/accounts/me/answers/vertical/query"
    params = {
        'experienceKey': 'tgi-fridays-search',
        'v': '20220511',
        'version': 'PRODUCTION',
        'verticalKey': 'locations',
        'limit': '50',
    }


    def start_requests(self) -> Iterable[Request]:
        self.set_api_key()
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            payload = copy.copy(self.params)
            payload['location'] = f"{zipcode['latitude']},{zipcode['longitude']}"
            payload['filters'] = json.dumps({"builtin.location":{"$near":{"lat": zipcode['latitude'],"lng": zipcode['longitude'],"radius":2414016}}})
            yield scrapy.FormRequest(
                url=self.endpoint,
                formdata=payload,
                method="GET",
                callback=self.parse,
                cb_kwargs={"payload": payload}
            )


    def set_api_key(self) -> None:
        try:
            response = requests.get("https://locations.tgifridays.com/assets/static/global-77e8a5f9.js", impersonate="chrome")
        except Exception as e:
            self.logger.error("Error setting api-key: %s", e, exc_info=True)
            raise CloseSpider()
        else:
            match = re.search(r'(?:apiKey:")(.*?)(?:")', response.text)        
            if match:
                apikey = match.group(1)
                self.params['api_key'] = apikey
            else:
                raise CloseSpider("API key not found!")
        
        
    def parse(self, response: Response, **kwargs: Dict) -> Iterable[Union[Dict, Request]]:
        yield from self.parse_stores(response)
        obj = json.loads(response.text)

        results_count = obj['response']['resultsCount']
        total_pages = math.ceil(results_count / 50) - 1
        offset = 50
        for _ in range(total_pages):
            offset += 50
            kwargs['payload'].update({"offset": f"{offset}"})
            yield scrapy.FormRequest(
                url=self.endpoint,
                formdata=kwargs['payload'],
                method="GET",
                callback=self.parse_stores
            )


    def parse_stores(self, response: Response, **kwargs: Dict) -> Iterable[Dict]:
        obj = json.loads(response.text)
        for store in obj['response']['results']:
            data = store['data']
            store_id = str(data['id'])
            item = {
                "number": store_id,
                "name": data['name'],
                "address": self._get_address(data['address']),
                "location": {
                    "type": "Point",
                    "coordinates": [data.get('geocodedCoordinate',{}).get('longitude'), data.get('geocodedCoordinate',{}).get('latitude')]
                },
                "phone_number": data['mainPhone'],
                "hours": self._get_hours(data['hours']),
                "services": data['pickupAndDeliveryServices'],
                "url": data['website'],
                "raw": data
            }
            yield item


    def _get_address(self, address_obj: Dict) -> str:
        try:
            address_parts = [
                address_obj.get("line1", ""),
                address_obj.get("line2", ""),
                address_obj.get("line3", "")
            ]
            street = ", ".join(filter(None, address_parts))
            city = address_obj.get("city", "")
            state = address_obj.get("region", "")
            zipcode = address_obj.get("postalCode", "")
            city_state_zip = f"{city}, {state} {zipcode}".strip()
            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_hours(self, hours_obj: Dict) -> Dict:
        new_item = {}
        try:
            for day in hours_obj:
                if "openIntervals" not in hours_obj[day]:
                    self.logger.warning("Hours are not available!")
                    return {}
                day_hours = hours_obj[day].get('openIntervals')[0]
                new_item[day] = {
                    "open": convert_to_12h_format(day_hours['start']),
                    "close": convert_to_12h_format(day_hours['end'])
                }
            return new_item
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}


    