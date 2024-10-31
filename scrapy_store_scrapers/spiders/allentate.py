import scrapy
from typing import Iterable, Dict, List
import json
from scrapy.http import Response, JsonRequest
from copy import deepcopy


class Allentate(scrapy.Spider):
    name = "allentate"
    payload = {
        'SouthLat': '9.847128466400651',
        'WestLng': '-122.8291015625',
        'NorthLat': '59.73842178504959',
        'EastLng': '-73.17089843750001',
        'Location': 'My Current Location',
        'Radius': '100',
        'OrderBy': 'Closest',
    }
    store_processed = set()


    def start_requests(self) -> Iterable[scrapy.Request]:
        url = "https://www.allentate.com/Office/MapOffices"
        yield scrapy.Request(
            url=url,
            callback=self.parse,
            method="POST",
            body=json.dumps(self.payload)
        )


    def parse(self, response: Response) -> Iterable[Dict]:
        stores = json.loads(response.text).get("Properties", [])
        partial_items = []
        for store in stores:
            store_id = store.get("MlsNumber")
            if store_id in self.store_processed:
                continue
            self.store_processed.add(store_id)
            partial_item = {
                "number": store_id,
                "name": store.get("OfficeName"),
                "address": self._get_address(store),
                "location": self._get_location(store),
                "url": f"https://www.allentate.com/Office/Detail/{store.get('OfficeName')}/{store.get('MlsNumber')}",
                "raw": store
            }
            partial_items.append(partial_item)


        payload = {"propertyKeys": []}
        for idx, item in enumerate(partial_items, start=1):
            payload["propertyKeys"].append({"MlsName": "Office", "MlsNumber": f"{item['number']}"})
            if idx % 10 == 0:
                yield JsonRequest(
                    url="https://www.allentate.com/Office/MapList",
                    callback=self.parse_store,
                    method="POST",
                    data=deepcopy(payload),
                    cb_kwargs={"partial_items": partial_items},
                )
                payload['propertyKeys'].clear()
        yield JsonRequest(
            url="https://www.allentate.com/Office/MapList",
            callback=self.parse_store,
            method="POST",
            data=deepcopy(payload),
            cb_kwargs={"partial_items": partial_items},
        )


    def parse_store(self, response: Response, partial_items: List[Dict]):
        for store in partial_items:
            store_id = store['number']
            phone = response.xpath(f"//tr//a[contains(@href, '{store_id}')]/ancestor::tr/preceding-sibling::tr//a/text()").get()
            if phone:
                store.update({"phone_number": phone})
                yield store


    def _get_address(self, address_obj: Dict) -> str:
        try:
            address_parts = [
                address_obj.get("Address", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = address_obj.get("City", "")
            state = address_obj.get("StateProv", "")
            zipcode = address_obj.get("ZipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_location(self, store: Dict) -> Dict:
        try:
            lat = float(str(store.get("Latitude", 0)))
            lon = float(str(store.get("Longitude", 0)))
            return {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        except (ValueError, TypeError) as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}


