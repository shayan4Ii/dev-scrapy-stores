from typing import Iterable
import scrapy
from scrapy_store_scrapers.utils import *
from scrapy.http import JsonRequest



class SweetGreen(scrapy.Spider):
    name = "sweetgreen"


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            payload = {
                "operationName": "LocationsSearchBySearchStringWithDisclosureFields",
                "query": "query LocationsSearchBySearchStringWithDisclosureFields($searchString: String!, $showHidden: Boolean) {\n  searchLocationsByString(searchString: $searchString, showHidden: $showHidden) {\n    score\n    location {\n      ...LocationDetails\n      outpostPriceDifferentiationEnabled\n      __typename\n    }\n    __typename\n  }\n}\nfragment LocationDetails on StoreLocation {\n  id\n  name\n  latitude\n  longitude\n  slug\n  address\n  city\n  state\n  zipCode\n  isOutpost\n  phone\n  storeHours\n  flexMessage\n  enabled\n  acceptingOrders\n  notAcceptingOrdersReason\n  imageUrl\n  hidden\n  showWarningDialog\n  warningDialogDescription\n  warningDialogTimeout\n  warningDialogTitle\n  __typename\n}",
                "variables": { "searchString": f"{zipcode}" }
            }
            yield JsonRequest(
                url="https://order.sweetgreen.com/graphql",
                data=payload,
                callback=self.parse,
                method="POST"
            )


    def parse(self, response: Response):
        locations = response.json()['data']['searchLocationsByString']
        for location in locations:
            location = location['location']
            yield {
                "number": f"{location['id']}",
                "name": location['name'],
                "address": self._get_address(location),
                "location": self._get_location(location),
                "phone_number": location.get("phone"),
                "hours": self._get_hours(location),
                "url": f"https://order.sweetgreen.com/{location['slug']}/menu",
                # "services": [], not available
                "raw": location
            }


    def _get_address(self, location: Dict) -> str:
        try:
            address_parts = [
                location['address'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = location.get("city", "")
            state = location.get("state", "")
            zipcode = location.get("zipCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        

    def _get_location(self, location: Dict) -> Dict:
        try:
            return {
                "type": "Point",
                "coordinates": [location['longitude'], location['latitude']]
            }
        except Exception as e:
            self.logger.error("Error getting location: %s", e, exc_info=True)
            return {}
        

    def _get_hours(self, location: Dict) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = location.get("storeHours", "")
            if not hours:
                self.logger.warning(f"No hours found for store {location.get('name', 'Unknown')}")
                return {}

            hours_example = HoursExample()
            normalized_hours = hours_example.normalize_hours_text(hours)
            return hours_example._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}