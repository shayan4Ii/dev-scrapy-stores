import scrapy
from scrapy_store_scrapers.utils import *



class WhiteCastle(scrapy.Spider):
    name = "whitecastle"


    def start_requests(self) -> Iterable[Request]:
        zipcodes = load_zipcode_data("data/zipcode_lat_long.json")
        for zipcode in zipcodes:
            url = f"https://www.whitecastle.com/api/vtl/get-nearest-location?lat={zipcode['latitude']}&long={zipcode['longitude']}&distance=20&count=200"
            yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        stores = json.loads(response.text)['results']
        for store in stores:
            number = store.get("storeNumber")
            if number == "TBD":
                # store with TBD contain open hour but not close so it will be dropped, hence skipping it beforehand
                self.logger.info(f"number not available for store: {response.url}")
                continue
            yield {
                "number": number,
                "name": store.get("name"),
                "location": {
                    "type": "Point",
                    "coordinates": [float(store.get("lng")), float(store.get("lat"))]
                },
                "url": f"https://www.whitecastle.com/locations/{number}" if number else "",
                "address": self._get_address(store),
                "phone_number": store.get("telephone"),
                "hours": self._get_hours(store),
                "raw": store,
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store.get("address")
            ]
            street = ", ".join(filter(None, address_parts))

            city = store.get("city")
            state = store.get("state")
            zipcode = store.get("zip")
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""


    def _get_hours(self, store: Dict) -> dict:
        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours = {}
        try:
            for day in days:
                key = f"{day}Hours"
                value = store.get(key)
                if not value:
                    continue
                if "24 hr" in value:
                    hours[day] = {
                        "open": "12:00 am",
                        "close": "11:59 pm"
                    }
                elif "am" in value.lower() or "pm" in value.lower():
                    value = value.lower().replace("am", " am").replace("pm"," pm")
                    hours[day] = {
                        "open": convert_to_12h_format(value.split("-")[0].strip()),
                        "close": convert_to_12h_format(value.split("-")[-1].strip())
                    }
                else:
                    self.logger.info(store)
                    break
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
