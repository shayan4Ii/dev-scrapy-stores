import scrapy
from scrapy_playwright.handler import PageMethod
from spidermon import data
from scrapy_store_scrapers.utils import *
from scrapy.http import JsonRequest



class DairyQueen(scrapy.Spider):
    name = "dairyqueen"
    custom_settings = dict(
        PLAYWRIGHT_ABORT_REQUEST = should_abort_request,
        PLAYWRIGHT_BROWSER_TYPE = "firefox",
        PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30*1000,
        PLAYWRIGHT_MAX_CONTEXTS = 1,
        DOWNLOAD_HANDLERS = {
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        USER_AGENT = None,
        PLAYWRIGHT_PROCESS_REQUEST_HEADERS = None,
        CONCURRENT_REQUESTS = 1,
    )


    def start_requests(self) -> Iterable[Request]:
        url = "https://www.dairyqueen.com/en-us/locations/"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        states = response.xpath("//a[contains(@aria-label, 'View')]/@href").getall()
        yield from response.follow_all(urls=states, callback=self.parse_state)


    def parse_state(self, response: Response):
        cities = response.xpath("//a[contains(@aria-label, 'View locations')]/@href").getall()
        yield from response.follow_all(urls=cities, callback=self.parse_city, meta={
            "playwright": True,
            "playwright_page_methods": [
                PageMethod("wait_for_selector", "//a[contains(@title, 'Report error')]")
            ]
        })

    
    def parse_city(self, response: Response):
        get_coordinates = lambda url: url.split("/@")[-1].split("/")[0].split(",")[:-1] if "/@" in url else (None, None)
        map_url = response.xpath("//a[contains(@title, 'Report error')]/@href").get()
        if map_url is None or "/@" not in map_url:
            self.logger.info(f"Coordinates are not found for store: {response.url}")
            return
        lat, lng = get_coordinates(map_url)
        json_data = {
            'operationName': 'NearbyStores',
            'variables': {
                'lat': float(lat),
                'lng': float(lng),
                'country': 'US',
                'searchRadius': 25,
            },
            'query': 'fragment StoreDetailFields on Store {\n  id\n  storeNo\n  address3\n  city\n  stateProvince\n  postalCode\n  country\n  latitude\n  longitude\n  phone\n  availabilityStatus\n  conceptType\n  restaurantId\n  utcOffset\n  supportedTimeModes\n  advanceOrderDays\n  storeHours(hoursFormat: "yyyy/MM/dd HH:mm") {\n    calendarType\n    ranges {\n      start\n      end\n      weekday\n      __typename\n    }\n    __typename\n  }\n  minisite {\n    webLinks {\n      isDeliveryPartner\n      description\n      url\n      __typename\n    }\n    hours {\n      calendarType\n      ranges {\n        start\n        end\n        weekday\n        __typename\n      }\n      __typename\n    }\n    amenities {\n      description\n      featureId\n      __typename\n    }\n    __typename\n  }\n  flags {\n    blizzardFanClubFlag\n    brazierFlag\n    breakfastFlag\n    cakesFlag\n    canPickup\n    comingSoonFlag\n    creditCardFlag\n    curbSideFlag\n    deliveryFlag\n    dispatchFlag\n    driveThruFlag\n    foodAndTreatsFlag\n    giftCardsFlag\n    mobileDealsFlag\n    mobileOrderingFlag\n    mtdFlag\n    ojQuenchClubFlag\n    onlineOrderingFlag\n    ojFlag\n    temporarilyClosedFlag\n    supportsManualFire\n    supportsSplitPayments\n    isCurrentlyOpen\n    __typename\n  }\n  labels {\n    key\n    value\n    __typename\n  }\n  __typename\n}\n\nquery NearbyStores($lat: Float!, $lng: Float!, $country: String!, $searchRadius: Int!) {\n  nearbyStores(\n    lat: $lat\n    lon: $lng\n    country: $country\n    radiusMiles: $searchRadius\n    limit: 50\n    first: 20\n    order: {distance: ASC}\n  ) {\n    pageInfo {\n      endCursor\n      hasNextPage\n      __typename\n    }\n    nodes {\n      distance\n      distanceType\n      store {\n        ...StoreDetailFields\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n',
        }
        yield JsonRequest(
            url="https://prod-api.dairyqueen.com/graphql/", 
            method="POST", 
            data=json_data, 
            callback=self.parse_stores,
            headers={'partner-platform': 'Web'},
            cb_kwargs={"source": response.url}
        )


    def parse_stores(self, response: Response, source: str):
        nodes = response.json().get("data", {}).get("nearbyStores", {}).get("nodes", [])
        for node in nodes:
            store = node.get("store")
            yield {
                "number": store.get("storeNo"),
                "location": {
                    "type": "Point",
                    "coordinates": [store.get("longitude"), store.get("latitude")]
                },
                "url": source,
                "raw": store,
                "address": self._get_address(store),
                "phone_number": store.get("phone"),
                "hours": self._get_hours(store.get("storeHours"))
            }


    def _get_address(self, store: Dict) -> str:
        try:
            address_parts = [
                store.get('address1'),
                store.get('address2'),
                store.get('address3'),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store['city']
            state = store['stateProvince']
            zipcode = store['postalCode']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0].strip()

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,",",")
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""



    def _get_hours(self, hours_data: List):
        days_mapping = {
            "mon": "monday",
            "tue": "tuesday",
            "wed": "wednesday",
            "thu": "thursday",
            "fri": "friday",
            "sat": "saturday",
            "sun": "sunday"
        }
        hours = {}
        try:
            for calendar_type in hours_data:
                for hour_range in calendar_type.get("ranges"):
                    weekday = hour_range.get("weekday").lower()
                    hours[days_mapping.get(weekday)] = {
                        "open": convert_to_12h_format(hour_range.get('start').split(" ")[-1]), 
                        "close": convert_to_12h_format(hour_range.get('end').split(" ")[-1]), 
                    }
                if hours:
                    break
            return hours
        except Exception as e:
            self.logger.error("Error getting hours: %s", e, exc_info=True)
            return {}
