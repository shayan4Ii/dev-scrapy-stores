import scrapy
from scrapy_store_scrapers.utils import *



class Tidalwaveautospa(scrapy.Spider):
    name = "tidalwaveautospa"


    def start_requests(self) -> Iterable[Request]:
        url = "https://www.tidalwaveautospa.com/locations/"
        yield scrapy.Request(url, callback=self.parse)


    def parse(self, response: Response):
        location_links = response.xpath("//div[@id='accordionContent']//a[text()='View Location']/@href").getall()
        for link in location_links:
            yield scrapy.Request(link, callback=self.parse_location)

    def parse_location(self, response: Response):
        
        partial_item = {}
        partial_item["hours"] = self._get_hours(response)
        partial_item["url"] = response.url

        js_url = response.xpath("//script[contains(@src,'embed/v6')]/@src").get()

        if not js_url:
            self.logger.error(f"JS URL not found for {response.url}")
            return
        
        yield scrapy.Request(js_url, callback=self.parse_js, cb_kwargs={"partial_item": partial_item})

    def parse_js(self, response: Response, partial_item: Dict):
        js_parsed_objects = json.loads(response.text.split("var bizDataResp =")[-1].split("var locale")[0].strip().rstrip(";"))["businessLocations"]
        if len(js_parsed_objects) != 1:
            self.logger.error(f"Invalid JS data: {partial_item['url']} {js_parsed_objects}")
            return
        
        location = js_parsed_objects[0]

        js_parsed_info = {
            "number": f"{location['businessId']}",
            "name": location['location'],
            "address": self._get_address(location),
            "location": {
                "type": "Point",
                "coordinates": [location['longitude'], location['latitude']]
            },
            "phone_number": location['phone'],
            "raw": location
        }

        item = {**js_parsed_info, **partial_item}
        
        yield item

    def _get_address(self, address: Dict) -> str:
        try:
            address_parts = [
                address['address1'],
            ]
            street = ", ".join(filter(None, address_parts))

            city = address['city']
            state = address['state']
            zipcode = address['zipcode']
            if "-" in zipcode:
                zipcode = zipcode.split("-")[0]

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip])).replace(",,", ",").strip()
        except Exception as e:
            self.logger.error("Error getting address: %s", e, exc_info=True)
            return ""
        
    def _get_location(self, response) -> dict:
        """Extract and format location coordinates."""
        try:
            lat_long_text = response.xpath('//a[@class="convertDirections h6"]/@href').re_first(r'destination=(.*)')
            latitude, longitude = lat_long_text.split(',')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning(f"Missing latitude or longitude for store")
            return {}
        except ValueError as e:
            self.logger.warning(f"Invalid latitude or longitude values: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting location: {e}", exc_info=True)
        return {}
        
    def _get_hours(self, response: Response) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours_texts = response.xpath("//span[contains(text(), 'Hours')]/following-sibling::text()").getall()
            hours_texts = [text.strip() for text in hours_texts if text.strip()]

            if len(hours_texts) == 2 and "day" in hours_texts[1]:
                hours_texts = hours_texts[::-1]
            
            hours = " ".join(hours_texts).strip()
            if not hours:
                self.logger.warning(f"No hours found for store {response.url}")
                return {}

            if 'coming' in hours.lower():
                self.logger.warning(f"Store is not open yet: {hours}, {response.url}")
                return {}
            hours_example = HoursExample()
            normalized_hours = hours_example.normalize_hours_text(hours)
            return hours_example._parse_business_hours(normalized_hours)
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

