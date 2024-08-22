import scrapy
import phpserialize

class StaterbrosSpider(scrapy.Spider):
    name = "staterbros"
    allowed_domains = ["www.staterbros.com"]
    start_urls = ["https://www.staterbros.com/wp-json/api/stores/"]

    STORE_PAGE_URL_FORMAT = "https://www.staterbros.com/stores/{}"

    NAME_XPATH = "//h1[@itemprop='headline']/text()"
    ADDRESS_XPATH = "//div[contains(@class, 'elementor-widget-heading') and .//h3[contains(./text(), 'Address')]]/following-sibling::div[contains(@class, 'elementor-widget-text-editor')]/div/text()"
    PHONE_XPATH = "//div[contains(@class, 'elementor-widget-heading') and .//h3[contains(./text(), 'Phone Number')]]/following-sibling::div[contains(@class, 'elementor-widget-text-editor')]/div/text()"
    MON_SUN_HOURS_XPATH = "//div[@class='elementor-icon-box-content'][contains(.//span,'Store Hours')]//p[contains(., 'MON-SUN')]/text()"
    SERVICES_XPATH = "//div[@class='elementor-widget-wrap elementor-element-populated' and .//span[contains(text(),'Store Features')]]//div[@class='elementor-widget-container']/p/text()"
    def parse(self, response):
        data = response.json()
        stores_lat_lng = self.get_store_lat_lng(data)

        for store_no, lat_lng_dict in stores_lat_lng.items():
            yield scrapy.Request(self.STORE_PAGE_URL_FORMAT.format(store_no), callback=self.parse_store_page, cb_kwargs={"store_no": store_no,"lat": lat_lng_dict["lat"], "lng": lat_lng_dict["lng"]})
            
        
    def parse_store_page(self, response, store_no, lat, lng):
        store_data = {}
        store_data["number"] = store_no
        store_data["name"] = response.xpath(self.NAME_XPATH).re_first('#\d+(.*)').strip()
        store_data["address"] = self.clean_text(response.xpath(self.ADDRESS_XPATH).get())
        store_data["phone"] = self.clean_text(response.xpath(self.PHONE_XPATH).get())
        store_data["services"] = self.get_services(response)
        store_data["location"] = self.get_location(lat, lng)
        store_data["hours"] = self.get_hours(response)
        yield store_data




    def get_hours(self, response):
        hours = response.xpath(self.MON_SUN_HOURS_XPATH).get()
        print(hours)
        if not hours:
            return {}
        
        hours = hours.replace(" ", "").lower()

        # split by - or  – 

        if "–" in hours:
            open_time, close_time = hours.split("–")
        else:
            open_time, close_time = hours.split("-")

        hours_dict = {}

        for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
            hours_dict[day] = {
                "open": open_time,
                "close": close_time
            }
        
        return hours_dict
        
    def get_location(self, lat, long):
        return {
            "type": "Point",
            "coordinates": [float(long), float(lat)]
        }


    def get_services(self, response):
        services = response.xpath(self.SERVICES_XPATH).getall()
        return [self.clean_text(service) for service in services if self.clean_text(service)]

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and strip whitespace from text"""
        return text.strip() if text else ""


    @staticmethod
    def get_store_lat_lng(data):
        stores_lat_lng = {}
        for data_dict in data:
            
            if data_dict["store_number"] not in stores_lat_lng:
                stores_lat_lng[data_dict["store_number"]] = {}
            
            if data_dict["meta_key"] == "map":
                meta_value = data_dict["meta_value"]

                meta_value_dict = phpserialize.loads(meta_value.encode(), decode_strings=True)

                stores_lat_lng[data_dict["store_number"]]["lat"] = meta_value_dict["lat"]
                stores_lat_lng[data_dict["store_number"]]["lng"] = meta_value_dict["lng"]

        return stores_lat_lng



