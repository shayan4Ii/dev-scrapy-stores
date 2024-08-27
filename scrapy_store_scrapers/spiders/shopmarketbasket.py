from typing import Generator

import scrapy
from scrapy.http import Response, Request

class ShopmarketbasketSpider(scrapy.Spider):
    name = "shopmarketbasket"
    allowed_domains = ["www.shopmarketbasket.com"]
    start_urls = ["https://www.shopmarketbasket.com/store-locations-rest"]

    NAME_XPATH = "//section[@class='flyer-header']/h2/span/text()"

    PHONE_XPATH = "//div[contains(@class, 'field--name-field-phone-number')]/div/a/text()"
    SERVICES = "//div[@class='departments']/ul/li/text()"

    MON_SAT_HOURS_XPATH = "normalize-space(//div[contains(@class,'field--name-field-hours')]/div/p[contains(., 'Monday - Saturday')])"
    SUN_HOURS_XPATH = "normalize-space(//div[contains(@class,'field--name-field-hours')]/div/p[contains(., 'Sunday')])"

    ADDRESS_CONTAINER = "//p[@class='address']"
    STREET_ADDRESS_XPATH = ".//span[@class='address-line1']/text()"
    CITY_XPATH = ".//span[@class='locality']/text()"
    STATE_XPATH = ".//span[@class='administrative-area']/text()"
    ZIP_XPATH = ".//span[@class='postal-code']/text()"

    MON_SAT_HOURS_RANGE_REGEX = r"Monday - Saturday(.*)"
    SUN_HOURS_REGEX = r"Sunday(.*)"

    def parse(self, response: Response) -> Generator[Request, None, None]:
        stores = response.json()
        for store in stores:
            yield response.follow(store["path"], callback=self.parse_store, cb_kwargs={"geo_loc_str": store["field_geolocation"]})

    def parse_store(self, response: Response, geo_loc_str: str) -> dict:
        
        store_info = {}
        store_info["name"] = self.get_name(response)
        store_info["address"] = self.get_address(response)
        store_info["phone"] = self.get_phone(response)
        store_info["location"] = self.get_location(geo_loc_str)
        store_info["hours"] = self.get_hours(response)
        store_info["services"] = self.get_services(response)

        return store_info


    def get_name(self, response: Response) -> str: 
        return response.xpath(self.NAME_XPATH).get()
    
    def get_phone(self, response: Response) -> str:
        return response.xpath(self.PHONE_XPATH).get()
    
    def get_services(self, response: Response) -> list:
        return response.xpath(self.SERVICES).getall()

    def get_hours(self, response: Response) -> dict[str, dict[str, str]]:
        hours_info = {}

        mon_sat_hours_range_text = response.xpath(self.MON_SAT_HOURS_XPATH).re_first(self.MON_SAT_HOURS_RANGE_REGEX,"").strip()
        sun_hours = response.xpath(self.SUN_HOURS_XPATH).re_first(self.SUN_HOURS_REGEX,"").strip()

        mon_sat_open, mon_sat_close = mon_sat_hours_range_text.split("-")
        sun_open, sun_close = sun_hours.split("-")

        for day in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]:
            if day == "sunday":
                hours_info[day] = {
                    "open": sun_open.strip(),
                    "close": sun_close.strip()
                }
            else:
                hours_info[day] = {
                    "open": mon_sat_open.strip(),
                    "close": mon_sat_close.strip()
                }
        return hours_info
    
    def get_address(self, response: Response) -> str:
        try:
            address_container = response.xpath(self.ADDRESS_CONTAINER)
            street = address_container.xpath(self.STREET_ADDRESS_XPATH).get()
            city = address_container.xpath(self.CITY_XPATH).get()
            state = address_container.xpath(self.STATE_XPATH).get()
            postal_code = address_container.xpath(self.ZIP_XPATH).get()

            formatted_address = f"{street}, {city}, {state} {postal_code}"
            return formatted_address
        except Exception as e:
            self.logger.error(f"Error extracting address: {str(e)}")
            return ""
        

    def get_location(self, geo_loc_str: str) -> dict:
        try:
            latitude, longitude = geo_loc_str.split(",")
            
            # Convert latitude and longitude to float if they exist
            if latitude is not None and longitude is not None:
                try:
                    longitude = float(longitude)
                    latitude = float(latitude)
                    return {
                        "type": "Point",
                        "coordinates": [longitude, latitude]  # GeoJSON uses [longitude, latitude] order
                    }
                except ValueError:
                    self.logger.warning(f"Invalid latitude or longitude values: {latitude}, {longitude}")
                    return None
            else:
                self.logger.warning("Missing latitude or longitude")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting location: {str(e)}")
            return None