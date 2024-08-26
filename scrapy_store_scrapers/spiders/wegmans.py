import json
from datetime import datetime
from typing import Generator

import scrapy
from scrapy.http import Request, Response

class WegmansSpider(scrapy.Spider):
    name = "wegmans"
    allowed_domains = ["www.wegmans.com"]
    start_urls = ["https://shop.wegmans.com/api/v2/stores"]

    def parse(self, response: Response) -> Generator[Request, None, None]:
        """Parse the response and yield new requests for store pages."""
        stores = response.json()["items"]

        for raw_store_info in stores:
            url = raw_store_info["external_url"]
            yield Request(url, callback=self.parse_store, cb_kwargs={"raw_store_info": raw_store_info})

        
    def parse_store(self, response: Response, raw_store_info: dict) -> dict:
        """Parse the store page and yield store information."""
        
        store_data = {}

        store_data["name"] = response.xpath("//div[@id='storeTitle']/h1/text()").get()
        store_data["number"] = response.xpath("//span[@id='store-number']/text()").get()
        store_data["address"] = self.get_address(raw_store_info)
        store_data["phone"] = self.get_phone_number(raw_store_info)
        store_data["hours"] = self.get_hours(response)
        store_data["services"] = self.get_services(response)
        store_data["location"] = self.get_location(raw_store_info)

        return store_data
    
    @staticmethod
    def get_address(raw_store_info: dict) -> str:
        """Extract the address from the raw store information for Wegmans."""
        
        address = raw_store_info.get('address', {})
        
        street = address.get('address1', '')
        city = address.get('city', '')
        state = address.get('province', '')
        postal_code = address.get('postal_code', '')
        
        formatted_address = f"{street}, {city}, {state} {postal_code}"
        
        # Remove any double spaces and strip leading/trailing whitespace
        formatted_address = ' '.join(formatted_address.split())
        
        return formatted_address

    @staticmethod
    def get_phone_number(raw_store_info: dict) -> str:
        """Extract the phone number from the raw store information for Wegmans."""
        
        return raw_store_info.get('phone_number', '')
    
    @staticmethod
    def get_location(raw_store_info: dict) -> dict:
        """
        Extract the location (longitude and latitude) from the raw store information
        and return it in GeoJSON Point format.
        """
        
        location = raw_store_info.get('location', {})
        
        latitude = location.get('latitude')
        longitude = location.get('longitude')
        
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
                # If conversion to float fails, return None
                return None
        else:
            # If either latitude or longitude is missing, return None
            return None

    def get_services(self, response: Response) -> list:
        """Extract the services offered by the store from the response."""
        
        services = []

        service_elems = response.xpath("//div[@class='storeServices']//*[contains(@class,'-header')]")
        for service_elem in service_elems:
            service_name = service_elem.xpath("normalize-space(.)").get()
            if service_name:
                services.append(service_name.strip())

        return services

    def get_hours(self, response: Response) -> dict:
        """Extract the store hours from the response."""
        
        script_text = response.xpath("//script[@id='localinfo']/text()").get()

        if not script_text:
            return

        # Extract the JSON data from the script tag
        json_data = json.loads(script_text)


        hours = {}

        hour_info = json_data["openingHoursSpecification"][0]

        for day in hour_info["dayOfWeek"]:
            hours[day.lower()] = {
                "open": self.convert_to_12h_format(hour_info["opens"]),
                "close": self.convert_to_12h_format(hour_info["closes"])
            }        
        
        return hours

    @staticmethod
    def convert_to_12h_format(time_str):
        t = datetime.strptime(time_str, '%H:%M').time()
        return t.strftime('%I:%M %p').lstrip('0')

    
