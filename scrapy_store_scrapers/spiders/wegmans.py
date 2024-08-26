import json
import logging
from datetime import datetime
from typing import Generator, Optional

import scrapy
from scrapy.http import Request, Response

class WegmansSpider(scrapy.Spider):
    name = "wegmans"
    allowed_domains = ["www.wegmans.com"]
    start_urls = ["https://shop.wegmans.com/api/v2/stores"]
    store_items = {}

    def parse(self, response: Response) -> Generator[Request, None, None]:
        """Parse the response and yield new requests for store pages."""
        try:
            stores = response.json()["items"]
        except KeyError:
            self.logger.error("Failed to parse JSON response: 'items' key not found")
            return
        except json.JSONDecodeError:
            self.logger.error("Failed to decode JSON response")
            return

        for raw_store_info in stores:
            try:
                retailer_store_id = raw_store_info["retailer_store_id"]
                self.store_items[retailer_store_id] = raw_store_info
            except KeyError:
                self.logger.warning(f"Missing 'retailer_store_id' for store: {raw_store_info}")
        
        yield Request('https://www.wegmans.com/stores/', callback=self.parse_stores)

    def parse_stores(self, response: Response) -> Generator[dict, None, None]:
        """Parse the stores page and yield store information."""
        try:
            store_links = response.xpath("//div[@class='wpb_wrapper']//a[contains(@href,'/stores/')]/@href")
            for store_url in store_links:
                yield response.follow(store_url, callback=self.parse_store)
        except Exception as e:
            self.logger.error(f"Error parsing stores page: {str(e)}")

    def parse_store(self, response: Response) -> Optional[dict]:
        """Parse the store page and yield store information."""
        try:
            store_data = {}

            store_data["name"] = response.xpath("//div[@id='storeTitle']/h1/text()").get()
            store_data["number"] = response.xpath("//span[@id='store-number']/text()").get()

            # Get the raw store information from the store_items dictionary
            raw_store_info = self.store_items.get(store_data["number"])

            store_data["address"] = self.get_address(raw_store_info)
            store_data["phone"] = self.get_phone_number(raw_store_info)
            store_data["hours"] = self.get_hours(response)
            store_data["services"] = self.get_services(response)
            store_data["location"] = self.get_location(raw_store_info)
            store_data["raw_dict"] = raw_store_info

            return store_data
        except Exception as e:
            self.logger.error(f"Error parsing store data: {str(e)}")
            return None

    @staticmethod
    def get_address(raw_store_info: dict) -> str:
        """Extract the address from the raw store information for Wegmans."""
        try:
            address = raw_store_info.get('address', {})
            
            street = address.get('address1', '')
            city = address.get('city', '')
            state = address.get('province', '')
            postal_code = address.get('postal_code', '')
            
            formatted_address = f"{street}, {city}, {state} {postal_code}"
            
            # Remove any double spaces and strip leading/trailing whitespace
            formatted_address = ' '.join(formatted_address.split())
            
            return formatted_address
        except Exception as e:
            logging.error(f"Error extracting address: {str(e)}")
            return ""

    @staticmethod
    def get_phone_number(raw_store_info: dict) -> str:
        """Extract the phone number from the raw store information for Wegmans."""
        try:
            return raw_store_info.get('phone_number', '')
        except Exception as e:
            logging.error(f"Error extracting phone number: {str(e)}")
            return ""

    @staticmethod
    def get_location(raw_store_info: dict) -> Optional[dict]:
        """
        Extract the location (longitude and latitude) from the raw store information
        and return it in GeoJSON Point format.
        """
        try:
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
                    logging.warning(f"Invalid latitude or longitude values: {latitude}, {longitude}")
                    return None
            else:
                logging.warning("Missing latitude or longitude")
                return None
        except Exception as e:
            logging.error(f"Error extracting location: {str(e)}")
            return None

    def get_services(self, response: Response) -> list:
        """Extract the services offered by the store from the response."""
        try:
            services = []

            service_elems = response.xpath("//div[@class='storeServices']//*[contains(@class,'-header')]")
            for service_elem in service_elems:
                service_name = service_elem.xpath("normalize-space(.)").get()
                if service_name:
                    services.append(service_name.strip())

            return services
        except Exception as e:
            self.logger.error(f"Error extracting services: {str(e)}")
            return []

    def get_hours(self, response: Response) -> Optional[dict]:
        """Extract the store hours from the response."""
        try:
            script_text = response.xpath("//script[@id='localinfo']/text()").get()

            if not script_text:
                self.logger.warning("No script text found for store hours")
                return self.extract_hours_from_html(response)

            # Extract the JSON data from the script tag
            json_data = json.loads(script_text)

            hours = {}

            opening_hours_spec = json_data.get("openingHoursSpecification")

            if not opening_hours_spec:
                self.logger.warning("No openingHoursSpecification found in JSON data")
                return self.extract_hours_from_html(response)

            if isinstance(opening_hours_spec, list):
                if len(opening_hours_spec) > 1:
                    self.logger.warning("Multiple openingHoursSpecification found")
                hour_info = opening_hours_spec[0]
            elif isinstance(opening_hours_spec, dict):
                hour_info = opening_hours_spec
            else:
                self.logger.error("Unexpected type for openingHoursSpecification")
                return self.extract_hours_from_html(response)

            for day in hour_info["dayOfWeek"]:
                hours[day.lower()] = {
                    "open": self.convert_to_12h_format(hour_info["opens"]),
                    "close": self.convert_to_12h_format(hour_info["closes"])
                }        
            
            return hours
        except json.JSONDecodeError:
            self.logger.error("Failed to decode JSON data for store hours")
            return self.extract_hours_from_html(response)
        except KeyError as e:
            self.logger.error(f"Missing key in JSON data for store hours: {str(e)}")
            return self.extract_hours_from_html(response)
        except Exception as e:
            self.logger.error(f"Error extracting store hours: {str(e)}")
            return self.extract_hours_from_html(response)

    def extract_hours_from_html(self, response: Response) -> Optional[dict]:
        """Extract store hours from HTML when JSON data is not available."""
        try:
            hours_text = response.xpath("//div[@id='storeHoursID']/text()").get()
            if not hours_text:
                self.logger.warning("No store hours found in HTML")
                return None

            hours_text = hours_text.strip()
            self.logger.info(f"Extracting hours from HTML: {hours_text}")

            # Parse the hours text
            if "Open" in hours_text and "to" in hours_text:
                parts = hours_text.split("Open")[1].split("to")
                open_time = parts[0].strip()
                close_time = parts[1].split(",")[0].strip()

                # Standardize the times
                open_time = self.standardize_time(open_time)
                close_time = self.standardize_time(close_time)

                # Create a dictionary for all days of the week
                days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                hours = {day: {"open": open_time, "close": close_time} for day in days}

                return hours
            else:
                self.logger.warning(f"Unexpected format for store hours: {hours_text}")
                return None

        except Exception as e:
            self.logger.error(f"Error extracting hours from HTML: {str(e)}")
            return None

    @staticmethod
    def standardize_time(time_str: str) -> str:
        """Standardize time format to HH:MM AM/PM."""
        time_str = time_str.strip().upper()
        
        # Handle special case for Midnight
        if time_str == "MIDNIGHT":
            return "12:00 AM"
        
        # Handle special case for Noon
        if time_str == "NOON":
            return "12:00 PM"
        
        # Remove any periods from AM/PM
        time_str = time_str.replace(".", "")
        
        # Add space before AM/PM if missing
        if time_str.endswith("AM") or time_str.endswith("PM"):
            if not time_str[-3].isspace():
                time_str = time_str[:-2] + " " + time_str[-2:]
        
        # Try parsing the time
        try:
            # Try parsing with minutes
            parsed_time = datetime.strptime(time_str, "%I:%M %p")
        except ValueError:
            try:
                # Try parsing without minutes
                parsed_time = datetime.strptime(time_str, "%I %p")
            except ValueError:
                logging.error(f"Unable to parse time: {time_str}")
                return time_str  # Return original string if parsing fails
        
        # Format the time consistently
        return parsed_time.strftime("%I:%M %p").lstrip("0")
    
    @staticmethod
    def convert_to_12h_format(time_str):
        try:
            t = datetime.strptime(time_str, '%H:%M').time()
            return t.strftime('%I:%M %p').lstrip('0')
        except ValueError:
            logging.error(f"Invalid time format: {time_str}")
            return time_str

