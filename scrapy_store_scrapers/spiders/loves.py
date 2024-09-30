import scrapy


class LovesSpider(scrapy.Spider):
    name = "loves"
    allowed_domains = ["www.loves.com"]
    api_format_url = 'https://www.loves.com/api/sitecore/StoreSearch/SearchStoresWithDetail?pageNumber={}&top=50&lat=38.130353038797594&lng=-97.81370249999999'

    def start_requests(self):
        yield scrapy.Request(url=self.api_format_url.format(0), callback=self.parse, cb_kwargs=dict(page=0))

    def parse(self, response, page):
        data = response.json()
        for store in data:
            yield self._parse_store(store)

        if data:
            yield scrapy.Request(url=self.api_format_url.format(page + 1), callback=self.parse, cb_kwargs=dict(page=page + 1))

    def _parse_store(self, store):
        parsed_store = {}

        parsed_store['number'] = str(store['Number'])
        parsed_store['name'] = store['PreferredName']
        parsed_store['phone_number'] = store['MainPhone']

        parsed_store['address'] = self._get_address(store)
        parsed_store['location'] = self._get_location(store)
        parsed_store['services'] = self._get_services(store)
        parsed_store['hours'] = self._get_hours(store)
        parsed_store['url'] = "https://www.loves.com/en/location-and-fuel-price-search/locationsearchresults#?state=All&city=All&highway=All"
        parsed_store['raw'] = store

        return parsed_store

    def _get_hours(self, store_info: dict):
        """Extract and format store hours."""
        try:
            days = ["monday", "tuesday", "wednesday",
                    "thursday", "friday", "saturday", "sunday"]
            business_hours = store_info.get("BusinessHours", [])
            loc_business_hours = store_info.get("LocationBusinessHours", [])

            hours = {}
            if business_hours:
                for day in business_hours:
                    day_name = day["SmaFieldName"].lower()
                    hours[day_name] = self._get_open_close(day["FieldValue"])
            elif loc_business_hours:
                for hours_info in loc_business_hours:
                    if hours_info["SmaFieldName"] == "storehours":
                        for day_name in days:
                            hours[day_name] = self._get_open_close(
                                hours_info["FieldValue"])

            if not hours:
                self.logger.warning(
                    "No hours found for store: %s", store_info.get("Number"))

            return hours
        except Exception as e:
            self.logger.error("Error extracting hours: %s", e, exc_info=True)
            return

    def _get_open_close(self, hours_text):
        """Extract open and close times from hours text."""
        try:
            hours_text = hours_text.strip().lower().replace('.', '')
            if hours_text == "open 24-hours":
                return {
                    "open": "12:00 am",
                    "close": "11:59 pm"
                }

            if "–" in hours_text:
                open_time, close_time = hours_text.split("–")
            else:
                open_time, close_time = hours_text.split("-")

            open_time = self._convert_time_format(open_time.strip())
            close_time = self._convert_time_format(close_time.strip())

            return {
                "open": open_time,
                "close": close_time
            }
        except ValueError as error:
            self.logger.warning("Invalid open/close times: %s, %s",
                                hours_text, error)
        except Exception as error:
            self.logger.error(
                "Error extracting open/close times: %s", error, exc_info=True)
        return {}

    def _get_services(self, store_info: dict):
        """Extract and format services."""
        try:
            services = [service['FieldName']
                        for service in store_info["Amenities"]]

            return services

        except Exception as e:
            self.logger.error(
                "Error extracting services: %s", e, exc_info=True)
            return []

    def _get_address(self, store_info: dict) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("Address", ""),
                # store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("City", "")
            state = store_info.get("State", "")
            zipcode = store_info.get("Zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(
                    f"Missing address information for store: {store_info.get('Number', 'Unknown')}")
            return full_address
        except Exception as e:
            self.logger.error(
                f"Error formatting address for store {store_info.get('Number', 'Unknown')}: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info: dict):
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('Latitude')
            longitude = store_info.get('Longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(
                "Missing latitude or longitude for store: %s", store_info.get("location_id"))
            return None
        except ValueError as error:
            self.logger.warning(
                "Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s",
                              error, exc_info=True)
        return None

    @staticmethod
    def _convert_time_format(time_str):
        # Split the input string into hours and period
        if ':' in time_str:
            return time_str
        
        parts = time_str.split()
        
        if len(parts) != 2:
            raise ValueError("Invalid time format. Please use 'X am' or 'X pm'.")
        
        hours, period = parts
        
        try:
            hours = int(hours)
        except ValueError:
            raise ValueError("Invalid hour. Please use a number.")
        
        if hours < 1 or hours > 12:
            raise ValueError("Hours must be between 1 and 12.")
        
        if period.lower() not in ['am', 'pm']:
            raise ValueError("Period must be 'am' or 'pm'.")
        
        # Format the time as "X:00 am/pm"
        return f"{hours:d}:00 {period.lower()}"
