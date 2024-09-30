from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from parsel import Selector
import json

class TotalWineScraper:

    start_url = "https://www.totalwine.com/store-finder/browse/AZ"

    def __init__(self):
        self.driver = uc.Chrome()

    def get_state_codes(self, data):
        states_list = data['search']['stores']['metadata']['states']
        state_codes = [state['stateIsoCode'] for state in states_list]
        return state_codes

    def get_json(self, driver):
        sel = Selector(driver.page_source)

        script_data = sel.xpath(
            '//script[contains(text(), "window.INITIAL_STATE")]/text()').get()

        json_text = script_data.replace("window.INITIAL_STATE = ", "")

        data = json.loads(json_text)
        return data

    def get_stores_by_state(self, state_code):
        self.driver.get(
            f"https://www.totalwine.com/store-finder/browse/{state_code}")
        data = self.get_json(self.driver)
        return self.get_parsed_stores(data)
    
    def get_stores(self, data):
        stores = data['search']['stores']['stores']
        return stores

    def get_parsed_stores(self, data):
        parsed_stores = []
        stores = self.get_stores(data)
        for store in stores:
            parsed_store = self.parse_store(store)
            parsed_stores.append(parsed_store)
        return parsed_stores

    def parse_store(self, store):
        parsed_store = {}
        parsed_store['number'] = store['storeNumber']
        parsed_store['name'] = store['name']
        parsed_store['phone_number'] = store['phoneFormatted']
        parsed_store['address'] = self._get_address(store)
        parsed_store['location'] = self._get_location(store)
        parsed_store['hours'] = self._get_hours(store)
        parsed_store['raw'] = store
        parsed_store['url'] = f"https://www.totalwine.com/store-info/{store['storeNumber']}"
        return parsed_store

    def _get_address(self, store_info) -> str:
        """Format store address."""
        try:
            address_parts = [
                store_info.get("address1", ""),
                store_info.get("address2", ""),
            ]
            street = ", ".join(filter(None, address_parts))

            city = store_info.get("city", "")
            state = store_info.get("state", "")
            zipcode = store_info.get("zip", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            full_address = ", ".join(filter(None, [street, city_state_zip]))
            if not full_address:
                self.logger.warning(f"Missing address information: {store_info}")
            return full_address
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""

    def _get_location(self, store_info):
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get('latitude')
            longitude = store_info.get('longitude')

            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }

            self.logger.warning(f"Missing latitude or longitude for store: {store_info}")
            return {}
        except ValueError as error:
            self.logger.warning(f"Invalid latitude or longitude values: {error}")
        except Exception as error:
            self.logger.error(f"Error extracting location: {error}", exc_info=True)
        return {}

    def _get_hours(self, store_info) -> dict[str, dict[str, str]]:
        """Extract and parse store hours."""
        try:
            hours = {}
            
            raw_hours = store_info.get("storeHours", {}).get("days", [])

            if not raw_hours:
                self.logger.warning(f"Missing store hours for: {store_info}")
                return hours
            
            for day_hours in raw_hours:
                day = day_hours.get("dayOfWeek").lower()
                open_time = day_hours.get("openingTime").lower()
                close_time = day_hours.get("closingTime").lower()

                hours[day] = {
                    "open": open_time,
                    "close": close_time
                }

            if not hours:
                self.logger.warning(f"Missing store hours for: {store_info}")

            return hours
        except Exception as e:
            self.logger.error(f"Error getting store hours: {e}", exc_info=True)
            return {}

    def scrape_stores(self):
        stores = []
        self.driver.get(self.start_url)
        data = self.get_json(self.driver)
        state_codes = self.get_state_codes(data)

        # removing AZ from state_codes to avoid duplicate stores
        state_codes.remove("AZ")

        stores.extend(self.get_parsed_stores(data))
        for state_code in state_codes:
            self.driver.get(
                f"https://www.totalwine.com/store-finder/browse/{state_code}")
            data = self.get_json(self.driver)
            stores.extend(self.get_parsed_stores(data))
        return stores

    def save_to_file(self, stores, filename) -> None:
        """Save scraped store data to a JSON file."""
        if filename is None:
            current_date = datetime.now().strftime("%Y%m%d")
            filename = f'data/totalwine-{current_date}.json'
        
        try:
            with open(filename, 'w') as f:
                json.dump(stores, f, indent=2)
            self.logger.info(f"Saved {len(stores)} stores to {filename}")
        except IOError as e:
            self.logger.error(f"Error saving data to file: {e}", exc_info=True)


    def quit(self):
        self.driver.quit()
        

if __name__ == "__main__":
    self = TotalWineScraper()
    stores = self.scrape_stores()
    self.save_to_file(stores)
    self.quit()