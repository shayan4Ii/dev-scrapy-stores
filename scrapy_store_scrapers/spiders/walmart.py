import scrapy
from typing import Dict, Iterator, List, Any
import json
from datetime import datetime
from scrapy_store_scrapers.items import WalmartStoreItem
from scrapy.exceptions import IgnoreRequest
import logging

class WalmartSpider(scrapy.Spider):
    """Spider for scraping Walmart store information."""
    name: str = "walmart"
    allowed_domains: List[str] = ["www.walmart.com"]
    start_urls: List[str] = ["https://www.walmart.com/store-directory"]

    @staticmethod
    def get_default_headers() -> Dict[str, str]:
        """Return default headers for requests."""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "TE": "Trailers",
            "Cookie": """ACID=189f431f-9133-4277-a3d1-0727a8e51ead; hasACID=true; _m=9; abqme=true; vtc=QaGRWY4n-_7N20ajWuaOjo; _pxhd=a3ef1a1bea2c8e364f4d0b7a682da4a8f8939c6aa69e2908f82f00165bee321b:35cb02ab-4f5d-11ef-a5fc-2ddff050109e; pxcts=36ad1344-4f5d-11ef-91c2-5fcebc78d9a5; _pxvid=35cb02ab-4f5d-11ef-a5fc-2ddff050109e; AID=wmlspartner%3D0%3Areflectorid%3D0000000000000000000000%3Alastupd%3D1723233126552; locGuestData=eyJpbnRlbnQiOiJTSElQUElORyIsImlzRXhwbGljaXQiOmZhbHNlLCJzdG9yZUludGVudCI6IlBJQ0tVUCIsIm1lcmdlRmxhZyI6ZmFsc2UsImlzRGVmYXVsdGVkIjp0cnVlLCJwaWNrdXAiOnsibm9kZUlkIjoiMzA4MSIsInRpbWVzdGFtcCI6MTcyMjQ0NDcyMjY5Miwic2VsZWN0aW9uVHlwZSI6IkRFRkFVTFRFRCJ9LCJzaGlwcGluZ0FkZHJlc3MiOnsidGltZXN0YW1wIjoxNzIyNDQ0NzIyNjkyLCJ0eXBlIjoicGFydGlhbC1sb2NhdGlvbiIsImdpZnRBZGRyZXNzIjpmYWxzZSwicG9zdGFsQ29kZSI6Ijk1ODI5IiwiZGVsaXZlcnlTdG9yZUxpc3QiOlt7Im5vZGVJZCI6IjMwODEiLCJ0eXBlIjoiREVMSVZFUlkiLCJ0aW1lc3RhbXAiOjE3MjMyMzMxMjcxMjUsImRlbGl2ZXJ5VGllciI6bnVsbCwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIiwic2VsZWN0aW9uU291cmNlIjpudWxsfV0sImNpdHkiOiJTYWNyYW1lbnRvIiwic3RhdGUiOiJDQSJ9LCJwb3N0YWxDb2RlIjp7InRpbWVzdGFtcCI6MTcyMjQ0NDcyMjY5MiwiYmFzZSI6Ijk1ODI5In0sIm1wIjpbXSwidmFsaWRhdGVLZXkiOiJwcm9kOnYyOjE4OWY0MzFmLTkxMzMtNDI3Ny1hM2QxLTA3MjdhOGU1MWVhZCJ9; userAppVersion=us-web-1.154.0-6dfbdc10a51252e64ac08f3302ba4ff0452a3e5a-080516; io_id=46ceba27-b37e-4ae8-aa2d-339bf6c82de3; _gcl_au=1.1.682725587.1723447900; _uetsid=ea7b20d0587c11efa328c7f8b8f6eee3; _uetvid=ea7b6490587c11ef9a60bb6c34b935ed; assortmentStoreId=3081; _shcc=USA; _intlbu=false; hasLocData=1; mobileweb=0; xpa=0bGxN|CoXWr|D2oRZ|Ej-_h|IuElO|K3iS3|LzMhr|MSCCn|McEea|Mo1oz|NbX17|SqH-y|Y9NmX|bWE5d|eRpzo|ejJi5|fdm-7|j8DMe|pOpc-|wEHwu|xyFVh|zf8aF; exp-ck=0bGxN2CoXWr2D2oRZ1Ej-_h1IuElO1MSCCn2Mo1oz1NbX172SqH-y2Y9NmX2eRpzo2ejJi51fdm-71wEHwu2xyFVh1zf8aF2; bstc=UwI5MywQfDirAgdAu8EnLo; xpth=x-o-mverified%2Bfalse~x-o-mart%2BB2C; bm_mi=3F37AF66CDFEDB6CC9474D7ACF804128~YAAQjqhkaKYO4kORAQAAAlraRhiv+gIxLyEds3T1g8tuDdjTvRRzNa34DHppg6BBgDu3N3MWkJxpm/W8SpS0WJRCZkxQvFR/F78956pSh5VkaomKpqW32RQwOWpWOwGSMneURTbklg5c3ntGvaQd+VCrw84mLrEmUvx/INNRRY/KDkZWHrlz4Iolm2PI8QZ6N4kbMGgeywH+aU0h8X0cScRgqe7TURW/2S8IwVufH9JXkofW5XTb/Y73FWRIHdauqbCQGAGGhtfc434UCCt7r5QjnwtxQOmUDSnetRul/okbPamGQr+dlAB+A1QaoA1Ot1cmd5hF~1; bm_sv=294EE3CC5CE99630E601842D6EC0F7C1~YAAQjqhkaKcO4kORAQAAAlraRhhxp9od1A9g6zJquUj3bkwmd0s5cQ6DLff0QdQ7jMrDeKVqiuYzyRgQ9qJBmVR4/vjoX5qwssS2omZZGdl5i1K1EbMYjq2jzyQE1CF4HH49lBI2XgcozSYiMgwpKZmwTXzwkdLo7StlGECXOToerneDfbTMUVKWtdyDNjfQdLi7LTXmJByNetycEsc48dRezw/IAYtTmb6KOnc5CgrPW+FQP6Law/s4d5QwG66y3Ss=~1; ak_bmsc=30A34FB95DDBEA0C2E3C69E592ECC6E7~000000000000000000000000000000~YAAQjqhkaDwP4kORAQAAvWTaRhjNOvOGjg3QZCj7fE2eXk81+uoaqjCLOPx2C2XnFRkg8axmI7osf8EyNX1w6jq8eB/KA+sU7+TNgwLL2X7x7hfyVtGsTbuiqMK8y/sU7kx3R5/YD6YGRhq4Dm4OF+LiHBu2awuPHzSDo7YM/L+Jg+86Gi1jOh9QpevjI72LHNhYC4u+Vko9clk+rItqBTEJGKcejFMUHydh7aDn5tmX+OMcXRzHkjlxQE7g6dlOguro0onT99oNwWM8cQR5A0sy5Os51ObyK/AlK4KMcyt0YUpWHXPJoKc2umHsLtJnGQJfffgmrkhDiEMMTVm+33Tpd1IEAo90jgZzFj02m6jryVQkDAGCdCTftT1kju8ourq5zLODZNFse6EE4JCojzaYsBHawU0oLbYJujuM/zTkk7a9RXg0RpHg5Ysggth62b7XAeynvVP2Yi0hRLGijQQURqYFX8LZODaYqjxVyeJjMQaGpOtLhV/koJq0bdMSHQGo0yiXWjWo9EMC6Jy4/U62Tnw0fwzDA04=; _astc=5a87a28756e05a52b64ffa00f82e1b0f; auth=MTAyOTYyMDE4tJ9n1Zn%2F0dcCTAR12Gr8fC%2FBoRx9luCpZdaOaUvp4mt13S%2F2lPbzQaRcSI6qM%2B0L25EmZqm5zlmypumswjJiswKMFzgPYmbru6BDj3qWQwpDGUVNApuTuVOK2xo8kdwq767wuZloTfhm7Wk2KcjygqjPQjfEaB1WK%2FMFlTnguVWTyqjzgIgxNqeXIXoHbb7%2FO8T3JAFcVKywEXrGIMfOLbmC6kCvAJuBAADQfXV5%2FaUUMk70P8glgOEpLOprhDfMJ0tmvH1FCaN9tZDh4SCrHeKRS65iYJ8PyMLftuDxv71dDqbBu531Uqr5q3um73ZnEYV2Q6m2hkTHUOUi82M%2BvkAt3oc8c1ULwO0bm%2FMlQCegmYV%2Fqv%2B42FAow3YI%2FWgImPeCLoP9LJlN0%2FzsLK41AUjyrOXbKKhH072NS%2FW0j%2FU%3D; locDataV3=eyJpc0RlZmF1bHRlZCI6dHJ1ZSwiaXNFeHBsaWNpdCI6ZmFsc2UsImludGVudCI6IlNISVBQSU5HIiwicGlja3VwIjpbeyJidUlkIjoiMCIsIm5vZGVJZCI6IjMwODEiLCJkaXNwbGF5TmFtZSI6IlNhY3JhbWVudG8gU3VwZXJjZW50ZXIiLCJub2RlVHlwZSI6IlNUT1JFIiwiYWRkcmVzcyI6eyJwb3N0YWxDb2RlIjoiOTU4MjkiLCJhZGRyZXNzTGluZTEiOiI4OTE1IEdFUkJFUiBST0FEIiwiY2l0eSI6IlNhY3JhbWVudG8iLCJzdGF0ZSI6IkNBIiwiY291bnRyeSI6IlVTIiwicG9zdGFsQ29kZTkiOiI5NTgyOS0wMDAwIn0sImdlb1BvaW50Ijp7ImxhdGl0dWRlIjozOC40ODI2NzcsImxvbmdpdHVkZSI6LTEyMS4zNjkwMjZ9LCJpc0dsYXNzRW5hYmxlZCI6dHJ1ZSwic2NoZWR1bGVkRW5hYmxlZCI6dHJ1ZSwidW5TY2hlZHVsZWRFbmFibGVkIjp0cnVlLCJzdG9yZUhycyI6IjA2OjAwLTIzOjAwIiwiYWxsb3dlZFdJQ0FnZW5jaWVzIjpbIkNBIl0sInN1cHBvcnRlZEFjY2Vzc1R5cGVzIjpbIlBJQ0tVUF9TUEVDSUFMX0VWRU5UIiwiUElDS1VQX0lOU1RPUkUiLCJQSUNLVVBfQ1VSQlNJREUiXSwidGltZVpvbmUiOiJQU1QiLCJzdG9yZUJyYW5kRm9ybWF0IjoiV2FsbWFydCBTdXBlcmNlbnRlciIsInNlbGVjdGlvblR5cGUiOiJERUZBVUxURUQifV0sInNoaXBwaW5nQWRkcmVzcyI6eyJsYXRpdHVkZSI6MzguNDc0NiwibG9uZ2l0dWRlIjotMTIxLjM0MzgsInBvc3RhbENvZGUiOiI5NTgyOSIsImNpdHkiOiJTYWNyYW1lbnRvIiwic3RhdGUiOiJDQSIsImNvdW50cnlDb2RlIjoiVVNBIiwiZ2lmdEFkZHJlc3MiOmZhbHNlLCJ0aW1lWm9uZSI6IkFtZXJpY2EvTG9zX0FuZ2VsZXMiLCJhbGxvd2VkV0lDQWdlbmNpZXMiOlsiQ0EiXX0sImFzc29ydG1lbnQiOnsibm9kZUlkIjoiMzA4MSIsImRpc3BsYXlOYW1lIjoiU2FjcmFtZW50byBTdXBlcmNlbnRlciIsImludGVudCI6IlBJQ0tVUCJ9LCJpbnN0b3JlIjpmYWxzZSwiZGVsaXZlcnkiOnsiYnVJZCI6IjAiLCJub2RlSWQiOiIzMDgxIiwiZGlzcGxheU5hbWUiOiJTYWNyYW1lbnRvIFN1cGVyY2VudGVyIiwibm9kZVR5cGUiOiJTVE9SRSIsImFkZHJlc3MiOnsicG9zdGFsQ29kZSI6Ijk1ODI5IiwiYWRkcmVzc0xpbmUxIjoiODkxNSBHRVJCRVIgUk9BRCIsImNpdHkiOiJTYWNyYW1lbnRvIiwic3RhdGUiOiJDQSIsImNvdW50cnkiOiJVUyIsInBvc3RhbENvZGU5IjoiOTU4MjktMDAwMCJ9LCJnZW9Qb2ludCI6eyJsYXRpdHVkZSI6MzguNDgyNjc3LCJsb25naXR1ZGUiOi0xMjEuMzY5MDI2fSwiaXNHbGFzc0VuYWJsZWQiOnRydWUsInNjaGVkdWxlZEVuYWJsZWQiOmZhbHNlLCJ1blNjaGVkdWxlZEVuYWJsZWQiOmZhbHNlLCJhY2Nlc3NQb2ludHMiOlt7ImFjY2Vzc1R5cGUiOiJERUxJVkVSWV9BRERSRVNTIn1dLCJpc0V4cHJlc3NEZWxpdmVyeU9ubHkiOmZhbHNlLCJhbGxvd2VkV0lDQWdlbmNpZXMiOlsiQ0EiXSwic3VwcG9ydGVkQWNjZXNzVHlwZXMiOlsiREVMSVZFUllfQUREUkVTUyJdLCJ0aW1lWm9uZSI6IlBTVCIsInN0b3JlQnJhbmRGb3JtYXQiOiJXYWxtYXJ0IFN1cGVyY2VudGVyIiwic2VsZWN0aW9uVHlwZSI6IkxTX1NFTEVDVEVEIn0sImlzZ2VvSW50bFVzZXIiOmZhbHNlLCJyZWZyZXNoQXQiOjE3MjM0NzU4MTE5MjcsInZhbGlkYXRlS2V5IjoicHJvZDp2MjoxODlmNDMxZi05MTMzLTQyNzctYTNkMS0wNzI3YThlNTFlYWQifQ%3D%3D; xptwj=uz:055a8e1e150d2fc32cb7:nNmRopZMCeAcOWPY3UTS6/o29IYkwhQRoqBHOhbO8aDJiqzIS2GUp7bI8E9wEUIOO+f4ToqYNP1/PYZa6QZxWYY7ULjcuUEYY+Oi4UufZWhJHiuPEtqnGpdy7SGBtgZMjQBV8xxhR8nQSPX2MGDh6IhzaeH0w0YYNLfjvO0=; xptwg=3415760789:1D34ED1B3D5C8E0:48C8D0E:924DE488:E7558FF0:1D269782:; akavpau_p1=1723473511~id=8275360e38e536cfabcfec4181e0c33e; if_id=FMEZARSFZenzvP6SSDbusUZutoaE11N39aLMA1H2ox+8PioDiXd+tF60cIxsvuu/7ed7j5EudnYqKW9D636bEb702PdElwOfBmyhY9wi7LoACVFGhvcW09o74MRBoI2Rj4KDaoEU9DG4kvbhl6AgebuJBDj8KpHggqDjQQxHAC3xS7sKK2AECK5FFJct8BNbj1Ed1D0kGKcL1ZlUow==; TS016ef4c8=01c6e9ec03109c940696835663b1a25c8f7bbf8c1dfeba1b1a5e6bbe3975d48ddc03178f053dc87eecd279a935701e4a3d172c311f; TS01f89308=01c6e9ec03109c940696835663b1a25c8f7bbf8c1dfeba1b1a5e6bbe3975d48ddc03178f053dc87eecd279a935701e4a3d172c311f; TS8cb5a80e027=0842fb9b35ab2000ac8a03e36f256a4325992ac923bd890cb522c459e571a947b4645e3e6dcaa18908dc140c221130006100573a18a7cf6915d1d4bc39b01ff1b0cab4333eb189f0baa5958c492d7f709584ae92f66559bdcbae68c7774dbda3; _px3=f192527a5172eb5e7f1cd99cd14c0fe5943db3cad8a1b3f3c2de169c020dff71:YeW1EEbzL/tU/GiIKA9M1IXP/CAFtzz9Ea6xPOf6hBx1Q/DlRWUZxmsAS8YFjw4L+rc19wqskUXLgReQAQ156Q==:1000:ZiLZTd6emtpJ8+EnjaUcvusuBO8wSB49Pk/fx6Y1R/EiB+IyTkHaTBYsUzABcYAzuN3ZKeS0Qzks/bxV3O+U7LSqFMDyIUPm8sa+zHPjYk6gbldk9KuXB3anC5GHC6XkPlhejqsKP1fP8zPwzPH4f5g/IeAon2CXmT009f/2UIRDbZhb7vav8WqGqGpsaNW7EIm27K+Aw3Sc778CgaJj1WoNBf6GnqTHcQ/57c0vmTw=; _pxde=d060d91ce11730ca7df8a89242e28259930a0b1ebcdbdea8e65e49ef161750cd:eyJ0aW1lc3RhbXAiOjE3MjM0NzMzMjg3MzB9; com.wm.reflector="reflectorid:0000000000000000000000@lastupd:1723473331554@firstcreate:1722444722550"; xptc=_m%2B9~assortmentStoreId%2B3081; xpm=0%2B1723473331%2BQaGRWY4n-_7N20ajWuaOjo~%2B0; TS012768cf=01bab6ef386a98ce5d25d809fb841c0689bf0dea63106c001c895c304758727a5e2ad9271f2ea26abc830a3f8d6cc7e05cace4cc87; TS01a90220=01bab6ef386a98ce5d25d809fb841c0689bf0dea63106c001c895c304758727a5e2ad9271f2ea26abc830a3f8d6cc7e05cace4cc87; TS2a5e0c5c027=0826a4e0adab20004d7c303a5de52c1162166086b5850a2181aca59a67ea81b0534bc8cf9480fa1308e01a1286113000e4e225df381cc4fe716b5c224eb1d567402d6cabe8c0545bbb164df9e9c5e696cb47137ca5f0071a55b6c7724689a233"""
        }

    def start_requests(self) -> Iterator[scrapy.Request]:
        """Generate initial requests for store directory pages."""
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=self.get_default_headers(), callback=self.parse_store_directory)

    def extract_store_ids(self, stores_by_location: Dict[str, List[Dict[str, Any]]]) -> List[str]:
        """Extract store IDs from the stores by location data."""
        store_ids = []

        for state, cities in stores_by_location.items():
            for city_data in cities:
                stores = city_data.get('stores', [city_data])
                if not isinstance(stores, list):
                    self.logger.error(f"Stores data is not a list for city in state {state}: {city_data}")
                    continue

                for store in stores:
                    # Try both 'storeId' and 'storeid' keys
                    store_id = store.get('storeId') or store.get('storeid')
                    if store_id:
                        store_ids.append(str(store_id))
                    else:
                        self.logger.warning(f"No store ID found for store in state {state}: {store}")

        return store_ids

    def parse_store_directory(self, response: scrapy.http.Response) -> Iterator[scrapy.Request]:
        """Parse the store directory page and yield requests for individual store pages."""
        try:
            # Extract JSON data from script tag
            script_content = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
            if not script_content:
                raise ValueError("Script content not found")
            
            json_data = json.loads(script_content)

            # Extract stores by location data
            stores_by_location_json = json_data["props"]["pageProps"]["bootstrapData"]["cv"]["storepages"]["_all_"]["sdStoresPerCityPerState"]
            stores_by_location = json.loads(stores_by_location_json.strip('"'))

            # Extract store IDs and generate requests for each store
            store_ids = self.extract_store_ids(stores_by_location)
            self.logger.info(f"Found {len(store_ids)} store IDs")

            for store_id in store_ids:
                store_url = f"https://www.walmart.com/store/{store_id}"
                yield scrapy.Request(url=store_url, headers=self.get_default_headers(), callback=self.parse_store)
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in parse_store_directory: {str(e)}")
        except KeyError as e:
            self.logger.error(f"Key error in parse_store_directory: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse_store_directory: {str(e)}")

    def parse_store(self, response: scrapy.http.Response) -> WalmartStoreItem:
        """Parse individual store page and extract store information."""
        try:
            # Extract JSON data from script tag
            script_content = response.xpath('//script[@id="__NEXT_DATA__"]/text()').get()
            if not script_content:
                raise ValueError("Script content not found")
            
            json_data = json.loads(script_content)
            store_data = json_data['props']['pageProps']['initialData']['initialDataNodeDetail']['data']['nodeDetail']

            store_latitude, store_longitude = self.extract_geo_info(store_data['geoPoint'])

            # Create and return WalmartStoreItem
            store_item = WalmartStoreItem(
                name=store_data['displayName'],
                number=int(store_data['id']),
                address=self.format_address(store_data['address']),
                phone_number=store_data['phoneNumber'],
                hours=self.format_hours(store_data['operationalHours']),
                location={
                    "type": "Point",
                    "coordinates": [store_longitude, store_latitude]
                },
                services=[service['displayName'] for service in store_data['services']],
            )

            return store_item
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON decode error in parse_store for {response.url}: {str(e)}")
        except KeyError as e:
            self.logger.error(f"Key error in parse_store for {response.url}: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error in parse_store for {response.url}: {str(e)}")
        
        raise IgnoreRequest(f"Failed to parse store data for {response.url}")

    @staticmethod
    def format_address(address: Dict[str, str]) -> str:
        """Format the store address."""
        return f"{address['addressLineOne']}, {address['city']}, {address['state']} {address['postalCode']}"

    @staticmethod
    def extract_geo_info(geo_info: Dict[str, float]) -> tuple:
        """Extract latitude and longitude from geo info."""
        return geo_info['latitude'], geo_info['longitude']

    def format_hours(self, operational_hours: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Format the store operational hours."""
        formatted_hours = {}
        for day_hours in operational_hours:
            formatted_hours[day_hours['day'].lower()] = {
                "open": self.convert_to_12h_format(day_hours['start']),
                "close": self.convert_to_12h_format(day_hours['end'])
            }
        return formatted_hours

    @staticmethod
    def convert_to_12h_format(time_str: str) -> str:
        """Convert 24-hour time format to 12-hour format."""
        if not time_str:
            return time_str
        time_obj = datetime.strptime(time_str, '%H:%M').time()
        return time_obj.strftime('%I:%M %p').lower()

# https://www.walmart.com/store/5697-undefined-undefined
# https://www.walmart.com/store/2936-undefined-undefined
# 
