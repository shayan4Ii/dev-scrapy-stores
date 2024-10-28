
# Store Location Scraper Development Guide

## 1. Field Specifications

### Core Fields

| Field Name | Required | Type | Format | Description |
|------------|----------|------|---------|------------|
| number | Optional | String | Store/dealer number or ID | Unique identifier for the store |
| name | Optional | String | Store name | Name or title of the store location |
| address | Required | String | "\<street>, \<city>, \<state> \<zipcode>" | Full formatted address with consistent comma separation |
| location | Required | Object | GeoJSON Point | Geographical coordinates in GeoJSON format |
| phone_number | Optional | String | Phone number | Contact number for the store |
| hours | Optional | Object | Day-based operating hours | Operating hours organized by day |
| services | Optional | Array | List of strings | Available services at the location |
| url | Required | String | Valid URL | Source URL where the data was scraped from |
| raw | Required | Object | Original data | Complete raw data from source |

### Field Details

#### Location Format
```json
{
    "type": "Point",
    "coordinates": [<longitude>, <latitude>]
}
```
- Must use float values for coordinates
- Longitude comes first in the coordinates array
- Validate coordinate ranges (-180 to 180 for longitude, -90 to 90 for latitude)

#### Hours Format
```json
{
    "<day_name>": {
        "open": "<time>",
        "close": "<time>"
    }
}
```
- Day names must be lowercase (monday, tuesday, etc.)
- Times should be in 12-hour format (e.g., "9:00 am", "5:30 pm")
- Handle special cases like "24 hours" or "closed"
- Missing or invalid hours should result in empty object `{}`

#### Address Format
- Street address first, followed by city, state, and zipcode
- Consistent comma separation between components
- Filter out empty address components
- Handle multi-line addresses and suites/units
- Example: "123 Main St, Suite 100, Springfield, IL 12345"

## 2. Types of Scrapers

### 1. API-Based Scrapers
- **Zipcode-Based Search**
  - Examples: Wingstop, Sbarro, Kia
  - Uses list of zipcodes to query store locations
  - Often requires API tokens or headers
  - Usually provides structured JSON responses
  ```python
  zipcode_api_format_url = "https://api.example.com/stores?zipcode={zipcode}"
  ```

### 2. Directory Navigation Scrapers
- **Geographic Navigation**
  - Examples: Shell, Mercedes-Benz, O'Reilly Auto
  - Follows state → city → store hierarchy
  - Requires multiple requests to reach store data
  - Often HTML-based with some JavaScript data
  ```python
  start_urls = ["https://example.com/stores/"]
  state_urls = response.xpath('//state-selector/@href').getall()
  city_urls = response.xpath('//city-selector/@href').getall()
  store_urls = response.xpath('//store-selector/@href').getall()
  ```

### 3. Single Page Directory Scrapers
- **Direct Store Listings**
  - Examples: Tractor Supply, Piggly Wiggly
  - All stores listed on a single or paginated page
  - May include embedded JavaScript data
  - Often uses store locator widgets

## 3. Common Extraction Patterns

### 1. API Response Parsing
```python
def parse(self, response):
    try:
        data = response.json()
        stores = data.get("stores", [])
        for store in stores:
            yield self._parse_store(store)
    except json.JSONDecodeError:
        self.logger.error("Failed to parse JSON response")
```

### 2. Embedded JavaScript Data
```python
# Pattern 1: JSON in script tags
STORES_JSON_XPATH = '//script[@type="application/ld+json"]/text()'

# Pattern 2: JavaScript variable assignment
STORES_INFO_JSON_RE = re.compile(r'var locations = (.*);')
store_info_json = self.STORES_INFO_JSON_RE.search(response.text)
```

### 3. HTML Parsing
```python
# Address extraction pattern
address_elem = response.xpath('//address[@itemprop="address"]')
street = address_elem.xpath('.//span[@class="street"]/text()').get()
city = address_elem.xpath('.//span[@class="city"]/text()').get()
state = address_elem.xpath('.//span[@class="state"]/text()').get()
zipcode = address_elem.xpath('.//span[@class="zip"]/text()').get()

# Hours extraction pattern
hours_rows = response.xpath('//table[@class="hours"]//tr')
for row in hours_rows:
    day = row.xpath('./td[@class="day"]/text()').get()
    open_time = row.xpath('./td[@class="open"]/text()').get()
    close_time = row.xpath('./td[@class="close"]/text()').get()
```

## 4. Field Validation and Error Handling

### 1. Required Field Validation
```python
def _validate_store(self, store: dict) -> bool:
    required_fields = ["address", "location", "url", "raw"]
    return all(store.get(field) for field in required_fields)
```

### 2. Location Validation
```python
def _validate_coordinates(self, lat: float, lon: float) -> bool:
    return -90 <= lat <= 90 and -180 <= lon <= 180

def _get_location(self, loc_info: dict) -> dict:
    try:
        latitude = loc_info.get('latitude')
        longitude = loc_info.get('longitude')
        
        if latitude is not None and longitude is not None:
            if self._validate_coordinates(float(latitude), float(longitude)):
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
        self.logger.warning("Invalid coordinates")
        return {}
    except Exception as e:
        self.logger.error(f"Error extracting location: {e}")
        return {}
```

### 3. Error Handling Hierarchy
1. **Field Level Errors**
   - Handle missing or invalid data
   - Provide default values where appropriate
   - Log warnings for unexpected formats

2. **Store Level Errors**
   - Validate required fields
   - Drop invalid stores
   - Log store-level failures

3. **Request Level Errors**
   - Handle API errors
   - Retry failed requests
   - Log request failures

### 4. Logging Best Practices
```python
# Warning for missing data
self.logger.warning(f"Missing address for store: {store_id}")

# Error for parsing failures
self.logger.error(f"Failed to parse store data: {e}", exc_info=True)

# Debug for duplicate detection
self.logger.debug(f"Duplicate store found: {store_id}")
```

## 5. Best Practices

### Must Have Practices
1. **Consistent Field Formatting**
   - Standardize address formatting
   - Use lowercase for day names
   - Consistent time format (12-hour with am/pm)

2. **Comprehensive Error Handling**
   - Try-except blocks around parsing logic
   - Validate all required fields
   - Log errors with appropriate context

3. **Deduplication**
   - Track processed store IDs
   - Skip duplicate stores
   - Log duplicate occurrences

4. **Data Validation**
   - Validate coordinates
   - Check address components
   - Verify phone number formats

### Good to Have Practices
1. **Performance Optimization**
   - Compile regular expressions
   - Minimize request count

2. **Code Organization**
   - Separate parsing logic
   - Reusable utility functions
   - Clear method naming

3. **Enhanced Logging**
   - Log successful extractions
   - Track extraction rates
   - Monitor performance metrics

4. **Other**
	-   Add error handling with try/except and log exceptions
	-   Use self.logger for logging (warn on missing data)
	-   Drop items missing required fields: address, location, url, raw
	-   Add type hints (use lowercase dict/list + typing.Generator)
	-   Follow PEP 8 (imports, style, naming)
	-   Add one-line docstrings
	-   Reorder methods logically
    