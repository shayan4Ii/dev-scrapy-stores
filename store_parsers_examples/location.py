import logging
import json

class LocationExample:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _get_location(self, store_info: dict) -> dict:
        """Extract and format location coordinates."""
        try:
            latitude = store_info.get("latitude")
            longitude = store_info.get("longitude")
            if latitude is not None and longitude is not None:
                return {
                    "type": "Point",
                    "coordinates": [float(longitude), float(latitude)]
                }
            self.logger.warning("Missing latitude or longitude for store: %s", store_info.get("storeId"))
        except ValueError as error:
            self.logger.warning("Invalid latitude or longitude values: %s", error)
        except Exception as error:
            self.logger.error("Error extracting location: %s", error, exc_info=True)
        return {}

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Create example instance
    location_processor = LocationExample()
    
    # Test data
    sample_location = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "storeId": "NYC001"
    }
    
    # Process and display results
    processed_location = location_processor._get_location(sample_location)
    print(f"\nProcessed Location: \n{json.dumps(processed_location, indent=2)}")

