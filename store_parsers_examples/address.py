import logging

class AddressExample():
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _get_address(self, raw_store_data: dict) -> str:
        """Get the formatted store address."""
        try:
            address_parts = [
                raw_store_data.get("addressLine1", ""),
                raw_store_data.get("addressLine2", ""),
                raw_store_data.get("addressLine3", "")
            ]
            street = ", ".join(filter(None, address_parts))

            city = raw_store_data.get("city", "")
            state = raw_store_data.get("countyProvinceState", "")
            zipcode = raw_store_data.get("postCode", "")

            city_state_zip = f"{city}, {state} {zipcode}".strip()

            return ", ".join(filter(None, [street, city_state_zip]))
        except Exception as e:
            self.logger.error(f"Error formatting address: {e}", exc_info=True)
            return ""
        
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    
    # Create example instance
    address_processor = AddressExample()
    
    # Test data
    sample_address = {
        "addressLine1": "123 Main Street",
        "addressLine2": "Suite 100",
        "addressLine3": "Building A",
        "city": "New York",
        "countyProvinceState": "NY",
        "postCode": "10001"
    }
    
    # Process and display results
    formatted_address = address_processor._get_address(sample_address)
    print("\nFormatted Address:")
    print(formatted_address)