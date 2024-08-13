import requests
import json
from bs4 import BeautifulSoup

def fetch_shoprite_data():
    url = "https://www.shoprite.com/sm/pickup/rsid/3000/store/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Headers: {response.headers}")
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the script containing the data
        script = soup.find('script', text=lambda t: t and 'window.__PRELOADED_STATE__=' in t)
        
        if script:
            # Extract the JSON data
            json_text = script.string.split('window.__PRELOADED_STATE__=')[1].strip()
            data = json.loads(json_text)
            
            # Extract store information
            stores = data['stores']['availablePlanningStores']['items']
            print(f"Number of stores found: {len(stores)}")
            
            # Save all stores to a JSON file
            with open('shoprite_stores.json', 'w') as f:
                json.dump(stores, f, indent=2)
            
            print(f"All stores have been saved to 'shoprite_stores.json'")
            
            # Print details of the first store as an example
            if stores:
                print("First store details:")
                print(json.dumps(stores[0], indent=2))
        else:
            print("Could not find the required script tag.")
        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_shoprite_data()