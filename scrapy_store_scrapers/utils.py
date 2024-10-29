from datetime import datetime
from typing import Union
import json



def convert_to_12h_format(time_str: str) -> str:
    """Convert time to 12-hour format."""
    if not time_str:
        return ""
    try:
        time_obj = datetime.strptime(time_str, '%H:%M').time()
        return time_obj.strftime('%I:%M %p').lower().lstrip('0')
    except ValueError:
        return None
    

def load_zipcode_data(zipcode_file_path: str) -> list[dict[str, Union[str, float]]]:
    """Load zipcode data from a JSON file."""
    with open(zipcode_file_path, 'r') as f:
        return json.load(f)