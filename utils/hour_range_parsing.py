import re
from typing import Dict, Tuple, Optional, List

# Dictionary mapping day abbreviations to full names
DAY_MAPPING = {
    'sun': 'sunday',
    'mon': 'monday',
    'tue': 'tuesday',
    'wed': 'wednesday',
    'thu': 'thursday',
    'fri': 'friday',
    'sat': 'saturday',
}

def normalize_hours_text(hours_text: str) -> str:
    """Normalize the hours text by removing non-alphanumeric characters and converting to lowercase."""
    return re.sub(r'[^a-z0-9:]', '', hours_text.lower().replace('to', ''))

def extract_business_hours(input_string: str) -> List[Tuple[str, str, str]]:
    normalized_input = normalize_hours_text(input_string)
    
    # Regex components
    days = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
    day_suffix = r"(?:day)?"
    optional_colon = r"(?::)?"
    time = r"(\d{1,2}(?::\d{2})?)([ap]m)"
    
    # Construct the full pattern
    pattern = f"({days}{day_suffix}){optional_colon}?{time}{time}"
    
    matches = re.finditer(pattern, normalized_input, re.MULTILINE)
    
    results = []
    for match in matches:
        day = match.group(1)[:3]
        open_time = f"{match.group(2)} {match.group(3)}"
        close_time = f"{match.group(4)} {match.group(5)}"
        results.append((day, open_time, close_time))
    
    return results

def extract_business_hour_range(input_string: str) -> List[Tuple[str, str, str, str]]:
    normalized_input = normalize_hours_text(input_string)
    
    # Regex components
    days = r"(?:mon|tues?|wed(?:nes)?|thur?s?|fri|sat(?:ur)?|sun)"
    day_suffix = r"(?:day)?"
    optional_colon = r"(?::)?"
    time = r"(\d{1,2}(?::\d{2})?)([ap]m)"
    
    # Construct the full pattern
    pattern = f"({days}{day_suffix})({days}{day_suffix}){optional_colon}?{time}{time}"
    
    matches = re.finditer(pattern, normalized_input, re.MULTILINE)
    
    results = []
    for match in matches:
        start_day = match.group(1)[:3]
        end_day = match.group(2)[:3]
        open_time = f"{match.group(3)} {match.group(4)}"
        close_time = f"{match.group(5)} {match.group(6)}"
        results.append((start_day, end_day, open_time, close_time))
    
    return results

def parse_business_hours(input_text: str) -> Dict[str, Dict[str, str]]:
    result = {day: {'open': None, 'close': None} for day in DAY_MAPPING.values()}
    
    # Extract and process day ranges
    day_ranges = extract_business_hour_range(input_text)
    for start_day, end_day, open_time, close_time in day_ranges:
        start_index = list(DAY_MAPPING.keys()).index(start_day)
        end_index = list(DAY_MAPPING.keys()).index(end_day)
        if end_index < start_index:  # Handle cases like "Saturday to Sunday"
            end_index += 7
        for i in range(start_index, end_index + 1):
            day = list(DAY_MAPPING.keys())[i % 7]
            full_day = DAY_MAPPING[day]
            if result[full_day]['open'] and result[full_day]['close']:
                # If day already has hours, skip this range
                print(f"Day {full_day} already has hours, skipping range {start_day} to {end_day}")
                continue
            result[full_day]['open'] = open_time
            result[full_day]['close'] = close_time
    
    # Extract and process individual days (overwriting any conflicting day ranges)
    single_days = extract_business_hours(input_text)
    for day, open_time, close_time in single_days:
        full_day = DAY_MAPPING[day]
        if result[full_day]['open'] and result[full_day]['close']:
            # If day already has hours, skip this range
            print(f"Day {full_day} already has hours, skipping range {start_day} to {end_day}")
            continue
        result[full_day]['open'] = open_time
        result[full_day]['close'] = close_time
    
    return result

# # Example usage
# examples = [
#     "Sunday - Saturday: 7AM - 11PM",
#     "Monday - Saturday: 7 AM - 11 PM\nSunday: 7 AM - 9 PM",
#     "Monday - Thursday 9 am - 9 pm | Friday - Saturday 9 am -10 pm | Sunday 10 am - 6 pm",
#     "Monday & Tuesday 9 am-8 pm; Wednesday to Saturday 9 am-9 pm; Sunday 11 am-6 pm",
# ]
examples = [
    # "Sunday - Saturday: Open 24 hours",
    # "Monday - Sunday: Open 24 hours",
    # "Open 24 hours",
    "Sunday - Saturday: 6 AM - 10 PM",
    "Sunday - Saturday: 7 AM - 11 PM",
    "Sunday - Saturday: 6 AM - 12 AM",
    # "6 am to 12 am daily",
    "Sunday - Saturday: 7 AM - 9 PM",
    "Sunday - Saturday: 6:30 AM - 11 PM",
    # "7am-11pm daily",
    "Monday - Saturday: 7 AM - 11 PM\nSunday: 7 AM - 9 PM",
    "Monday - Saturday: 6 AM - 10 PM\nSunday: 6 AM - 9 PM",
    "Monday - Friday: 7 AM - 12 AM\nSaturday - Sunday: 6 AM - 12 AM",
    "Monday - Saturday: 6 AM - 12 AM\nSunday: 6 AM - 10 PM",
    "Monday -Thursday 9 am - 9 pm;\nFriday - Saturday 9 am -10 pm;\nSun 12 pm-6 pm",
    "Monday to Saturday 8 am-10 pm (Beer & Wine);\nMonday to Saturday 9 am-10 pm (Spirits);\nSunday 10:45 am-8:45 pm (Beer);\nSunday 12 pm-8:45 pm (Wine & Spirits)"
]

for example in examples:
    print(f"Input: {example}")
    result = parse_business_hours(example)
    for day, hours in result.items():
        print(f"{day.capitalize()}: Open - {hours['open']}, Close - {hours['close']}")
    print()