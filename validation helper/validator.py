import json
import random
from typing import List, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path
from glob import glob

def load_json_files(file_patterns: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Load multiple JSON files
    
    Args:
        file_patterns: List of file patterns (e.g., ["data/*.json", "other/*.json"])
    
    Returns:
        Dictionary mapping filename to list of records from that file
    """
    file_data = {}
    
    for pattern in file_patterns:
        for filepath in glob(pattern):
            with open(filepath, 'r') as f:
                try:
                    data = json.load(f)
                    # Handle both single objects and lists of objects
                    if isinstance(data, dict):
                        data = [data]
                    file_data[Path(filepath).name] = data
                    print(f"Loaded {len(data)} records from {filepath}")
                except json.JSONDecodeError as e:
                    print(f"Error loading {filepath}: {e}")
    
    return file_data

def is_falsy_location(location: Any) -> bool:
    if location is None or location == "":
        return True
    if not isinstance(location, dict):
        return True
    if location.get("type") != "Point":
        return True
    coords = location.get("coordinates", [])
    if not coords or len(coords) != 2:
        return True
    return any(coord is None or coord == "" for coord in coords)

def is_falsy_hours(hours: Any) -> bool:
    if not hours or not isinstance(hours, dict):
        return True
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    for day in days:
        day_schedule = hours.get(day)
        if not day_schedule or not isinstance(day_schedule, dict):
            return True
        if not day_schedule.get('open') or not day_schedule.get('close'):
            return True
    return False

def is_day_falsy(day_data: Any) -> bool:
    if not day_data or not isinstance(day_data, dict):
        return True
    return not day_data.get('open') or not day_data.get('close')

def generate_single_source_report(filename: str, data: List[Dict[str, Any]], output_dir: str):
    """Generate a markdown report for a single data source"""
    expected_fields = ['name', 'number', 'address', 'phone_number', 'hours', 'location', 'services', 'url', 'raw']
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    output_file = Path(output_dir) / f"{Path(filename).stem}_report.md"
    
    with open(output_file, 'w') as f:
        # Header
        f.write(f"# Data Quality Report - {filename}\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Data Source Summary
        f.write("## Data Source Summary\n\n")
        f.write(f"- **Source File**: {filename}\n")
        f.write(f"- **Total Records**: {len(data)}\n\n")

        # Missing Fields Analysis
        f.write("## Missing Fields Analysis\n\n")

        # Fields missing in all records
        all_missing = set(expected_fields)
        for item in data:
            all_missing &= set(expected_fields) - set(item.keys())
        
        if all_missing:
            f.write("### Fields Missing in All Records\n\n")
            for field in sorted(all_missing):
                f.write(f"- {field}\n")
            f.write("\n")

        # Fields missing in specific records
        f.write("### Fields Missing in Specific Records\n\n")
        for field in expected_fields:
            missing_records = [item for item in data if field not in item]
            if missing_records and len(missing_records) != len(data):
                f.write(f"#### {field}\n\n")
                f.write(f"Missing in {len(missing_records)} out of {len(data)} records ")
                f.write(f"({(len(missing_records)/len(data)*100):.1f}%). Sample record:\n\n")
                f.write("```json\n")
                f.write(json.dumps(missing_records[0], indent=2))
                f.write("\n```\n\n")

        # Falsy Values Analysis
        f.write("## Falsy Values Analysis\n\n")

        # Regular fields
        for field in expected_fields:
            if field in ['location', 'hours']:
                continue
            
            falsy_records = [item for item in data if field in item and not item[field]]
            if falsy_records:
                f.write(f"### {field}\n\n")
                f.write(f"Found {len(falsy_records)} records with falsy values. ")
                f.write(f"Showing first 5:\n\n")
                for i, record in enumerate(falsy_records[:5]):
                    f.write(f"#### Record {i+1}\n```json\n")
                    f.write(json.dumps(record, indent=2))
                    f.write("\n```\n\n")

        # Location field
        f.write("### location\n\n")
        falsy_locations = [item for item in data if 'location' in item and is_falsy_location(item['location'])]
        if falsy_locations:
            f.write(f"Found {len(falsy_locations)} records with invalid location data. ")
            f.write(f"Showing first 5:\n\n")
            for i, record in enumerate(falsy_locations[:5]):
                f.write(f"#### Record {i+1}\n```json\n")
                f.write(json.dumps(record, indent=2))
                f.write("\n```\n\n")

        # Hours field
        f.write("### hours\n\n")
        falsy_hours = [item for item in data if 'hours' in item and is_falsy_hours(item['hours'])]
        if falsy_hours:
            f.write(f"Found {len(falsy_hours)} records with invalid hours data. ")
            f.write(f"Showing first 5:\n\n")
            for i, record in enumerate(falsy_hours[:5]):
                f.write(f"#### Record {i+1}\n```json\n")
                f.write(json.dumps(record, indent=2))
                f.write("\n```\n\n")

        # Hours by day analysis
        f.write("### Hours by Day Analysis\n\n")
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        
        for day in days:
            f.write(f"#### {day.capitalize()}\n\n")
            day_falsy = [
                item for item in data 
                if 'hours' in item 
                and isinstance(item['hours'], dict) 
                and day in item['hours'] 
                and is_day_falsy(item['hours'][day])
            ]
            
            if day_falsy:
                f.write(f"Found {len(day_falsy)} records with invalid hours for {day}. ")
                f.write("Showing first 2:\n\n")
                for i, record in enumerate(day_falsy[:2]):
                    f.write(f"Record {i+1}:\n```json\n")
                    f.write(json.dumps(record, indent=2))
                    f.write("\n```\n\n")
            else:
                f.write("No invalid hours found for this day.\n\n")

        # Random Sample Records
        f.write("## Random Sample Records\n\n")
        f.write("Here are 5 random records from the dataset:\n\n")
        sample_size = min(5, len(data))
        sample_records = random.sample(data, sample_size)
        for i, record in enumerate(sample_records, 1):
            f.write(f"### Sample Record {i}\n\n")
            f.write("```json\n")
            f.write(json.dumps(record, indent=2))
            f.write("\n```\n\n")

def generate_summary_report(file_data: Dict[str, List[Dict[str, Any]]], output_dir: str):
    """Generate a summary report for all data sources"""
    output_file = Path(output_dir) / "00_summary_report.md"
    
    with open(output_file, 'w') as f:
        f.write("# Data Quality Summary Report\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Overall statistics
        total_records = sum(len(records) for records in file_data.values())
        f.write("## Overall Statistics\n\n")
        f.write("| Source File | Record Count |\n")
        f.write("|-------------|--------------|")
        for filename, records in file_data.items():
            f.write(f"\n|{filename}|{len(records)}|")
        f.write(f"\n|**Total**|**{total_records}**|\n\n")

def main():
    # Configuration
    file_patterns = [
        "data/v4 - 11_07_2024/*.json",          # Process all JSON files in data directory
        # "other_data/*.json"     # Process all JSON files in other_data directory
    ]
    output_dir = "reports"
    
    # Load all data
    file_data = load_json_files(file_patterns)
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Generate individual reports
    for filename, data in file_data.items():
        print(f"Generating report for {filename}...")
        generate_single_source_report(filename, data, output_dir)
    
    # Generate summary report
    print("Generating summary report...")
    generate_summary_report(file_data, output_dir)
    
    print(f"\nReports generated in '{output_dir}' directory.")

if __name__ == "__main__":
    main()