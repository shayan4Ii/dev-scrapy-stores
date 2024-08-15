import json
import jsonlines
import argparse
from collections import OrderedDict

def read_jsonl(file_path):
    data = []
    with jsonlines.open(file_path) as reader:
        for obj in reader:
            data.append(obj)
    return data

def write_jsonl(data, file_path):
    with jsonlines.open(file_path, mode='w') as writer:
        for item in data:
            writer.write(item)

def write_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def remove_duplicates(data, key):
    seen = set()
    return [x for x in data if x[key] not in seen and not seen.add(x[key])]

def filter_errors(error_log):
    return [error for error in error_log if "Message: unknown error: net::ERR_INTERNET_DISCONNECTED" not in error.get('error', '')]

def main(scraped_data_file, error_log_file, output_scraped_file, output_error_file):
    # Read the files
    scraped_data = read_jsonl(scraped_data_file)
    error_log = read_jsonl(error_log_file)

    # Filter out specific errors
    error_log = filter_errors(error_log)

    # Create a set of store_ids from the scraped data
    scraped_store_ids = set(str(item['number']) for item in scraped_data)

    # Filter out entries from error log that are in the scraped data
    cleaned_error_log = [error for error in error_log if error['store_id'] not in scraped_store_ids]

    # Remove duplicates from error log
    cleaned_error_log = remove_duplicates(cleaned_error_log, 'store_id')

    # Write the cleaned data back to files
    write_json(scraped_data, output_scraped_file)  # Write all scraped data
    write_jsonl(cleaned_error_log, output_error_file)

    print(f"Original scraped data count: {len(scraped_data)}")
    print(f"Cleaned scraped data count: {len(scraped_data)}")  # This will be the same as original
    print(f"Original error log count: {len(error_log)}")
    print(f"Cleaned error log count: {len(cleaned_error_log)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean up scraped data and error log")
    parser.add_argument("--scraped-data", required=True, help="Input scraped data file (JSONL)")
    parser.add_argument("--error-log", required=True, help="Input error log file (JSONL)")
    parser.add_argument("--output-scraped", required=True, help="Output cleaned scraped data file (JSON)")
    parser.add_argument("--output-error", required=True, help="Output cleaned error log file (JSONL)")
    args = parser.parse_args()

    main(args.scraped_data, args.error_log, args.output_scraped, args.output_error)