import json
import jsonlines
import sqlite3
import argparse
from tqdm import tqdm

def read_jsonl(file_path):
    data = []
    with jsonlines.open(file_path) as reader:
        for obj in reader:
            data.append(obj)
    return data

def determine_status(error):
    # List of error messages that should be marked as 'completed'
    completed_errors = [
        "'NoneType' object is not subscriptable",
        # Add any other error messages that should be treated as 'completed'
    ]
    
    if any(msg in error.get('error', '') for msg in completed_errors):
        return 'completed'
    else:
        return 'error'

def update_database(db_file, error_data):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Ensure the table exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scrape_progress
        (store_id TEXT PRIMARY KEY, status TEXT)
    ''')

    # Prepare the update query
    update_query = '''
        INSERT OR REPLACE INTO scrape_progress (store_id, status)
        VALUES (?, ?)
    '''

    # Update the database
    completed_count = 0
    error_count = 0
    for error in tqdm(error_data, desc="Updating database"):
        status = determine_status(error)
        cursor.execute(update_query, (error['store_id'], status))
        if status == 'completed':
            completed_count += 1
        else:
            error_count += 1

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    return completed_count, error_count

def main(error_log_file, db_file):
    # Read the error log file
    error_data = read_jsonl(error_log_file)

    # Update the database
    completed_count, error_count = update_database(db_file, error_data)

    print(f"Updated {len(error_data)} store statuses in the database.")
    print(f"Marked as completed: {completed_count}")
    print(f"Marked as error: {error_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update error statuses in the database")
    parser.add_argument("--error-log", required=True, help="Input error log file (JSONL)")
    parser.add_argument("--db", required=True, help="SQLite database file")
    args = parser.parse_args()

    main(args.error_log, args.db)