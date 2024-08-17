import json

def load_data(filename):
    with open(filename, 'r') as file:
        return json.load(file)

def save_data(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=2)

def find_duplicates(data):
    seen = set()
    duplicates = []
    for i, item in enumerate(data):
        # Convert the entire dictionary to a JSON string for hashing
        item_hash = json.dumps(item, sort_keys=True)
        if item_hash in seen:
            duplicates.append(i)
        else:
            seen.add(item_hash)
    return duplicates

def main():
    filename = input("Enter the JSON file name to read: ")
    try:
        data = load_data(filename)
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: '{filename}' is not a valid JSON file.")
        return

    print(f"Loaded {len(data)} items.")
    duplicates = find_duplicates(data)
    
    if duplicates:
        print(f"Found {len(duplicates)} duplicate(s).")
        choice = input("Do you want to remove duplicates? (yes/no): ").lower()
        
        if choice == 'yes':
            data = [item for i, item in enumerate(data) if i not in duplicates]
            print(f"Removed duplicates. {len(data)} items remaining.")
            
            save_choice = input("Do you want to save the updated data? (yes/no): ").lower()
            if save_choice == 'yes':
                output_filename = input("Enter the output file name: ")
                try:
                    save_data(data, output_filename)
                    print(f"Data saved to {output_filename}")
                except IOError:
                    print(f"Error: Unable to write to file '{output_filename}'.")
        else:
            print("No changes made.")
    else:
        print("No duplicates found.")

if __name__ == "__main__":
    main()