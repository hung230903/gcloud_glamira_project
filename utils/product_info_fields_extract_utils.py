import json
import os

def extract_keys(obj, path=""):
    keys = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            keys.add(new_path)
            keys.update(extract_keys(v, new_path))
    elif isinstance(obj, list):
        for item in obj[:1000]: # sample first 10 items in lists
            keys.update(extract_keys(item, f"{path}[]"))
    return keys

def main():
    file_path = "/home/hung/Documents/PycharmProject/gcloud_glamira_project/data/product_info/success/product_info_1.json"
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    all_keys = set()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Since it's a JSON array, we can try to read a chunk and handle it
            # But let's just use json.load if memory permits, or sample it
            data = json.load(f)
            for item in data[:500]: # check first 500 products
                all_keys.update(extract_keys(item))
    except Exception as e:
        print(f"Error reading file: {e}")
        return

    print("All fields found in product_info data (first 500 items):")
    for key in sorted(all_keys):
        print(key)

if __name__ == "__main__":
    main()
