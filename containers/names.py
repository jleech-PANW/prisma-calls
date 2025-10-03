import requests
import csv
import sys
import os
from auth.intact import auth_func

# --- Main Script Logic ---
def get_all_container_names(token, cwp_url):
    """
    Fetches all container names from the Prisma Cloud API using offset pagination.
    """
    all_names = []
    offset = 0
    limit = 50  # Max limit per the API documentation
    page_num = 1

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    api_url = f"{cwp_url}/api/v1/containers/names"

    print("🚀 Starting to fetch container names...")

    while True:
        params = {
            'offset': offset,
            'limit': limit
        }
        
        try:
            print(f"Fetching page {page_num} (offset: {offset})...")
            response = requests.get(api_url, headers=headers, params=params)
            response.raise_for_status()  # Raises an exception for bad responses (4xx or 5xx)

            data = response.json()

            # If the API returns an empty list, we've fetched all records.
            if not data:
                print("No more data to fetch. All pages retrieved.")
                break

            all_names.extend(data)
            
            # If the number of results is less than the limit, it's the last page.
            if len(data) < limit:
                print(f"Last page reached with {len(data)} items.")
                break

            # Prepare for the next page
            offset += limit
            page_num += 1

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}", file=sys.stderr)
            print(f"Response body: {response.text}", file=sys.stderr)
            break
        except requests.exceptions.RequestException as req_err:
            print(f"An error occurred during the request: {req_err}", file=sys.stderr)
            break

    return all_names

def write_to_csv(container_names, filename="container_names.csv"):
    """
    Writes a list of container names to a CSV file.
    """
    if not container_names:
        print("No container names to write to CSV.")
        return

    print(f"\nWriting {len(container_names)} container names to '{filename}'...")
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Container Name'])  # Write header
            for name in container_names:
                writer.writerow([name])
        print(f"Successfully created '{filename}'.")
    except IOError as e:
        print(f"Error writing to file '{filename}': {e}", file=sys.stderr)

if __name__ == "__main__":
    # 1. Get credentials from the authentication function
    access_token, _, compute_url = auth_func()

    # 2. Fetch all container names from the API
    names = get_all_container_names(access_token, compute_url)
    
    # 3. Write the results to a CSV file if any names were found
    if names:
        print(f"\nTotal unique container names found: {len(names)}")
        write_to_csv(names)
    else:
        print("\nNo container names were found in the environment.")
