import requests
import csv
import os
import json
import sys
from auth.panw import auth_func

token, API_URL, _ = auth_func()

def get_all_policies(token):
    """Fetches all policies and returns a dictionary mapping policy name to policy data."""
    print("\nFetching all policies from Prisma Cloud (this may take a moment)...")
    url = f"{API_URL}/policy"
    headers = {
        "Content-Type": "application/json",
        "x-redlock-auth": token
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        policies = response.json()
        # Create a dictionary for quick lookups by policy name
        policy_map = {p['name']: p for p in policies}
        print(f"Found {len(policy_map)} total policies in your tenant.")
        return policy_map
    except requests.exceptions.HTTPError as err:
        print(f"❌ HTTP Error fetching policies: {err}")
        print(f"   Response Body: {err.response.text}")
    except requests.exceptions.RequestException as err:
        print(f"❌ Request failed fetching policies: {err}")
    return {}


def update_policy_with_label(token, policy_data, label_to_add):
    """Updates a given policy by adding a new label to its 'labels' list."""
    policy_id = policy_data.get('policyId')
    policy_name = policy_data.get('name')

    # Get current labels or initialize an empty list if 'labels' key doesn't exist
    current_labels = policy_data.get('labels', [])

    # Check if the label is already present
    if label_to_add in current_labels:
        print(f"⏭️  Skipping '{policy_name}': Label '{label_to_add}' already exists.")
        return "skipped"

    # Add the new label
    policy_data['labels'] = current_labels + [label_to_add]

    print(f"    - Updating '{policy_name}'...")

    url = f"{API_URL}/policy/{policy_id}"
    headers = {
        "Content-Type": "application/json",
        "x-redlock-auth": token
    }
    try:
        # The body of the PUT request must be the entire modified policy object
        response = requests.put(url, headers=headers, data=json.dumps(policy_data), timeout=10)
        response.raise_for_status()
        print(f"    ✔ Success! Added label '{label_to_add}'.")
        return "success"
    except requests.exceptions.HTTPError as err:
        print(f"    ❌ HTTP Error updating policy '{policy_name}': {err}")
        print(f"       Response Body: {err.response.text}")
    except requests.exceptions.RequestException as err:
        print(f"    ❌ Request failed updating policy '{policy_name}': {err}")
    return "failure"


# --- Main Execution ---
if __name__ == "__main__":
    print("--- Prisma Cloud Policy Label Updater ---")

    csv_file_path = "policy-list.csv"
    label_to_add = "jleech-test"

    #Read policy names from the specified CSV file
    policy_names_from_csv = []
    try:
        with open(csv_file_path, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.reader(infile)
            # Skip the header row
            next(reader, None)
            for row in reader:
                if row:  # Ensure the row is not empty
                    policy_names_from_csv.append(row[0].strip())
    except FileNotFoundError:
        print(f"Error: The file '{csv_file_path}' was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        sys.exit(1)

    if not policy_names_from_csv:
        print("⚠️ Warning: No policy names found in the CSV file.")
        sys.exit(0)

    print(f"\nFound {len(policy_names_from_csv)} policy names in '{csv_file_path}'.")

    # Authenticate and fetch all policies
    auth_token = token
    all_policies_map = get_all_policies(auth_token)

    if not all_policies_map:
        print("Exiting due to failure in fetching policies.")
        sys.exit(1)

    # Loop through CSV policies and update them
    print(f"\n⚙️  Processing policies from CSV to add label: '{label_to_add}'...")
    results = {"success": 0, "skipped": 0, "failure": 0, "not_found": 0}

    for policy_name in policy_names_from_csv:
        policy_data = all_policies_map.get(policy_name)

        if policy_data:
            # Pass a copy of the policy data to avoid modifying the original map
            status = update_policy_with_label(auth_token, policy_data.copy(), label_to_add)
            results[status] += 1
        else:
            print(f"Policy '{policy_name}' not found in Prisma Cloud. Skipping.")
            results["not_found"] += 1

    # 6. Print a final summary
    print("\n--- Summary ---")
    print(f"Successful updates: {results['success']}")
    print(f"Skipped (label already present): {results['skipped']}")
    print(f"Failed updates: {results['failure']}")
    print(f"Policies not found: {results['not_found']}")
    print("-----------------")
