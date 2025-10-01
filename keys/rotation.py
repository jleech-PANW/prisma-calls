# prisma_key_rotator.py
import os
import sys
import requests
import json
from datetime import datetime, timezone

# --- Configuration ---
# Load credentials and API endpoint from environment variables
PRISMA_API_URL = os.getenv("PRISMA_API_URL")
ACCESS_KEY_ID = os.getenv("PRISMA_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("PRISMA_SECRET_KEY")

# --- Helper Functions ---

def get_auth_token():
    """Authenticates with Prisma Cloud and returns a session token."""
    url = f"{PRISMA_API_URL}/login"
    payload = {
        "username": ACCESS_KEY_ID,
        "password": SECRET_KEY
    }
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return response.json().get("token")
    except requests.exceptions.RequestException as e:
        print(f"Error authenticating with Prisma Cloud: {e}", file=sys.stderr)
        sys.exit(1)

def get_all_keys(token):
    """Retrieves all access keys for the current service account."""
    url = f"{PRISMA_API_URL}/access-keys"
    headers = {"x-redlock-auth": token}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving access keys: {e}", file=sys.stderr)
        return []

def delete_inactive_keys(token, keys):
    """Finds and deletes any inactive keys."""
    inactive_keys = [key for key in keys if key.get("status") == "INACTIVE"]
    if not inactive_keys:
        print("No inactive keys found to delete.")
        return

    key_ids_to_delete = [key['id'] for key in inactive_keys]
    print(f"Found {len(key_ids_to_delete)} inactive keys to delete: {key_ids_to_delete}")
    
    url = f"{PRISMA_API_URL}/access-keys"
    headers = {"x-redlock-auth": token, "Content-Type": "application/json"}
    payload = {"ids": key_ids_to_delete}
    
    try:
        response = requests.request("DELETE", url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        print("Successfully deleted inactive keys.")
    except requests.exceptions.RequestException as e:
        print(f"Error deleting inactive keys: {e}", file=sys.stderr)
        # Continue execution even if deletion fails

def create_new_key(token):
    """Creates a new access key for the service account."""
    url = f"{PRISMA_API_URL}/access-keys"
    headers = {"x-redlock-auth": token}
    
    # The name helps identify keys created by the rotation script
    key_name = f"vault-rotated-key-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    payload = {"name": key_name}
    
    try:
        print("Creating a new access key...")
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        new_key = response.json()
        print(f"Successfully created new access key with ID: {new_key['id']}")
        return new_key
    except requests.exceptions.RequestException as e:
        print(f"FATAL: Could not create new access key: {e}", file=sys.stderr)
        sys.exit(1) # Exit if we can't create a new key

def deactivate_key(token, key_id):
    """Sets the status of a given key ID to INACTIVE."""
    url = f"{PRISMA_API_URL}/access-keys/{key_id}"
    headers = {"x-redlock-auth": token, "Content-Type": "application/json"}
    payload = {"status": "INACTIVE"}

    try:
        print(f"Deactivating previous access key with ID: {key_id}")
        response = requests.patch(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        print("Successfully deactivated previous key.")
    except requests.exceptions.RequestException as e:
        # This is not a fatal error, as the new key is already active
        print(f"Warning: Failed to deactivate previous key {key_id}: {e}", file=sys.stderr)

# --- Main Execution Logic ---

def main():
    """Main function to orchestrate the key rotation process."""
    if not all([PRISMA_API_URL, ACCESS_KEY_ID, SECRET_KEY]):
        print("Error: Required environment variables (PRISMA_API_URL, PRISMA_ACCESS_KEY_ID, PRISMA_SECRET_KEY) are not set.", file=sys.stderr)
        sys.exit(1)

    # 1. Get auth token
    auth_token = get_auth_token()

    # 2. Get all existing keys
    all_keys = get_all_keys(auth_token)
    
    # 3. Find the currently active key (this will become the 'old' key)
    # Note: A service account should ideally have only one active key at a time.
    # If multiple are active, this script will pick the first one it finds.
    previous_active_key = next((key for key in all_keys if key.get("status") == "ACTIVE"), None)

    # 4. Clean up any keys that are already inactive from previous rotations
    delete_inactive_keys(auth_token, all_keys)

    # 5. Create the new key. This is the most critical step.
    new_key = create_new_key(auth_token)

    # 6. Deactivate the old key, if one existed
    if previous_active_key:
        deactivate_key(auth_token, previous_active_key['id'])
    else:
        print("No previously active key found to deactivate.")

    # 7. Output the new credentials in JSON format for Vault
    # Vault will read this from stdout and update its secret store.
    output = {
        "id": new_key["id"],
        "secretKey": new_key["secretKey"]
    }
    print(json.dumps(output))

if __name__ == "__main__":
    main()
