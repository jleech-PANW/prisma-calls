import json
import logging
import requests
import os

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
VAULT_EXTENSION_ADDR = "http://127.0.0.1:8200"
CONFIG_FILE = "secrets_config.json"

def get_vault_secret(path):
    """Retrieves secret data from the Vault Lambda Extension."""
    # The path in config might be 'secret/data/my-app'. 
    # The extension expects the full API path: /v1/secret/data/my-app
    full_url = f"{VAULT_EXTENSION_ADDR}/v1/{path}"
    
    headers = {
        "X-Vault-Request": "true"
    }
    
    try:
        response = requests.get(full_url, headers=headers)
        response.raise_for_status()
        return response.json()['data']['data']
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to retrieve secret from Vault at {path}: {str(e)}")
        raise

def update_vault_secret(path, new_data):
    """Updates the secret in Vault via the Extension."""
    full_url = f"{VAULT_EXTENSION_ADDR}/v1/{path}"
    
    headers = {
        "X-Vault-Request": "true",
        "Content-Type": "application/json"
    }
    
    # Vault KV v2 expects payload in {"data": {...}} format
    payload = {"data": new_data}
    
    try:
        response = requests.post(full_url, headers=headers, json=payload)
        response.raise_for_status()
        logger.info(f"Successfully updated Vault secret at {path}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to update Vault secret at {path}: {str(e)}")
        raise

def prisma_login(api_url, access_key, secret_key):
    """Logs into Prisma Cloud and returns the JWT token."""
    url = f"{api_url}/login"
    payload = {
        "username": access_key,
        "password": secret_key
    }
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("token")
    except requests.exceptions.RequestException as e:
        logger.error(f"Prisma Login failed for key {access_key}: {str(e)}")
        raise

def process_rotation(config_item):
    vault_path = config_item['vault_secret_path']
    prisma_url = config_item['prisma_api_url']
    
    logger.info(f"Starting rotation for {vault_path} on {prisma_url}")
    
    # 1. Retrieve current credentials from Vault
    current_secrets = get_vault_secret(vault_path)
    old_access_key = current_secrets.get('access_key_id')
    old_secret_key = current_secrets.get('secret_key')
    
    if not old_access_key or not old_secret_key:
        raise ValueError("Vault secret missing 'access_key_id' or 'secret_key'")

    # 2. Login with Old Credentials (Session A)
    logger.info("Authenticating with current credentials...")
    token_a = prisma_login(prisma_url, old_access_key, old_secret_key)
    headers_a = {"x-redlock-auth": token_a, "Content-Type": "application/json"}

    # 3. List Keys and Cleanup Inactive
    logger.info("Fetching access keys...")
    keys_resp = requests.get(f"{prisma_url}/access_keys", headers=headers_a)
    keys_resp.raise_for_status()
    all_keys = keys_resp.json()
    
    for key in all_keys:
        if not key['status']: # If status is False (Inactive)
            logger.info(f"Deleting inactive key: {key['id']}")
            del_resp = requests.delete(f"{prisma_url}/access_keys/{key['id']}", headers=headers_a)
            del_resp.raise_for_status()

    # 4. Generate New Access Key
    logger.info("Generating new access key...")
    # Name the key specifically or use default. Assuming default handling.
    new_key_payload = {
        "name": f"rotation-lambda-{old_access_key[:4]}" # Example naming convention
    }
    create_resp = requests.post(f"{prisma_url}/access_keys", json=new_key_payload, headers=headers_a)
    create_resp.raise_for_status()
    new_key_data = create_resp.json()
    
    new_access_id = new_key_data['id']
    new_secret_key = new_key_data['secretKey']
    
    logger.info(f"New key generated: {new_access_id}")

    # 5. Verify New Key (Session B)
    # CRITICAL: Before we disable the old key, we must prove the new key works.
    try:
        logger.info("Verifying new credentials...")
        token_b = prisma_login(prisma_url, new_access_id, new_secret_key)
        headers_b = {"x-redlock-auth": token_b, "Content-Type": "application/json"}
    except Exception as e:
        logger.error("New key validation failed! Attempting to delete new key to maintain state.")
        # Rollback: Delete the created key using Session A
        requests.delete(f"{prisma_url}/access_keys/{new_access_id}", headers=headers_a)
        raise RuntimeError("New key generation failed validation. Rolled back.") from e

    # 6. Update Vault
    # We update Vault now. If this fails, we have two active keys, which is safe.
    logger.info("Updating Vault secret...")
    new_vault_data = current_secrets.copy()
    new_vault_data['access_key_id'] = new_access_id
    new_vault_data['secret_key'] = new_secret_key
    update_vault_secret(vault_path, new_vault_data)

    # 7. Set Old Key to Inactive using New Session (Session B)
    # We use Session B to prove we really have control with the new key.
    logger.info(f"Deactivating old key {old_access_key}...")
    patch_payload = {"status": False}
    patch_resp = requests.patch(
        f"{prisma_url}/access_keys/{old_access_key}", 
        json=patch_payload, 
        headers=headers_b
    )
    patch_resp.raise_for_status()
    
    logger.info("Rotation complete.")

def lambda_handler(event, context):
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config file: {e}")
        return {"statusCode": 500, "body": "Config Error"}

    results = []
    
    for item in config:
        try:
            process_rotation(item)
            results.append({"path": item['vault_secret_path'], "status": "Success"})
        except Exception as e:
            logger.error(f"Rotation failed for {item.get('vault_secret_path')}: {e}")
            results.append({"path": item.get('vault_secret_path'), "status": "Failed", "error": str(e)})

    return {
        "statusCode": 200,
        "body": json.dumps(results)
    }
