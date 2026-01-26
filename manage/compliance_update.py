import requests
import json
from auth.panw import auth_func

# --- CONFIGURATION ---
policy_label = "jleech-test"
compliance_standard = "jleech test"          # e.g., "NIST 800-53"
compliance_requirement = "1"     # e.g., "CM-6" (Required by API)
compliance_section = "1.1"         # e.g., "Configuration Settings" (Required by API)

def get_list():
    token, cspm_url, cwp_url = auth_func()
    base_url = f'{cspm_url}/v2/policy'
    payload = {}
    parameters = {
        "policy.label": policy_label
    }
    headers = {
        "x-redlock-auth": token,
        "Accept": "application/json"
    }

    response = requests.request("GET", base_url, headers=headers, data=payload, params=parameters)
    response.raise_for_status() 
    return response.json()

def update_policies():
    print(f"Fetching policies with label: {policy_label}...")
    policies = get_list()
    
    if not policies:
        print("No policies found.")
        return

    token, cspm_url, cwp_url = auth_func()
    
    headers = {
        "x-redlock-auth": token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    for policy in policies:
        policy_id = policy['policyId']
        policy_name = policy['name']
        
        print(f"Processing policy: {policy_name}")

        # 1. Prepare the new metadata entry
        new_compliance_data = {
            "standardName": compliance_standard,
            "requirementId": compliance_requirement,
            "sectionId": compliance_section,
            "customAssigned": True
        }

        # 2. Initialize complianceMetadata if it doesn't exist
        if 'complianceMetadata' not in policy:
            policy['complianceMetadata'] = []

        # 3. Check for duplicates to avoid adding the same standard twice
        # We check if this specific Standard + Section combo already exists
        exists = False
        for meta in policy['complianceMetadata']:
            if (meta.get('standardName') == compliance_standard and 
                meta.get('sectionId') == compliance_section):
                exists = True
                break
        
        if exists:
            print(f" - Skipped: Standard '{compliance_standard}' already assigned.")
            continue

        # 4. Append new data
        policy['complianceMetadata'].append(new_compliance_data)

        # 5. PUT the update
        # We use cspm_url because these are Config policies
        update_url = f'{cspm_url}/v2/policy/{policy_id}'
        
        try:
            update_response = requests.put(update_url, headers=headers, json=policy)
            update_response.raise_for_status()
            print(f" - Success: Updated policy {policy_id}")
        except requests.exceptions.HTTPError as e:
            print(f" - Error updating policy: {e.response.text}")

# Run the update
update_policies()
