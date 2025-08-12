import requests
import csv
import json

from auth. import auth_func # token, cspm_url, cwp_url

def get_all_alerts(cspm_url, token):
    """
    Retrieves all open alerts from the Prisma Cloud API based on specified filters,
    using the GET method and refreshing the token every 5 requests.

    Args:
        cspm_url (str): The URL of the Prisma Cloud API.
        token (str): The authentication token.

    Returns:
        list: A list of all retrieved alerts.
    """
    url = f"{cspm_url}/v2/alert"
    
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "x-redlock-auth": token
    }

    query_params_list = [
        # Query 1: by policy type
        {"alert.status": "open", "policy.type": ["attack_path", "config", "network"], "detailed": "true"},
        # Query 2: by policy label
        {"alert.status": "open", "policy.label": "bar", "detailed": "true"}
    ]

    all_alerts = []
    request_count = 0
    
    for i, params in enumerate(query_params_list):
        print(f"\n--- Running Query {i+1} ---")
        print(f"Filters: {params}")
        
        page_token = None
        
        while True:
            current_params = params.copy()
            if page_token:
                current_params["pageToken"] = page_token

            response = requests.get(url, headers=headers, params=current_params)
            request_count += 1
            
            response.raise_for_status()
            data = response.json()
            
            items = data.get("items", [])
            if items:
                print(f"Fetched {len(items)} alerts... (Request #{request_count})")
                all_alerts.extend(items)
            
            if request_count > 0 and request_count % 5 == 0:
                print(f"\n--- Refreshing token after {request_count} requests ---")
                try:
                    token, cspm_url, cwp_url = auth_func()
                    headers["x-redlock-auth"] = token
                    print("Token refreshed successfully for next request.")
                except Exception as e:
                    print(f"Error refreshing token: {e}")
                    break
            
            page_token = data.get("nextPageToken")
            if not page_token:
                print("No more pages for this query.")
                break
            
    return all_alerts

def write_alerts_to_csv(alerts, schema_map, filename="prisma_alerts_aggregated.csv"):
    """
    Writes a list of alerts to a CSV file based on a schema map.
    This version includes robust error handling for missing nested keys.
    """
    if not alerts:
        print("No alerts were found to write to CSV.")
        return

    unique_alerts = {alert['id']: alert for alert in alerts}.values()
    print(f"\nTotal unique alerts to export: {len(unique_alerts)}")

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(schema_map.keys())
        
        for alert in unique_alerts:
            row = []
            for json_path in schema_map.values():
                value = alert
                try:
                    # CORRECTED: This loop now safely handles missing intermediate keys.
                    for key in json_path:
                        if isinstance(value, list) and isinstance(key, int):
                           value = value[key]
                        else:
                           # The line that caused the error
                           value = value.get(key)
                        
                        # NEW: If at any point the value becomes None, stop digging deeper.
                        if value is None:
                            break
                            
                except (KeyError, IndexError, TypeError):
                    # This will catch errors like trying to index a non-list
                    value = None
                
                # Append the final value, or an empty string if it's None
                row.append(value if value is not None else "")
            writer.writerow(row)

if __name__ == "__main__":
    try:
        initial_token, initial_cspm_url, initial_cwp_url = auth_func()
    except Exception as e:
        print(f"Failed to get initial authentication: {e}")
        exit()
    
    schema_map = {
        "Alert ID": ["id"],
        "Policy Name": ["policy", "name"],
        "Policy Type": ["policy", "policyType"],
        "Description": ["policy", "description"],
        "Policy Labels": ["policy", "labels"],
        "Policy Severity": ["policy", "severity"],
        "Resource Name": ["resource", "name"],
        "Cloud Type": ["resource", "cloudType"],
        "Cloud Account Id": ["resource", "accountId"],
        "Cloud Account Name": ["resource", "account"],
        "Region": ["resource", "region"],
        "Recommendation": ["policy", "remediation", "description"],
        "Alert Status": ["status"],
        "Alert Time": ["alertTime"],
        "Event Occurred": ["history", 0, "modifiedOn"],
        "Dismissed On": ["dismissalUntilTs"],
        "Dismissed By": ["dismissedBy"],
        "Dismissal Reason": ["dismissalNote"],
        "Resolved On": ["lastUpdated"],
        "Resolution Reason": ["reason"],
        "Resource ID": ["resource", "id"],
    }
    
    alerts_data = get_all_alerts(initial_cspm_url, initial_token)
    
    write_alerts_to_csv(alerts_data, schema_map)

    print(f"\nScript finished. All data exported to 'prisma_alerts_aggregated.csv'.")
