import requests
import time
import json
import io

# --- Authentication Function (Placeholder) ---
# As requested, this is a placeholder for your actual authentication function.
# It should return the base API URL and a valid authentication token.
def auth_func():
    """
    Placeholder for your authentication function.
    Replace this with your actual implementation that retrieves
    the API base URL and a valid token.
    """
    # Example values - replace with your actual configuration
    base_api_url = "https://api2.prismacloud.io"
    auth_token = "your-prisma-cloud-auth-token" # Replace with a real token or token retrieval logic
    return base_api_url, auth_token

# --- Main Logic ---

def submit_and_download_csv(base_url, headers, alert_filter_payload):
    """
    Submits a job to generate an alert CSV, polls for completion,
    and downloads the resulting data as a string.

    Args:
        base_url (str): The base URL for the Prisma Cloud API.
        headers (dict): The request headers, including the auth token.
        alert_filter_payload (dict): The filter payload for the alert CSV job.

    Returns:
        str: A string containing the CSV data, or None on failure.
    """
    session = requests.Session()
    session.headers.update(headers)

    # Step 1: Submit the alert CSV generation job
    submit_url = f"{base_url}/alert/csv"
    print(f"Submitting CSV generation job with filter: {json.dumps(alert_filter_payload)}")
    try:
        response = session.post(submit_url, json=alert_filter_payload)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        job_details = response.json()
        job_id = job_details.get("id")
        status_uri = job_details.get("statusUri")
        if not job_id or not status_uri:
            print("Error: Failed to get job ID or status URI from submission response.")
            return None
        print(f"Successfully submitted job. Job ID: {job_id}")
    except requests.exceptions.RequestException as e:
        print(f"Error submitting CSV job: {e}")
        return None
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON response from job submission.")
        return None


    # Step 2: Get the status of the job, wait if necessary
    status_url = f"{base_url}{status_uri}"
    download_uri = None
    while True:
        print(f"Checking status for job ID: {job_id}...")
        try:
            response = session.get(status_url)
            response.raise_for_status()
            status_details = response.json()
            job_status = status_details.get("status")

            print(f"Job status is: {job_status}")

            if job_status == "READY_TO_DOWNLOAD":
                download_uri = status_details.get("downloadUri")
                if not download_uri:
                    print("Error: Job is ready, but no download URI was provided.")
                    return None
                print("Job is ready for download.")
                break
            elif job_status == "IN_PROGRESS":
                print("Job is in progress. Waiting 5 seconds...")
                time.sleep(5)
            else:
                print(f"Error: Job failed or entered an unexpected state: {job_status}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"Error checking job status: {e}")
            return None
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON response from status check.")
            return None


    # Step 3: Download the CSV
    download_url = f"{base_url}{download_uri}"
    print(f"Downloading CSV from: {download_url}")
    try:
        response = session.get(download_url)
        response.raise_for_status()
        csv_data = response.text
        if not csv_data:
            print("Warning: Downloaded CSV is empty.")
            return "" # Return an empty string for empty files
        
        print(f"Successfully downloaded CSV data.")
        return csv_data
    except requests.exceptions.RequestException as e:
        print(f"Error downloading CSV: {e}")
        return None


def main():
    """
    Main function to orchestrate the download and combination of CSVs.
    """
    try:
        base_api_url, token = auth_func()
    except Exception as e:
        print(f"An error occurred in the authentication function: {e}")
        return

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json;charset=UTF-8",
        "x-redlock-auth": token
    }

    # --- Define the two different filters for the download jobs ---
    
    # Filter 1: Critical severity alerts (from HAR file)
    filter_1 = {
        "filters": [
            {"name": "alert.status", "operator": "=", "value": "open"},
            {"name": "timeRange.type", "operator": "=", "value": "ALERT_OPENED"},
            {"name": "policy.severity", "operator": "=", "value": "critical"}
        ],
        "timeRange": {"type": "to_now", "value": "epoch"}
    }

    # Filter 2: High severity alerts (example for the second required filter)
    filter_2 = {
        "filters": [
            {"name": "alert.status", "operator": "=", "value": "open"},
            {"name": "timeRange.type", "operator": "=", "value": "ALERT_OPENED"},
            {"name": "policy.severity", "operator": "=", "value": "high"}
        ],
        "timeRange": {"type": "to_now", "value": "epoch"}
    }

    # --- Process both filters ---
    print("--- Starting process for Filter 1 (Critical Alerts) ---")
    csv_1_str = submit_and_download_csv(base_api_url, headers, filter_1)

    print("\n--- Starting process for Filter 2 (High Alerts) ---")
    csv_2_str = submit_and_download_csv(base_api_url, headers, filter_2)

    # --- Combine the CSV outputs ---
    
    # Collect all non-empty CSV strings
    all_csvs = []
    if csv_1_str:
        all_csvs.append(csv_1_str)
    if csv_2_str:
        all_csvs.append(csv_2_str)

    if not all_csvs:
        print("\nNo data was downloaded. Exiting.")
        return

    print("\nCombining downloaded CSV data...")

    # Take the header from the first CSV and all its data rows
    first_csv_lines = all_csvs[0].strip().split('\n')
    header = first_csv_lines[0]
    combined_rows = first_csv_lines[1:] # Get data rows from the first file

    # For subsequent CSVs, skip the header and append only the data rows
    for csv_str in all_csvs[1:]:
        lines = csv_str.strip().split('\n')
        combined_rows.extend(lines[1:])

    # Reconstruct the final CSV string
    final_csv_output = header + '\n' + '\n'.join(combined_rows)
    
    print(f"Successfully combined data. Total rows (excluding header): {len(combined_rows)}")
    
    # The 'final_csv_output' variable now holds the final, merged CSV as a single string.
    # You can now proceed with your SharePoint upload logic using this variable.
    print("\nFinal combined CSV string is ready for SharePoint upload.")
    # print(final_csv_output[:1000]) # Print first 1000 characters as a sample


if __name__ == "__main__":
    main()
