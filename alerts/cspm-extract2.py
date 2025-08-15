import requests
import time
import json
import io
import pandas as pd
from auth import auth_func

# --- Main Logic ---

def submit_and_download_csv(base_url, headers, alert_filter_payload):
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
        # Use pandas to read the CSV content directly from the response text
        # This keeps the entire process in memory
        csv_data = response.text
        if not csv_data:
            print("Warning: Downloaded CSV is empty.")
            return pd.DataFrame() # Return an empty DataFrame
        
        # Use io.StringIO to treat the string data as a file
        csv_file = io.StringIO(csv_data)
        df = pd.read_csv(csv_file)
        print(f"Successfully downloaded and parsed CSV. Found {len(df)} rows.")
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error downloading CSV: {e}")
        return None
    except pd.errors.ParserError as e:
        print(f"Error parsing CSV data with pandas: {e}")
        return None


def main():
    """
    Main function to orchestrate the download and combination of CSVs.
    """
    try:
        token, base_api_url = auth_func()
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
    df1 = submit_and_download_csv(base_api_url, headers, filter_1)

    print("\n--- Starting process for Filter 2 (High Alerts) ---")
    df2 = submit_and_download_csv(base_api_url, headers, filter_2)

    # --- Combine the CSV outputs ---
    dataframes_to_combine = []
    if df1 is not None and not df1.empty:
        dataframes_to_combine.append(df1)
    if df2 is not None and not df2.empty:
        dataframes_to_combine.append(df2)

    if not dataframes_to_combine:
        print("\nNo data was downloaded. Exiting.")
        return

    print("\nCombining downloaded CSV data...")
    # Use pd.concat to merge the DataFrames.
    # ignore_index=True resets the index of the combined DataFrame.
    combined_df = pd.concat(dataframes_to_combine, ignore_index=True)

    print(f"Successfully combined data. Total rows: {len(combined_df)}")
    

if __name__ == "__main__":
    main()
