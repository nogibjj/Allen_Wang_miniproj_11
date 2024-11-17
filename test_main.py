import requests
import os
import json
from dotenv import load_dotenv

def main_test(file_name):
    load_dotenv()
    server_h = os.getenv("SERVER_HOSTNAME")
    access_token = os.getenv("ACCESS_TOKEN")
    FILESTORE_PATH = "dbfs:/FileStore/Allen_mini_project11"
    headers = {'Authorization': f'Bearer {access_token}'}

    # Construct the API URL for listing files in the Filestore path
    url = f"https://{server_h}/api/2.0/dbfs/list"

    # Request payload
    payload = {
        "path": FILESTORE_PATH
    }

    try:
        # Make the API request
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise an error for non-200 status codes

        # Parse the response
        data = response.json()
        files = [file["path"] for file in data.get("files", [])]

        # Check if the file exists
        if any(file_name in file for file in files):
            print(f"File '{file_name}' exists in the Databricks Filestore path.")
        else:
            print(f"File '{file_name}' does NOT exist in the Databricks Filestore path.")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while accessing Databricks API: {e}")

if __name__ == "__main__":
    main_test("zw308_drink.csv")
    main_test("zw308_drug_use.csv")