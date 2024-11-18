import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/Allen_mini_project11/zw_308_transformed_drink"
url = f"https://{server_h}/api/2.0"

# Function to check if a file path exists and if the authorization settings still work
def check_filestore_path(path, headers):
    """
    Checks if the specified path exists in the Databricks Filestore.
    """
    try:
        # Make a GET request to check the file path status
        response = requests.get(f"{url}/dbfs/get-status?path={path}", headers=headers)
        
        # Raise an error for bad responses
        response.raise_for_status()
        
        # Check if path is found in the response
        if response.json().get('path') is not None:
            print(f"File path '{path}' exists in Databricks Filestore.")
            return True
        else:
            print(f"File path '{path}' does not exist.")
            return False
    except requests.exceptions.RequestException as e:
        # Print detailed error message
        print(f"Error checking file path: {e}")
        return False

# Test if the specified FILESTORE_PATH exists
def test_databricks():
    """
    Test function to check if the FILESTORE_PATH exists.
    """
    headers = {'Authorization': f'Bearer {access_token}'}
    
    # Perform the path check
    file_exists = check_filestore_path(FILESTORE_PATH, headers)
    
    assert file_exists
    # Assert if the file exists (or could handle failure more gracefully)
    if file_exists:
        print("Test Passed: File exists.")
    else:
        print("Test Failed: File does not exist.")
        
if __name__ == "__main__":
    test_databricks()
