import requests
import os
import json
from dotenv import load_dotenv
from base64 import b64encode


load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/Allen_mini_project11"
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"


def perform_query(path, headers, data={}):
  session = requests.Session()
  resp = session.request('POST', url + path, data=json.dumps(data), verify=True, headers=headers)
  return resp.json()

def mkdirs(path, headers):
  _data = {}
  _data['path'] = path
  return perform_query('/dbfs/mkdirs', headers=headers, data=_data)

def create(path, overwrite, headers):
  _data = {}
  _data['path'] = path
  _data['overwrite'] = overwrite
  return perform_query('/dbfs/create', headers=headers, data=_data)

def add_block(handle, data, headers):
  _data = {}
  _data['handle'] = handle
  _data['data'] = data
  return perform_query('/dbfs/add-block', headers=headers, data=_data)

def close(handle, headers):
  _data = {}
  _data['handle'] = handle
  return perform_query('/dbfs/close', headers=headers, data=_data)

def put_file(src_path, dbfs_path, overwrite, headers):
    # Get the file content from the URL
    print(f"Downloading file from {src_path}")
    response = requests.get(src_path)
    
    if response.status_code != 200:
        raise Exception(f"Failed to download file from URL: {src_path}")
    
    # Get the content of the file
    file_content = response.content
    
    # Create file in Databricks
    handle = create(dbfs_path, overwrite, headers=headers)['handle']
    print(f"Putting file: {dbfs_path}")
    
    # Add content to Databricks
    while len(file_content) > 0:
        # Chunk the file content to upload
        chunk = file_content[:2**20]  # 1 MB chunks
        file_content = file_content[2**20:]  # Remaining content
        add_block(handle, b64encode(chunk).decode(), headers=headers)
    
    # Close the file in Databricks
    close(handle, headers=headers)

def extract(url="https://raw.githubusercontent.com/fivethirtyeight/data/master/alcohol-consumption/drinks.csv",
        url2="https://raw.githubusercontent.com/fivethirtyeight/data/master/drug-use-by-age/drug-use-by-age.csv",
        file_path=FILESTORE_PATH+"/zw_308_drink.csv",
        file_path2=FILESTORE_PATH+"/zw_308_drug_use.csv",
        directory=FILESTORE_PATH,
        overwrite=True):
        mkdirs(path=directory, headers=headers)
        put_file(url, file_path, overwrite, headers=headers)
        put_file(url2, file_path2, overwrite, headers=headers)


if __name__ == "__main__":
    extract()
