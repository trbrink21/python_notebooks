import requests
import os
import pandas as pd
import re
import json
import hashlib
import concurrent.futures
from datetime import datetime
from urllib.parse import urljoin

# COMMAND ----------

# Configuration values
API_URL = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items'
DOWNLOAD_DIR = './datasets'
METADATA_FILE = 'metadata.json'
THEME = 'Hospitals'

# COMMAND ----------

# Convert column names to snake_case
def to_snake_case(name):
    name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)
    name = re.sub(r'\W+', '_', name)  # Remove non-alphanumeric characters
    name = name.strip('_').lower()  # Remove leading/trailing underscores and convert to lowercase
    return name

# COMMAND ----------

# Check if the file has been modified
def file_modified_since_last_run(dataset, metadata):
    last_run = metadata.get('last_run', {})
    dataset_id = dataset.get('id')
    dataset_last_modified = dataset.get('last_modified')
    
    if dataset_id in last_run:
        last_download_time = last_run[dataset_id]
        return datetime.strptime(dataset_last_modified, '%Y-%m-%dT%H:%M:%S') > last_download_time
    return True

# COMMAND ----------

# Download dataset CSV and process it
def download_and_process(dataset, metadata):
    # Check if the dataset is modified since the last run
    if not file_modified_since_last_run(dataset, metadata):
        print(f"Skipping {dataset['name']} - No updates.")
        return
    
    # Download CSV file
    download_url = dataset['download_url']
    response = requests.get(download_url)
    
    if response.status_code == 200:
        print(f"Downloading {dataset['name']}...")

        # Read CSV into pandas DataFrame
        df = pd.read_csv(response.content.decode('utf-8'))
        
        # Rename columns to snake_case
        df.columns = [to_snake_case(col) for col in df.columns]
        
        # Save the processed CSV
        file_path = os.path.join(DOWNLOAD_DIR, f"{dataset['id']}.csv")
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        df.to_csv(file_path, index=False)
        print(f"Saved {dataset['name']} to {file_path}")

        # Update metadata with the download timestamp
        metadata['last_run'][dataset['id']] = datetime.strptime(dataset['last_modified'], '%Y-%m-%dT%H:%M:%S')
        with open(METADATA_FILE, 'w') as f:
            json.dump(metadata, f, default=str, indent=2)
    else:
        print(f"Failed to download {dataset['name']} - Status Code: {response.status_code}")  

# COMMAND ----------

# Main function to handle the workflow
def main():
    # Load metadata file (or create if it doesn't exist)
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, 'r') as f:
            metadata = json.load(f)
    else:
        metadata = {'last_run': {}}
    
    # Fetch the list of datasets from the CMS API
    response = requests.get(API_URL)
    if response.status_code != 200:
        print(f"Failed to retrieve datasets - Status Code: {response.status_code}")
        return
    
    datasets = response.json()['items']
    
    # Filter datasets related to 'Hospitals'
    hospital_datasets = [dataset for dataset in datasets if THEME.lower() in dataset['name'].lower()]
    
    # Download and process datasets in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for dataset in hospital_datasets:
            futures.append(executor.submit(download_and_process, dataset, metadata))
        
        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()
    
    print("Download and processing complete.")

if __name__ == "__main__":
    main()
