"""
Download and Process CMS Hospital Datasets (Optimized Version)

This script performs the following tasks:
1. Fetches dataset metadata from the CMS API.
2. Filters datasets related to "Hospitals" based on their theme.
3. Downloads and processes datasets:
   - Converts column names to snake_case for consistency.
   - Saves the processed datasets as CSV files in a local directory.
   - Skips datasets not modified since the last run.
4. Maintains a metadata file (`metadata.json`) to track the last run time.

Optimizations:
- Uses in-memory file handling to reduce disk I/O.
- Implements parallel processing to speed up dataset processing.
- Configures logging for better debugging and traceability.
"""
import os
import re
import json
import requests
from io import BytesIO
from datetime import datetime
import pytz
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants
LOCAL_PROCESSED_DIR = "./processed_data/"
METADATA_FILE = "metadata.json"

# Optimized: Use a persistent session for HTTP requests
session = requests.Session()

def to_snake_case(name):
    """
    Convert a string to snake_case.
    Args:
        name (str): The input string.
    Returns:
        str: The string in snake_case format.
    """
    return re.sub(r"[^a-z0-9\s]", "", name.lower()).replace(" ", "_")

def load_metadata():
    """
    Load metadata from a JSON file.
    Returns:
        dict: A dictionary containing the metadata.
    """
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as file:
            return json.load(file)
    return {}

def save_metadata(metadata):
    """
    Save metadata to a JSON file.
    Args:
        metadata (dict): The metadata dictionary to save.
    """
    with open(METADATA_FILE, "w") as file:
        json.dump(metadata, file)

def process_dataset(dataset):
    """
    Process a single dataset by downloading, cleaning, and saving it locally.
    Args:
        dataset (dict): The dataset metadata.
    """
    dataset_id = dataset["identifier"]
    csv_url = dataset["distribution"][0]["downloadURL"]

    for attempt in range(3):  # Retry logic for transient failures
        try:
            # Optimized: Download CSV file in memory
            response = session.get(csv_url)
            response.raise_for_status()  # Raise HTTP errors if any

            # Load CSV into a DataFrame
            df = pd.read_csv(BytesIO(response.content), low_memory=False)
            df.rename(columns={col: to_snake_case(col) for col in df.columns}, inplace=True)

            # Save processed DataFrame to a CSV file
            output_path = os.path.join(LOCAL_PROCESSED_DIR, f"{dataset_id}.csv")
            df.to_csv(output_path, index=False)
            logging.info(f"Processed and saved dataset: {output_path}")
            break  # Exit loop if successful
        except requests.exceptions.RequestException as req_err:
            logging.warning(f"Network error for dataset {dataset_id} on attempt {attempt + 1}: {req_err}")
            time.sleep(5)  # Wait before retrying
        except pd.errors.ParserError as parse_err:
            logging.error(f"Parsing error for dataset {dataset_id}: {parse_err}")
            break
        except Exception as e:
            logging.error(f"Unexpected error for dataset {dataset_id}: {e}")
            break

def main():
    """
    Main function to fetch, process, and save hospital datasets from the CMS API.
    """
    # Ensure output directory exists
    os.makedirs(LOCAL_PROCESSED_DIR, exist_ok=True)

    # Fetch datasets metadata
    api_response = session.get("https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items")
    datasets = api_response.json()

    # Filter datasets related to "Hospitals"
    hospital_datasets = [d for d in datasets if "Hospitals" in d.get("theme", [])]
    logging.info(f"Found {len(hospital_datasets)} hospital datasets.")

    # Load metadata to track previously processed datasets
    metadata = load_metadata()
    last_run_time = metadata.get("last_run_time", "")

    # Filter datasets modified after the last run
    new_datasets = [d for d in hospital_datasets if d["modified"] > last_run_time]
    logging.info(f"Processing {len(new_datasets)} new or updated datasets.")

    # Process datasets in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_dataset, new_datasets)

    # Update metadata with the current run time
    metadata["last_run_time"] = datetime.now(pytz.UTC).isoformat()
    save_metadata(metadata)
    logging.info("Metadata updated.")

if __name__ == "__main__":
    main()
