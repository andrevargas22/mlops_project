import json
import os
import pandas as pd
from google.cloud import storage
from datetime import datetime

# Configurações do Google Cloud
GCP_BUCKET = 'mlops_energy_consumption_data'
GCP_FOLDER = 'energy_data'

def authenticate_with_gcs():
    """
    Authenticate with Google Cloud Storage using service account credentials from an environment variable.
    
    Returns:
    storage.Client: Authenticated Google Cloud Storage client.
    """
    service_account_info = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
    storage_client = storage.Client.from_service_account_info(service_account_info)
    return storage_client

def download_existing_data(storage_client, bucket_name, folder, file_name):
    """
    Download the existing data from Google Cloud Storage.
    
    Args:
    storage_client (storage.Client): Authenticated Google Cloud Storage client.
    bucket_name (str): The name of the GCS bucket.
    folder (str): The folder in the GCS bucket.
    file_name (str): The name of the file to download.
    
    Returns:
    pd.DataFrame: The existing data as a DataFrame.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{file_name}")
    blob.download_to_filename(file_name)
    recent_df = pd.read_csv(file_name)
    return recent_df

if __name__ == "__main__":
    
    current_date = datetime.now()
    month_var = str(current_date.month)
    year_var = str(current_date.year)
    
    FILE_NAME = f'energy_consumption-{month_var}-{year_var}.csv'  # Substitua pelo nome do arquivo desejado

    # Authenticate with GCS
    storage_client = authenticate_with_gcs()
    
    # Download the file from GCS
    energy_df = download_existing_data(storage_client, GCP_BUCKET, GCP_FOLDER, FILE_NAME)
    
    print(energy_df)
