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

def process_data(df):
    
    # Map month names to month numbers
    month_map = {
        'January': 1,
        'February': 2,
        'March': 3,
        'April': 4,
        'May': 5,
        'June': 6,
        'July': 7,
        'August': 8,
        'September': 9,
        'October': 10,
        'November': 11,
        'December': 12
    }

    # Apply the month_map to create a month number column
    df['Month'] = df['Month'].map(month_map)

    # Pegar a estação do ano e criar uma coluna Estação 
    def get_season(month):
        if month in [12, 1, 2]:
            return 'Summer'
        elif month in [3, 4, 5]:
            return 'Autumn'
        elif month in [6, 7, 8]:
            return 'Winter'
        else:
            return 'Spring'
        
    df['Season'] = df['Month'].apply(get_season)
    
    # sort by Region, Year, Month
    df = df.sort_values(['Region', 'Year', 'Month']).reset_index(drop=True)
    
    df = df[['Region', 'Year', 'Month', 'Season', 'Energy']]
    
    return df
    
if __name__ == "__main__":
    
    current_date = datetime.now()
    month_var = str(current_date.month)
    year_var = str(current_date.year)
    
    FILE_NAME = f'energy_consumption-{month_var}-{year_var}.csv'  # Substitua pelo nome do arquivo desejado

    # Authenticate with GCS
    storage_client = authenticate_with_gcs()
    
    # Download the file from GCS
    energy_df = download_existing_data(storage_client, GCP_BUCKET, GCP_FOLDER, FILE_NAME)
    energy_processed = process_data(energy_df)
    
    print(energy_processed)
