import json
import os
from datetime import datetime
import pandas as pd
from google.cloud import storage

# Configurações do Google Cloud
GCP_PROJECT = 'mlops-project-428219'
GCP_BUCKET = 'mlops_energy_consumption_data'
GCP_FOLDER = 'energy_data'
FILE_PATH = 'data/raw/consumo_mar.xls'

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
    existing_df = pd.read_csv(file_name)
    return existing_df

def clean_energy_data(file_path):
    """
    Clean and process energy consumption data from an Excel file.
    
    This function reads an Excel file containing energy consumption data, cleans it by removing 
    unnecessary rows, renaming columns, and adding a year column. The cleaned data is then 
    transformed into a long format suitable for analysis and saved as a CSV file.
    
    Args:
    file_path (str): The path to the Excel file containing the raw energy consumption data.
    
    Returns:
    tuple: The name and path of the cleaned and processed CSV file.
    """
    
    current_date = datetime.now()
    
    # Read the Excel file into a DataFrame
    df = pd.read_excel(file_path)
    
    # Drop rows with any missing values
    df = df.dropna()
    
    # Rename columns to more meaningful names
    df.columns = ["Region", "January", "February", "March", "April", "May", "June", "July", 
                  "August", "September", "October", "November", "December", "Total"]

    # Reset the index of the DataFrame
    df = df.reset_index(drop=True)
    
    # Rename 'TOTAL BRASIL' to 'Brasil' in the 'Região' column
    df.loc[df['Region'] == 'TOTAL BRASIL', 'Region'] = 'Brasil'
    
    # Create an empty DataFrame to store the filtered data
    df_filter_total = pd.DataFrame()

    # Loop through the DataFrame in chunks of 11 rows
    for i in range(0, len(df), 11):
        df_filter = df.iloc[i:i+11].copy().reset_index(drop=True)
        
        # Drop rows from index 6 to 10
        df_filter = df_filter.drop(df_filter.index[6:11])
        
        # Concatenate the filtered chunk to the total filtered DataFrame
        df_filter_total = pd.concat([df_filter_total, df_filter])
            
    # Reset the index of the total filtered DataFrame
    df_filter_total = df_filter_total.reset_index(drop=True)
    
    # Number of rows per group (6 rows per year)
    rows_per_group = 6
    
    # Calculate the number of groups (years)
    num_groups = len(df_filter_total) // rows_per_group
    
    years = []

    # Assign the current year and previous years to each group of rows
    for i in range(num_groups):
        year = current_date.year - i
        years.extend([year] * rows_per_group)

    # Add remaining rows if any
    remaining_rows = len(df_filter_total) % rows_per_group
    if remaining_rows > 0:
        years.extend([current_date.year - num_groups] * remaining_rows)
    
    # Add the 'Ano' (Year) column to the DataFrame
    df_filter_total['Year'] = years

    # List of month columns
    month_columns = ["January", "February", "March", "April", "May", "June", "July", 
                  "August", "September", "October", "November", "December"]

    # Transform the DataFrame from wide format to long format
    cleaned_df = pd.melt(df_filter_total, id_vars=['Region', 'Year'], value_vars=month_columns, 
                         var_name='Month', value_name='Energy')
    
    return cleaned_df

def upload_to_gcs(storage_client, dataframe, bucket_name, folder, file_name):
    """
    Upload a DataFrame to Google Cloud Storage.
    
    Args:
    storage_client (storage.Client): Authenticated Google Cloud Storage client.
    dataframe (pd.DataFrame): The DataFrame to upload.
    bucket_name (str): The name of the GCS bucket.
    folder (str): The folder in the GCS bucket.
    file_name (str): The name of the file to upload.
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f"{folder}/{file_name}")
    
    # Convert DataFrame to CSV format in memory
    csv_buffer = dataframe.to_csv(index=False)
    
    # Upload CSV to GCS
    blob.upload_from_string(csv_buffer, content_type='text/csv')
    print(f"File {file_name} uploaded to {folder}/{file_name} in bucket {bucket_name}.")
    
if __name__ == "__main__":
    
    month_var = str(2)
    year_var = str(2024)
    
    next_month_var = str(3)
    
    # Authenticate with GCS once
    storage_client = authenticate_with_gcs()
    
    # Clean the new data
    new_data = clean_energy_data(FILE_PATH)
    
    # Download the existing data
    existing_data = download_existing_data(storage_client, GCP_BUCKET, GCP_FOLDER, f'energy_consumption-{month_var}-{year_var}.csv')
    
    if not existing_data.equals(new_data):
        # Upload the new data to GCS
        upload_to_gcs(storage_client, new_data, GCP_BUCKET, GCP_FOLDER, f'energy_consumption-{next_month_var}-{year_var}.csv')
    else:
        print("No changes detected. Data not updated.")