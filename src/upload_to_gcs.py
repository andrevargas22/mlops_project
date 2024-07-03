import json
import os
from datetime import datetime
import pandas as pd
from google.cloud import storage

# Configurações do Google Cloud
GCP_PROJECT = 'mlops-project-428219'
GCP_BUCKET = 'mlops_energy_consumption_data'
GCP_FOLDER = 'energy_data'
FILE_PATH = 'data/raw/consumo.xls'

def authenticate_with_gcs():
    """
    Authenticate with Google Cloud Storage using service account credentials from an environment variable.
    
    Returns:
    storage.Client: Authenticated Google Cloud Storage client.
    """
    # Read service account credentials from environment variable
    service_account_info = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
    storage_client = storage.Client.from_service_account_info(service_account_info)
    return storage_client

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

    # Define the path for the cleaned CSV file
    cleaned_file_name = f'energy_consumption-{current_date.month}-{current_date.year}.csv'
    cleaned_file_path = f'data/temp/{cleaned_file_name}'
    
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
 
    # Save the cleaned DataFrame to a CSV file
    cleaned_df.to_csv(cleaned_file_path, index=False)
    
    return cleaned_file_name, cleaned_file_path

def upload_to_gcs(source_file_name, bucket_name, destination_blob_name):
    """
    Upload a file to Google Cloud Storage.
    
    Args:
    source_file_name (str): The path of the file to upload.
    bucket_name (str): The name of the GCS bucket.
    destination_blob_name (str): The destination path and filename in the GCS bucket.
    """
    storage_client = authenticate_with_gcs()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    
if __name__ == "__main__":
    cleaned_df_name, cleaned_df_path = clean_energy_data(FILE_PATH)
    destination_blob_name = f"{GCP_FOLDER}/{os.path.basename(cleaned_df_name)}"
    upload_to_gcs(cleaned_df_path, GCP_BUCKET, destination_blob_name)
