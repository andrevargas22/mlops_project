import requests
from bs4 import BeautifulSoup
import os

# URL of the webpage to scrape
url = "https://www.epe.gov.br/pt/publicacoes-dados-abertos/publicacoes/consumo-de-energia-eletrica"

download_dir = 'data/raw'
file_name = 'consumo.xls'

# Function to check for updates and download the file
def download_data():
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the download link for the Excel file (assuming the link text or href contains '.xls')
    link = soup.find('a', href=lambda href: href and '.xls' in href)
    
    if link:
        file_url = link['href']
        
        # Check if the URL is relative and prefix with the base URL if needed
        if not file_url.startswith('http'):
            file_url = f"https://www.epe.gov.br{file_url}"
        
        file_path = os.path.join(download_dir, file_name)
        file_response = requests.get(file_url)
        
        os.makedirs(download_dir, exist_ok=True)
        
        with open(file_path, 'wb') as file:
            file.write(file_response.content)
        
        print(f"File downloaded from {file_url} to {file_path}")
    else:
        print("No Excel file link found.")

# Run the script
if __name__ == "__main__":
    download_data()
