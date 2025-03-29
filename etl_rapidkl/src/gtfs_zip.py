import requests
import zipfile
import io
import pandas as pd
import os

url = 'https://api.data.gov.my/gtfs-static/prasarana?category=rapid-bus-kl'

response = requests.get(url)

if response.status_code == 200:
    print("GTFS Static Download Successfull")

    with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_ref:
        extract_path = "gtfs_data"
        zip_ref.extractall(extract_path)
        print(f"Extracted files to: {extract_path}")


        for file_name in os.listdir(extract_path):
            if file_name.endswith('.txt'):
                file_path = os.path.join(extract_path, file_name)


                df = pd.read_csv(file_path)
                print(f"Processing: {file_name}")
                print(df.head())
else:
    print("Failed to download GTFS Static files!")