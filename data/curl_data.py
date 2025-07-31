import os
import requests

years = range(2024, 2025)
months = range(1, 2)
# trip_types = ["yellow", "green", "fhv", "fhvhv"]
trip_types = ["yellow", "green"]
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
print(f"GET ROOT_PATH: {root_path}")

for year in years:
    for month in months:
        for trip_type in trip_types:
        
            # Define where to download
            month_str = f"{month:02d}"
            file_name = f"{trip_type}_tripdata_{year}-{month_str}.parquet"
            url = base_url + file_name

            # Define where to store
            folder_path = os.path.join("data", str(year), trip_type)
            os.makedirs(folder_path, exist_ok=True)
            file_path = os.path.join(folder_path, file_name)

            if os.path.exists(file_path):
                print(f"File {file_name} already exists, skipping download")
                continue

            # Download
            print(f"Downloading {file_name}...")
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, "wb") as f:
                    f.write(response.content)
                print(f"Saved to {file_path}")
            else:
                print(f"File not found or error: {url}")

print("---END---")