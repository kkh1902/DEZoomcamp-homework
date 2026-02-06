import pandas as pd
from google.cloud import storage
import os

# Change this to your bucket name
BUCKET_NAME = "dezoomcamp-module-4"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/home/pc/dev/homework/module-4/setup/dezoomcamp-sa.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = client.bucket(BUCKET_NAME)

# FHV data settings
dataset = 'fhv'
year = 2019

for month in range(1, 13):
    # File name setup
    csv_file = f'fhv_tripdata_{year}-{month:02d}.csv.gz'
    parquet_file = f'fhv_tripdata_{year}-{month:02d}.parquet'
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{csv_file}'

    print(f'Downloading {csv_file}...')
    df = pd.read_csv(url, compression='gzip')
    print(f'Downloaded {len(df):,} records')

    # Save to local parquet
    df.to_parquet(parquet_file, engine='pyarrow')
    print(f'Saved to local: {parquet_file}')

    # Upload to GCS
    blob = bucket.blob(f'fhv/{parquet_file}')
    blob.upload_from_filename(parquet_file)
    print(f'Uploaded to GCS: gs://{BUCKET_NAME}/fhv/{parquet_file}')

    # Delete local file
    os.remove(parquet_file)
    print(f'Cleaned up local file\n')

print(f'All FHV 2019 data uploaded to gs://{BUCKET_NAME}/fhv/')
print(f'Total months: 12')                                                                      
                                                                                                                 
