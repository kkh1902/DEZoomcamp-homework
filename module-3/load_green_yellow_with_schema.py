import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud import bigquery
import os

# Change this to your bucket name
BUCKET_NAME = "dezoomcamp-module-4"
PROJECT_ID = "data-warehouse-dezoomcamp"  # Change to your project ID
DATASET_NAME = "trips_data_all"  # Change to your dataset name

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/home/pc/dev/homework/module-4/setup/dezoomcamp-sa.json"
storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = storage_client.bucket(BUCKET_NAME)

# Green Taxi schema definition
green_schema = pa.schema([
    ('VendorID', pa.int64()),
    ('lpep_pickup_datetime', pa.timestamp('us')),
    ('lpep_dropoff_datetime', pa.timestamp('us')),
    ('store_and_fwd_flag', pa.string()),
    ('RatecodeID', pa.int64()),
    ('PULocationID', pa.int64()),
    ('DOLocationID', pa.int64()),
    ('passenger_count', pa.int64()),
    ('trip_distance', pa.float64()),
    ('fare_amount', pa.float64()),
    ('extra', pa.float64()),
    ('mta_tax', pa.float64()),
    ('tip_amount', pa.float64()),
    ('tolls_amount', pa.float64()),
    ('ehail_fee', pa.float64()),
    ('improvement_surcharge', pa.float64()),
    ('total_amount', pa.float64()),
    ('payment_type', pa.int64()),
    ('trip_type', pa.int64()),
    ('congestion_surcharge', pa.float64())
])

# Yellow Taxi schema definition
yellow_schema = pa.schema([
    ('VendorID', pa.int64()),
    ('tpep_pickup_datetime', pa.timestamp('us')),
    ('tpep_dropoff_datetime', pa.timestamp('us')),
    ('passenger_count', pa.int64()),
    ('trip_distance', pa.float64()),
    ('RatecodeID', pa.int64()),
    ('store_and_fwd_flag', pa.string()),
    ('PULocationID', pa.int64()),
    ('DOLocationID', pa.int64()),
    ('payment_type', pa.int64()),
    ('fare_amount', pa.float64()),
    ('extra', pa.float64()),
    ('mta_tax', pa.float64()),
    ('tip_amount', pa.float64()),
    ('tolls_amount', pa.float64()),
    ('improvement_surcharge', pa.float64()),
    ('total_amount', pa.float64()),
    ('congestion_surcharge', pa.float64())
])

def process_taxi_data(service_type, years, schema, datetime_cols):
    """
    Download taxi data and upload to GCS

    Args:
        service_type: 'green' or 'yellow'
        years: [2019, 2020]
        schema: PyArrow schema
        datetime_cols: list of columns to parse as datetime
    """
    for year in years:
        for month in range(1, 13):
            # File name setup
            csv_file = f'{service_type}_tripdata_{year}-{month:02d}.csv.gz'
            parquet_file = f'{service_type}_tripdata_{year}-{month:02d}.parquet'
            url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service_type}/{csv_file}'

            print(f'Downloading {csv_file}...')
            try:
                df = pd.read_csv(url, compression='gzip', parse_dates=datetime_cols)
                print(f'Downloaded {len(df):,} records')

                # Convert to PyArrow Table (apply explicit schema)
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

                # Save to local parquet (apply coerce_timestamps)
                pq.write_table(
                    table,
                    parquet_file,
                    coerce_timestamps='us',  # convert to microseconds
                    compression='snappy'
                )
                print(f'Saved to local: {parquet_file}')

                # Upload to GCS
                blob = bucket.blob(f'{service_type}/{parquet_file}')
                blob.upload_from_filename(parquet_file)
                print(f'Uploaded to GCS: gs://{BUCKET_NAME}/{service_type}/{parquet_file}')

                # Delete local file
                os.remove(parquet_file)
                print(f'Cleaned up local file\n')
            except Exception as e:
                print(f'Error processing {csv_file}: {e}\n')
                continue

    print(f'All {service_type} taxi data uploaded to gs://{BUCKET_NAME}/{service_type}/\n')

def create_bigquery_tables(service_type, table_name):
    """Create BigQuery External and Native tables"""
    bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

    # Create external table
    print(f'Creating BigQuery External Table for {service_type}...')
    external_table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}_external"
    source_uris = [f'gs://{BUCKET_NAME}/{service_type}/*.parquet']

    external_config = bigquery.ExternalConfig('PARQUET')
    external_config.source_uris = source_uris
    external_config.autodetect = True

    table = bigquery.Table(external_table_id)
    table.external_data_configuration = external_config

    try:
        table = bq_client.create_table(table, exists_ok=True)
        print(f'Created external table: {external_table_id}')
    except Exception as e:
        print(f'Error creating external table: {e}')

    # Create native table
    print(f'Creating BigQuery Native Table for {service_type}...')
    native_table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
    query = f"""
    CREATE OR REPLACE TABLE `{native_table_id}` AS
    SELECT * FROM `{external_table_id}`
    """

    try:
        query_job = bq_client.query(query)
        query_job.result()
        print(f'Created native table: {native_table_id}')

        # Confirm record count
        count_query = f"SELECT COUNT(*) as cnt FROM `{native_table_id}`"
        count_result = bq_client.query(count_query).result()
        for row in count_result:
            print(f'Total records: {row.cnt:,}\n')
    except Exception as e:
        print(f'Error creating native table: {e}\n')

if __name__ == "__main__":
    # Process Green Taxi (2019, 2020)
    print("="*60)
    print("Processing Green Taxi Data")
    print("="*60)
    process_taxi_data(
        service_type='green',
        years=[2019, 2020],
        schema=green_schema,
        datetime_cols=['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    )
    create_bigquery_tables('green', 'green_tripdata')

    # Process Yellow Taxi (2019, 2020)
    print("="*60)
    print("Processing Yellow Taxi Data")
    print("="*60)
    process_taxi_data(
        service_type='yellow',
        years=[2019, 2020],
        schema=yellow_schema,
        datetime_cols=['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    )
    create_bigquery_tables('yellow', 'yellow_tripdata')

    print("="*60)
    print("All Done!")
    print("="*60)
    print("\nExpected record counts:")
    print("- Green Taxi: 7,778,101")
    print("- Yellow Taxi: 109,047,518")
