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
TABLE_NAME = "fhv_tripdata"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "/home/pc/dev/homework/module-4/setup/dezoomcamp-sa.json"
storage_client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
bucket = storage_client.bucket(BUCKET_NAME)

# FHV 데이터 명시적 스키마 정의
fhv_schema = pa.schema([
    ('dispatching_base_num', pa.string()),
    ('pickup_datetime', pa.timestamp('us')),  # microseconds
    ('dropOff_datetime', pa.timestamp('us')),
    ('PUlocationID', pa.int64()),
    ('DOlocationID', pa.int64()),
    ('SR_Flag', pa.int64()),
    ('Affiliated_base_number', pa.string())
])

# FHV 데이터 설정
dataset = 'fhv'
year = 2019

for month in range(1, 13):
    # 파일명 설정
    csv_file = f'fhv_tripdata_{year}-{month:02d}.csv.gz'
    parquet_file = f'fhv_tripdata_{year}-{month:02d}.parquet'
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{csv_file}'

    print(f'Downloading {csv_file}...')
    df = pd.read_csv(url, compression='gzip', parse_dates=['pickup_datetime', 'dropOff_datetime'])
    print(f'Downloaded {len(df):,} records')

    # PyArrow Table로 변환 (명시적 스키마 적용)
    table = pa.Table.from_pandas(df, schema=fhv_schema, preserve_index=False)

    # 로컬에 parquet으로 저장 (coerce_timestamps 적용)
    pq.write_table(
        table,
        parquet_file,
        coerce_timestamps='us',  # microseconds로 변환
        compression='snappy'
    )
    print(f'Saved to local: {parquet_file}')

    # GCS에 업로드
    blob = bucket.blob(f'fhv/{parquet_file}')
    blob.upload_from_filename(parquet_file)
    print(f'Uploaded to GCS: gs://{BUCKET_NAME}/fhv/{parquet_file}')

    # 로컬 파일 삭제
    os.remove(parquet_file)
    print(f'Cleaned up local file\n')

print(f'All FHV 2019 data uploaded to gs://{BUCKET_NAME}/fhv/')
print(f'Total months: 12\n')

# BigQuery External Table 생성
print('Creating BigQuery External Table...')
bq_client = bigquery.Client.from_service_account_json(CREDENTIALS_FILE)

table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}_external"
source_uris = [f'gs://{BUCKET_NAME}/fhv/*.parquet']

# ExternalConfig 설정
external_config = bigquery.ExternalConfig('PARQUET')
external_config.source_uris = source_uris
external_config.autodetect = True

# Table 생성
table = bigquery.Table(table_id)
table.external_data_configuration = external_config

try:
    table = bq_client.create_table(table, exists_ok=True)
    print(f'Created external table: {table_id}')
    print(f'Format: {table.external_data_configuration.source_format}')
    print(f'Source URIs: {table.external_data_configuration.source_uris}')
except Exception as e:
    print(f'Error creating external table: {e}')

# Native Table 생성 (Optional)
print('\nCreating BigQuery Native Table...')
native_table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"
query = f"""
CREATE OR REPLACE TABLE `{native_table_id}` AS
SELECT * FROM `{table_id}`
"""

try:
    query_job = bq_client.query(query)
    query_job.result()  # Wait for the job to complete
    print(f'Created native table: {native_table_id}')

    # 레코드 수 확인
    count_query = f"SELECT COUNT(*) as cnt FROM `{native_table_id}`"
    count_result = bq_client.query(count_query).result()
    for row in count_result:
        print(f'Total records: {row.cnt:,}')
except Exception as e:
    print(f'Error creating native table: {e}')                                                                      
                                                                                                                 