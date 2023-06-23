import os
import time
import polars as pl
from polars import GCSOptions


def measure_performance_time(function):
    start_time = time.time()
    result = function()
    end_time = time.time()
    execution_time = end_time - start_time
    print("Performance Time:", execution_time)
    return result


def read_parquet_from_gcs():
    tenant_name = os.getenv("YOUR_TENANT_NAME")
    table = os.getenv("YOUR_TABLE_NAME")
    bucket_name = os.getenv("YOUR_BUCKET_NAME")
    project_id = os.getenv("YOUR_PROJECT_ID")
    gcs_options = GCSOptions(bucket=bucket_name, project=project_id)
    df = pl.read_parquet(f"gs://{tenant_name}/processed_data/{table}", storage_options=gcs_options)
    return df


def write_parquet_local(df):
    tenant_name = os.getenv("YOUR_TENANT_NAME")
    local_file = f"/exports/{tenant_name}/metadata.parquet"
    df.write_parquet(local_file)


def main():
    # Read Parquet file from Google Cloud Storage
    df = measure_performance_time(read_parquet_from_gcs)

    # Write Parquet file locally
    measure_performance_time(lambda: write_parquet_local(df))


if __name__ == "__main__":
    main()
