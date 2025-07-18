# src/ai_trading_machine/utils/gcs_utils.py

from io import BytesIO

import pandas as pd
from google.cloud import storage


def upload_to_gcs(df: pd.DataFrame, bucket: str, destination_blob_name: str):
    """
    Upload DataFrame as Parquet file to GCS bucket.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(destination_blob_name)

    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    print("✅ Uploaded to gs://{bucket.name}/{destination_blob_name}")


def load_cleaned_data(
    ticker: str, start_date: str, end_date: str, bucket: str = "cleaned-data"
) -> pd.DataFrame:
    """
    Download a cleaned Parquet file from GCS and load as DataFrame.
    """
    client = storage.Client()
    bucket_obj = client.get_bucket(bucket)
    filename = "{ticker}_{start_date}_{end_date}.parquet"
    blob = bucket_obj.blob(filename)
    buffer = BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)
    df = pd.read_parquet(buffer)
    print("✅ Loaded cleaned data from gs://{bucket}/{filename}")
    return df
