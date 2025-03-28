# src/app/utils/aws_s3.py
import os
import traceback
from urllib.parse import urlparse, unquote
import boto3 # Make sure boto3 is imported if used directly here

def extract_bucket_and_key(s3_url):
    """Extracts bucket and key from s3:// URL."""
    parsed_url = urlparse(s3_url)
    bucket = parsed_url.netloc
    # Unquote to handle special characters in keys, remove leading slash
    key = unquote(parsed_url.path).lstrip('/')

    if not s3_url.startswith("s3://") or not bucket: # Key can be empty for bucket root
        raise ValueError(f"Invalid S3 URL format: {s3_url}")
    return bucket, key

def download_s3_file(s3_client, bucket, key, local_path):
    """Downloads a file from S3 to a local path."""
    try:
        # Ensure the local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"DEBUG: Downloading s3://{bucket}/{key} to {local_path}")
        s3_client.download_file(bucket, key, local_path)
        print(f"DEBUG: Download complete for s3://{bucket}/{key}")
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404' or e.response['Error']['Code'] == 'NoSuchKey':
            print(f"ERROR: S3 object not found: s3://{bucket}/{key}")
            # Raise FileNotFoundError for consistent handling upstream
            raise FileNotFoundError(f"S3 object not found: s3://{bucket}/{key}") from e
        else:
            print(f"ERROR: S3 ClientError downloading s3://{bucket}/{key}: {e}")
            raise # Re-raise other S3 client errors
    except Exception as e:
        print(f"ERROR: Unexpected error downloading s3://{bucket}/{key}: {e}")
        traceback.print_exc()
        raise # Re-raise unexpected errors