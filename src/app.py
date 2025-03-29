from flask import Flask, request, jsonify
import boto3
import os
import json
import tempfile
import traceback
import datetime
from dotenv import load_dotenv
from fastavro import reader as avro_reader # Keep for Iceberg
import pyarrow.parquet as pq
from urllib.parse import urlparse, unquote
from flask_cors import CORS
import re # Needed for Delta log file matching
import time # Needed for Delta/Iceberg timing
from botocore.exceptions import NoCredentialsError, ClientError # Import ClientError
from google.cloud import storage # Import GCS client library
from google.api_core import exceptions as gcs_exceptions # Import GCS exceptions
from google.auth import exceptions as gcs_auth_exceptions # Import GCS Auth exceptions

import google.generativeai as genai
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
# Configure Gemini API
genai.configure(api_key=API_KEY)
app = Flask(__name__)

CORS(app)

# --- Configuration ---

# Standard AWS Credentials (used if MinIO endpoint is not detected/specified)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1") # Region might be optional for MinIO

# Optional MinIO Credentials (use these if connecting to MinIO)
# If MINIO_ENDPOINT_URL is set, these keys will be preferred
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL") # e.g., http://3.7.189.228:9000
MINIO_ACCESS_KEY_ID = os.getenv("MINIO_ACCESS_KEY_ID")
MINIO_SECRET_ACCESS_KEY = os.getenv("MINIO_SECRET_ACCESS_KEY")
MINIO_USE_SSL = os.getenv("MINIO_USE_SSL", "true").lower() == "true" # Default to true, override if needed

# GCS Credentials
# Standard way: Set GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your service account key file.
GCS_SERVICE_ACCOUNT_KEY_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GCS_PROJECT_ID = os.getenv("GCS_PROJECT_ID") # Optional: Explicitly set project ID

# --- Helper Functions ---

def get_gemini_response(prompt):
    model = genai.GenerativeModel("gemini-1.5-flash")
    try:
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"ERROR: Gemini API call failed: {e}")
        # Fallback or error message
        return f"<p>Error generating summary: {e}</p>"


def format_bytes(size_bytes):
    """Converts bytes to a human-readable string (KB, MB, GB)."""
    if size_bytes is None:
        return "N/A"
    try:
        size_bytes = int(size_bytes)
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.2f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/(1024**2):.2f} MB"
        else:
            return f"{size_bytes/(1024**3):.2f} GB"
    except (ValueError, TypeError):
        return "Invalid Size"


def format_timestamp_ms(timestamp_ms):
    """Converts milliseconds since epoch to ISO 8601 UTC string."""
    if timestamp_ms is None:
        return None
    try:
        dt_object = datetime.datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=datetime.timezone.utc)
        return dt_object.isoformat().replace('+00:00', 'Z') # Standard ISO 8601 format
    except (ValueError, TypeError):
        return str(timestamp_ms) # Return original value if conversion fails

# --- URL Parsing (Updated) ---
def parse_storage_url(storage_url):
    """
    Parses S3, MinIO HTTP(S), or GCS URLs to extract connection details.

    Args:
        storage_url (str): The storage URL (e.g., s3://..., http://..., gs://...).

    Returns:
        tuple: (storage_type, bucket_name, key_prefix, endpoint_url, use_ssl)
               storage_type is 's3' or 'gcs'.
               endpoint_url/use_ssl are relevant only for S3/MinIO.

    Raises:
        ValueError: If the URL format is invalid or unsupported.
    """
    parsed_url = urlparse(unquote(storage_url)) # Decode URL encoding first
    scheme = parsed_url.scheme.lower()
    storage_type = None
    endpoint_url = None
    use_ssl = True
    bucket_name = None
    key_prefix = ""

    if scheme == "s3":
        storage_type = "s3"
        bucket_name = parsed_url.netloc
        key_prefix = parsed_url.path.lstrip('/')
        if not bucket_name:
            raise ValueError(f"Invalid S3 URL: Missing bucket name in {storage_url}")
        # For S3, endpoint_url remains None (use AWS default), use_ssl is True
    elif scheme in ["http", "https"]:
        storage_type = "s3" # Treat MinIO as S3 type for client creation
        endpoint_url = f"{scheme}://{parsed_url.netloc}"
        use_ssl = (scheme == "https")

        # Path for MinIO looks like /bucket/key/prefix
        path_parts = parsed_url.path.strip('/').split('/', 1)
        if len(path_parts) >= 1 and path_parts[0]:
            bucket_name = path_parts[0]
            if len(path_parts) > 1:
                key_prefix = path_parts[1]
        else:
            raise ValueError(f"Invalid MinIO URL: Missing bucket name in path for {storage_url}")
    elif scheme == "gs":
        storage_type = "gcs"
        bucket_name = parsed_url.netloc
        key_prefix = parsed_url.path.lstrip('/')
        if not bucket_name:
            raise ValueError(f"Invalid GCS URL: Missing bucket name in {storage_url}")
        endpoint_url = None # Not applicable to GCS client
        use_ssl = True # Not applicable to GCS client

    else:
        raise ValueError(f"Unsupported URL scheme: '{scheme}' in {storage_url}. Use 's3', 'http', 'https', or 'gs'.")

    # Ensure key_prefix ends with '/' if it represents a directory-like structure
    original_path = parsed_url.path
    is_directory_like = original_path.endswith('/') or not os.path.splitext(original_path)[1] # Ends with / or has no extension
    if key_prefix and not key_prefix.endswith('/') and is_directory_like:
         key_prefix += '/'
    elif not key_prefix and original_path == '/': # Handle root path case e.g. http://host/bucket/
         pass # key_prefix remains empty string ""
    elif not key_prefix and original_path.strip('/') == bucket_name: # Handle case s3://bucket or http://host/bucket
         pass # key_prefix remains empty string ""


    print(f"DEBUG parse_storage_url: URL='{storage_url}', Type='{storage_type}', Bucket='{bucket_name}', KeyPrefix='{key_prefix}', Endpoint='{endpoint_url}', SSL={use_ssl}")
    return storage_type, bucket_name, key_prefix, endpoint_url, use_ssl


# --- Storage Client Factory (Updated) ---
def create_storage_client(storage_type, endpoint_url=None, use_ssl=True, region_name=None):
    """
    Creates a Boto3 S3 client or a GCS client based on storage_type.

    Args:
        storage_type (str): 's3' or 'gcs'.
        endpoint_url (str, optional): S3 endpoint URL (for MinIO). Defaults to None.
        use_ssl (bool): For S3/MinIO SSL. Defaults to True.
        region_name (str, optional): AWS Region. Defaults to None.

    Returns:
        (boto3.client | google.cloud.storage.Client): Configured client instance.

    Raises:
        ValueError: If storage_type is invalid.
        NoCredentialsError: If S3/MinIO credentials are required and missing.
        gcs_auth_exceptions.DefaultCredentialsError: If GCS credentials cannot be found.
    """
    if storage_type == 's3':
        config_kwargs = {}
        access_key = AWS_ACCESS_KEY_ID
        secret_key = AWS_SECRET_ACCESS_KEY
        effective_region = region_name or AWS_REGION

        # Prioritize MinIO-specific config if endpoint_url is provided
        # Or if MINIO_ENDPOINT_URL is globally set
        effective_endpoint_url = endpoint_url or MINIO_ENDPOINT_URL

        if effective_endpoint_url:
            print(f"DEBUG: Configuring Boto3 client for S3 endpoint: {effective_endpoint_url}")
            config_kwargs['endpoint_url'] = effective_endpoint_url
            config_kwargs['use_ssl'] = use_ssl # Use provided or default from env
            # Use MinIO keys if available when endpoint is set
            access_key = MINIO_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID
            secret_key = MINIO_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY
            if effective_region: config_kwargs['region_name'] = effective_region
        elif effective_region:
            print(f"DEBUG: Configuring Boto3 client for AWS S3 (Region: {effective_region})")
            config_kwargs['region_name'] = effective_region

        if not access_key or not secret_key:
            print("WARNING: AWS/MinIO Access Key ID or Secret Access Key is missing.")
            # Boto3 will raise NoCredentialsError if they are truly needed and not found elsewhere (like IAM role)
            # raise NoCredentialsError("Access Key ID or Secret Access Key is missing.")

        try:
            return boto3.client('s3',
                                aws_access_key_id=access_key,
                                aws_secret_access_key=secret_key,
                                **config_kwargs)
        except NoCredentialsError as e:
             print("ERROR: Boto3 could not find S3 credentials.")
             raise NoCredentialsError("S3 credentials not found. Configure AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or IAM role/profile.") from e


    elif storage_type == 'gcs':
        print("DEBUG: Configuring GCS client.")
        try:
            if GCS_SERVICE_ACCOUNT_KEY_PATH:
                print(f"DEBUG: Using GCS service account key from: {GCS_SERVICE_ACCOUNT_KEY_PATH}")
                return storage.Client.from_service_account_json(GCS_SERVICE_ACCOUNT_KEY_PATH, project=GCS_PROJECT_ID)
            else:
                print("DEBUG: Using GCS Application Default Credentials (ADC).")
                # ADC will search for credentials in standard locations
                # (env var, gcloud config, metadata server)
                return storage.Client(project=GCS_PROJECT_ID)
        except FileNotFoundError as e:
             print(f"ERROR: GCS Service Account key file not found at path: {GCS_SERVICE_ACCOUNT_KEY_PATH}")
             raise FileNotFoundError(f"GCS credentials file not found: {GCS_SERVICE_ACCOUNT_KEY_PATH}") from e
        except gcs_auth_exceptions.DefaultCredentialsError as e:
             print("ERROR: Could not find GCS default credentials. Ensure GOOGLE_APPLICATION_CREDENTIALS is set or gcloud is configured.")
             raise gcs_auth_exceptions.DefaultCredentialsError("Could not automatically find GCS credentials. Set GOOGLE_APPLICATION_CREDENTIALS environment variable or configure Application Default Credentials.") from e
        except Exception as e:
             print(f"ERROR: Failed to initialize GCS client: {e}")
             raise Exception(f"Failed to initialize GCS client: {e}") from e

    else:
        raise ValueError(f"Invalid storage_type: '{storage_type}'. Must be 's3' or 'gcs'.")

# --- Abstracted Storage Operations ---

def download_file(client, storage_type, bucket, key, local_path):
    """Downloads a file from S3/MinIO or GCS to a local path."""
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"DEBUG: Downloading {storage_type}://{bucket}/{key} to {local_path}")

        if storage_type == 's3':
            client.download_file(bucket, key, local_path)
        elif storage_type == 'gcs':
            gcs_bucket = client.bucket(bucket)
            blob = gcs_bucket.blob(key)
            blob.download_to_filename(local_path)
        else:
            raise ValueError(f"Unsupported storage_type for download: {storage_type}")

        print(f"DEBUG: Download complete for {storage_type}://{bucket}/{key}")

    except ClientError as e: # S3 specific errors
        if e.response['Error']['Code'] == '404' or 'NoSuchKey' in str(e):
            print(f"ERROR: S3 object not found: {bucket}/{key}")
            raise FileNotFoundError(f"S3 object not found: {bucket}/{key}") from e
        elif e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"ERROR: S3 bucket not found: {bucket}")
            raise FileNotFoundError(f"S3 bucket not found: {bucket}") from e
        else:
            print(f"ERROR: S3 ClientError downloading {bucket}/{key}: {e}")
            raise
    except gcs_exceptions.NotFound as e: # GCS specific 404 error
        print(f"ERROR: GCS object not found: {bucket}/{key}")
        raise FileNotFoundError(f"GCS object not found: {bucket}/{key}") from e
    except Exception as e:
        print(f"ERROR: Unexpected error downloading {storage_type}://{bucket}/{key}: {e}")
        traceback.print_exc()
        raise


def list_storage_items(client, storage_type, bucket, prefix, delimiter=None, max_keys=None):
    """
    Lists items (objects/blobs) in a bucket, handling pagination.
    Returns a list of dictionaries with 'Key', 'Size', 'LastModified'.
    For delimiter usage, also returns a list of 'CommonPrefixes'.
    """
    items = []
    prefixes = [] # For common prefixes when delimiter is used

    print(f"DEBUG: Listing {storage_type}://{bucket}/{prefix} (Delimiter: {delimiter}, MaxKeys: {max_keys})")

    try:
        if storage_type == 's3':
            paginator = client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
                Delimiter=delimiter if delimiter else '',
                PaginationConfig={'MaxItems': max_keys} if max_keys else {}
            )
            for page in page_iterator:
                for content in page.get('Contents', []):
                    items.append({
                        'Key': content['Key'],
                        'Size': content.get('Size'),
                        'LastModified': content.get('LastModified')
                    })
                if delimiter:
                    for common_prefix in page.get('CommonPrefixes', []):
                        prefixes.append(common_prefix.get('Prefix'))
                # Early exit if max_keys is reached (approximated by items length)
                if max_keys is not None and len(items) >= max_keys:
                   break

        elif storage_type == 'gcs':
            # GCS list_blobs handles pagination automatically via the iterator
            blobs_iterator = client.list_blobs(
                bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_results=max_keys # Note: GCS max_results is per page, but iterator handles total
            )
            for blob in blobs_iterator:
                items.append({
                    'Key': blob.name,
                    'Size': blob.size,
                    'LastModified': blob.updated # GCS uses 'updated' timestamp
                })
                # Early exit if max_keys is reached
                if max_keys is not None and len(items) >= max_keys:
                    break

            if delimiter and hasattr(blobs_iterator, 'prefixes'):
                 prefixes.extend(blobs_iterator.prefixes)

        else:
            raise ValueError(f"Unsupported storage_type for listing: {storage_type}")

        print(f"DEBUG: Found {len(items)} items and {len(prefixes)} common prefixes.")
        return items, prefixes

    except ClientError as e: # S3 errors
        print(f"ERROR: S3 ClientError listing {bucket}/{prefix}: {e}")
        if e.response['Error']['Code'] == 'NoSuchBucket':
             raise FileNotFoundError(f"S3 bucket not found: {bucket}") from e
        # Allow AccessDenied for sub-checks, but raise for primary listing if desired
        # raise # Re-raise other S3 errors
        return [], [] # Return empty on error for now, especially for sub-checks
    except gcs_exceptions.NotFound as e: # GCS bucket not found
        print(f"ERROR: GCS bucket not found: {bucket}")
        raise FileNotFoundError(f"GCS bucket not found: {bucket}") from e
    # GCS Permissions error might be gcs_exceptions.Forbidden
    except gcs_exceptions.Forbidden as e:
         print(f"ERROR: GCS Access Denied listing {bucket}/{prefix}: {e}")
         raise PermissionError(f"GCS Access Denied for {bucket}/{prefix}") from e
    except Exception as e:
        print(f"ERROR: Unexpected error listing {storage_type}://{bucket}/{prefix}: {e}")
        traceback.print_exc()
        # raise # Re-raise unexpected errors
        return [], [] # Return empty on unexpected errors

def get_storage_object_metadata(client, storage_type, bucket, key):
    """Gets metadata (like size) for a single object/blob."""
    try:
        print(f"DEBUG: Getting metadata for {storage_type}://{bucket}/{key}")
        if storage_type == 's3':
            response = client.head_object(Bucket=bucket, Key=key)
            return {'ContentLength': response.get('ContentLength'), 'LastModified': response.get('LastModified')}
        elif storage_type == 'gcs':
            gcs_bucket = client.bucket(bucket)
            blob = gcs_bucket.get_blob(key) # More efficient than bucket.blob(key).reload() if exists check needed
            if blob:
                return {'ContentLength': blob.size, 'LastModified': blob.updated}
            else:
                raise gcs_exceptions.NotFound(f"Blob {key} not found in bucket {bucket}")
        else:
            raise ValueError(f"Unsupported storage_type for metadata: {storage_type}")

    except ClientError as e:
        if e.response['Error']['Code'] == '404' or 'NoSuchKey' in str(e):
             print(f"Warning: S3 object not found (metadata): {bucket}/{key}")
             raise FileNotFoundError(f"S3 object not found: {bucket}/{key}") from e
        print(f"ERROR: S3 ClientError getting metadata for {bucket}/{key}: {e}")
        raise
    except gcs_exceptions.NotFound as e:
         print(f"Warning: GCS object not found (metadata): {bucket}/{key}")
         raise FileNotFoundError(f"GCS object not found: {bucket}/{key}") from e
    except Exception as e:
        print(f"ERROR: Unexpected error getting metadata for {storage_type}://{bucket}/{key}: {e}")
        raise

# --- Parsing/Reading Functions (Modified to use abstracted download) ---

def parse_avro_file(file_path):
    """Parses an Avro file and returns a list of records."""
    records = []
    print(f"DEBUG: Attempting to parse Avro file: {file_path}")
    try:
        with open(file_path, 'rb') as fo:
            records = list(avro_reader(fo))
        print(f"DEBUG: Successfully parsed Avro file: {file_path}, Records found: {len(records)}")
        return records
    except Exception as e:
        print(f"Error parsing Avro file {file_path}: {e}")
        traceback.print_exc()
        raise


def read_parquet_sample(client, storage_type, bucket, key, local_dir, num_rows=10):
    """Downloads a Parquet file from S3/MinIO/GCS and reads a sample."""
    base_key = os.path.basename(key)
    local_filename = "sample_" + re.sub(r'[^\w\-.]', '_', base_key)
    local_path = os.path.join(local_dir, local_filename)

    try:
        download_file(client, storage_type, bucket, key, local_path) # Use abstracted download
        print(f"DEBUG: Reading Parquet sample from: {local_path}")
        table = pq.read_table(local_path)
        sample_table = table.slice(length=min(num_rows, len(table)))
        sample_data = sample_table.to_pylist()
        print(f"DEBUG: Successfully read {len(sample_data)} sample rows from Parquet: {key}")
        return sample_data
    except FileNotFoundError:
        print(f"ERROR: Sample Parquet file not found on {storage_type}: {bucket}/{key}")
        return [{"error": f"Sample Parquet file not found", "details": f"{storage_type}://{bucket}/{key}"}]
    except Exception as e:
        print(f"Error reading Parquet file {storage_type}://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        return [{"error": f"Failed to read sample Parquet data", "details": str(e)}]
    finally:
        if os.path.exists(local_path):
            try: os.remove(local_path); print(f"DEBUG: Cleaned up {local_path}")
            except Exception as rm_err: print(f"Warning: Could not remove temp sample file {local_path}: {rm_err}")


def read_delta_checkpoint(client, storage_type, bucket, key, local_dir):
    """Downloads and reads a Delta checkpoint Parquet file."""
    base_key = os.path.basename(key)
    local_filename = re.sub(r'[^\w\-.]', '_', base_key)
    local_path = os.path.join(local_dir, local_filename)
    actions = []
    try:
        download_file(client, storage_type, bucket, key, local_path) # Use abstracted download
        print(f"DEBUG: Reading Delta checkpoint file: {local_path}")
        table = pq.read_table(local_path)
        # ... (rest of the parquet reading logic remains the same) ...
        for batch in table.to_batches():
           batch_dict = batch.to_pydict()
           num_rows = len(batch_dict[list(batch_dict.keys())[0]])
           for i in range(num_rows):
               action = {}
               if batch_dict.get('add') and batch_dict['add'][i] is not None:
                   action['add'] = batch_dict['add'][i]
               elif batch_dict.get('remove') and batch_dict['remove'][i] is not None:
                   action['remove'] = batch_dict['remove'][i]
               elif batch_dict.get('metaData') and batch_dict['metaData'][i] is not None:
                   action['metaData'] = batch_dict['metaData'][i]
               elif batch_dict.get('protocol') and batch_dict['protocol'][i] is not None:
                   action['protocol'] = batch_dict['protocol'][i]
               elif batch_dict.get('txn') and batch_dict['txn'][i] is not None:
                   action['txn'] = batch_dict['txn'][i]
               if action: actions.append(action)

        print(f"DEBUG: Successfully read {len(actions)} actions from checkpoint: {key}")
        return actions
    except FileNotFoundError:
        print(f"ERROR: Checkpoint file not found on {storage_type}: {bucket}/{key}")
        raise # Re-raise to be caught by caller
    except Exception as e:
        print(f"Error reading Delta checkpoint file {storage_type}://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        raise
    finally:
        if os.path.exists(local_path):
            try: os.remove(local_path); print(f"DEBUG: Cleaned up {local_path}")
            except Exception as rm_err: print(f"Warning: Could not remove temp checkpoint file {local_path}: {rm_err}")


def read_delta_json_lines(client, storage_type, bucket, key, local_dir):
    """Downloads and reads a Delta JSON commit file line by line."""
    base_key = os.path.basename(key)
    local_filename = re.sub(r'[^\w\-.]', '_', base_key)
    local_path = os.path.join(local_dir, local_filename)
    actions = []
    try:
        download_file(client, storage_type, bucket, key, local_path) # Use abstracted download
        print(f"DEBUG: Reading Delta JSON file: {local_path}")
        with open(local_path, 'r') as f:
            for line in f:
                try:
                    action = json.loads(line)
                    actions.append(action)
                except json.JSONDecodeError as json_err:
                    print(f"Warning: Skipping invalid JSON line in {key}: {json_err} - Line: '{line.strip()}'")
        print(f"DEBUG: Successfully read {len(actions)} actions from JSON file: {key}")
        return actions
    except FileNotFoundError:
        print(f"ERROR: JSON commit file not found on {storage_type}: {bucket}/{key}")
        raise # Re-raise to be caught by caller
    except Exception as e:
        print(f"Error reading Delta JSON file {storage_type}://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        raise
    finally:
        if os.path.exists(local_path):
            try: os.remove(local_path); print(f"DEBUG: Cleaned up {local_path}")
            except Exception as rm_err: print(f"Warning: Could not remove temp JSON file {local_path}: {rm_err}")


# Function to make complex objects JSON serializable
def convert_bytes(obj):
    """Converts bytes, numpy types, datetimes etc. for JSON serialization."""
    if isinstance(obj, bytes):
        try: return obj.decode('utf-8', errors='replace') # Handle potential decode errors
        except Exception: return f"<bytes len={len(obj)} error>"
    elif isinstance(obj, dict): return {convert_bytes(k): convert_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list): return [convert_bytes(item) for item in obj]
    # Handle datetime objects (ensure timezone awareness if present)
    elif isinstance(obj, datetime.datetime):
        try: return obj.isoformat()
        except Exception: return str(obj)
    elif isinstance(obj, datetime.date):
         try: return obj.isoformat()
         except Exception: return str(obj)
    elif isinstance(obj, time.struct_time):
        try: return datetime.datetime.fromtimestamp(time.mktime(obj)).isoformat() # Convert time.struct_time
        except Exception: return str(obj)
    # Handle numpy types if numpy is installed and used (e.g., via pyarrow)
    elif type(obj).__module__ == 'numpy' and hasattr(obj, 'item'):
        try: return obj.item()
        except Exception: return str(obj)
    # Catch other potential non-serializable types
    elif hasattr(obj, 'isoformat'): # General catch for date/time like objects
        try: return obj.isoformat()
        except Exception: return str(obj)
    else:
        # Final check: if it's not a basic type, try converting to string
        if not isinstance(obj, (str, int, float, bool, type(None))):
            try:
                json.dumps(obj) # Test serialization
                return obj # It's already serializable
            except TypeError:
                # print(f"DEBUG: Converting unknown type {type(obj)} to string.") # Reduce verbosity
                return str(obj)
        return obj # Return basic types as is

# --- Delta Schema Parsing (No changes needed here) ---
def _parse_delta_schema_string(schema_string):
    """
    Parses Delta's JSON schema string into an Iceberg-like schema dict.
    Assigns sequential IDs and maps 'nullable' to 'required'.
    """
    try:
        delta_schema = json.loads(schema_string)
        if not delta_schema or delta_schema.get("type") != "struct" or not delta_schema.get("fields"):
            print("Warning: Invalid or empty Delta schema string provided.")
            return None

        iceberg_fields = []
        for i, field in enumerate(delta_schema["fields"], 1): # Start ID from 1
            field_name = field.get("name")
            field_type = field.get("type") # TODO: Type mapping might be needed for complex types
            nullable = field.get("nullable", True) # Assume nullable if missing

            if not field_name or not field_type:
                print(f"Warning: Skipping invalid field in Delta schema: {field}")
                continue

            iceberg_fields.append({
                "id": i,
                "name": field_name,
                "required": not nullable, # Iceberg 'required' is inverse of Delta 'nullable'
                "type": field_type,
                # "doc": field.get("metadata", {}).get("comment") # Optional: Add documentation if available
            })

        return {
            "schema-id": 0, # Default schema ID
            "type": "struct",
            "fields": iceberg_fields
        }
    except json.JSONDecodeError as e:
        print(f"Error parsing Delta schema string: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error parsing Delta schema: {e}")
        return None

# --- Schema Comparison Helper Functions ---

def get_iceberg_schema_for_version(client, storage_type, bucket_name, table_base_key, sequence_number, temp_dir):
    """Fetches the Iceberg schema associated with a specific sequence number."""
    print(f"DEBUG: Getting Iceberg schema for sequence_number: {sequence_number}")
    metadata_prefix = os.path.join(table_base_key, "metadata/").replace("\\", "/") # Use key prefix

    # 1. Find latest metadata.json (use abstracted list)
    try:
        # Use abstracted listing
        all_meta_objects, _ = list_storage_items(client, storage_type, bucket_name, metadata_prefix)
    except FileNotFoundError as e:
         print(f"ERROR: Could not list Iceberg metadata at {storage_type}://{bucket_name}/{metadata_prefix}: {e}")
         raise FileNotFoundError(f"Could not list Iceberg metadata prefix: {metadata_prefix}") from e
    except PermissionError as e:
         print(f"ERROR: Permission denied listing Iceberg metadata at {storage_type}://{bucket_name}/{metadata_prefix}: {e}")
         raise PermissionError(f"Permission denied listing Iceberg metadata prefix: {metadata_prefix}") from e


    if not all_meta_objects:
        raise FileNotFoundError(f"No objects found under Iceberg metadata prefix: {metadata_prefix}")

    metadata_files = []
    for obj in all_meta_objects:
        obj_key = obj['Key']
        if obj_key.endswith('.metadata.json'):
            match = re.search(r'(v?\d+)-[a-f0-9-]+\.metadata\.json$', os.path.basename(obj_key))
            version_num = 0
            if match:
                try: version_num = int(match.group(1).lstrip('v'))
                except ValueError: pass
            metadata_files.append({
                'key': obj_key,
                'version': version_num,
                'last_modified': obj['LastModified'] # Use abstracted key name
                 })

    if not metadata_files:
        raise FileNotFoundError(f"No *.metadata.json files found under {metadata_prefix}")

    metadata_files.sort(key=lambda x: (x['version'], x['last_modified']), reverse=True)
    latest_metadata_key = metadata_files[0]['key']
    print(f"DEBUG: Reading latest metadata file: {latest_metadata_key} to find snapshot info")

    local_latest_metadata_path = os.path.join(temp_dir, re.sub(r'[^\w\-.]', '_', os.path.basename(latest_metadata_key)))

    # Use abstracted download
    download_file(client, storage_type, bucket_name, latest_metadata_key, local_latest_metadata_path)
    with open(local_latest_metadata_path, 'r') as f: latest_meta = json.load(f)

    # ... (rest of the logic to find snapshot and schema remains the same) ...
    # 2. Find the snapshot for the target sequence number
    target_snapshot = None
    snapshots = latest_meta.get("snapshots", [])
    for snapshot in snapshots:
        if snapshot.get("sequence-number") == sequence_number:
            target_snapshot = snapshot
            break

    if not target_snapshot:
        raise ValueError(f"Snapshot with sequence number {sequence_number} not found in latest metadata file {latest_metadata_key}.")

    schema_id = target_snapshot.get("schema-id")
    if schema_id is None:
        current_schema_id = latest_meta.get("current-schema-id")
        if current_schema_id is not None:
            schema_id = current_schema_id
        else:
            print(f"Warning: Snapshot {target_snapshot.get('snapshot-id')} has no schema-id, trying top-level schema.")
            schema = latest_meta.get("schema")
            if schema:
                 # Add schema-id if missing for consistency
                 if "schema-id" not in schema: schema["schema-id"] = 0
                 return schema
            raise ValueError(f"Cannot determine schema for snapshot {target_snapshot.get('snapshot-id')} (sequence number {sequence_number}). Missing schema-id and no top-level schema.")

    print(f"DEBUG: Snapshot found (ID: {target_snapshot.get('snapshot-id')}), looking for schema-id: {schema_id}")

    # 3. Find the schema definition in the metadata
    schemas = latest_meta.get("schemas", [])
    target_schema = None
    for schema in schemas:
        if schema.get("schema-id") == schema_id:
            target_schema = schema
            break

    if not target_schema:
        # Handle case where only top-level schema exists (older format?)
        if schema_id == latest_meta.get("current-schema-id") and latest_meta.get("schema"):
            print(f"DEBUG: Schema ID {schema_id} not in 'schemas' list, using top-level 'schema'.")
            target_schema = latest_meta.get("schema")
            # Add schema-id if missing for consistency
            if target_schema and "schema-id" not in target_schema:
                target_schema["schema-id"] = schema_id

    if not target_schema:
        raise ValueError(f"Schema definition with schema-id {schema_id} not found in metadata file {latest_metadata_key}.")

    print(f"DEBUG: Found schema for sequence_number {sequence_number}")
    if os.path.exists(local_latest_metadata_path):
        try: os.remove(local_latest_metadata_path); print(f"DEBUG: Cleaned up {local_latest_metadata_path}")
        except Exception as rm_err: print(f"Warning: Could not remove temp meta file {local_latest_metadata_path}: {rm_err}")

    return target_schema


def get_delta_schema_for_version(client, storage_type, bucket_name, table_base_key, version_id, temp_dir):
    """Fetches the Delta schema active at a specific version ID."""
    print(f"DEBUG: Getting Delta schema for version_id: {version_id}")
    delta_log_prefix = os.path.join(table_base_key, "_delta_log/").replace("\\", "/") # Use key prefix

    # --- 1. Find Delta Log Files --- (use abstracted list)
    try:
        log_files_raw_objects, _ = list_storage_items(client, storage_type, bucket_name, delta_log_prefix)
    except FileNotFoundError as e:
        print(f"ERROR: Delta log prefix '{delta_log_prefix}' inaccessible.")
        raise FileNotFoundError(f"Delta log prefix '{delta_log_prefix}' inaccessible.") from e
    except PermissionError as e:
        print(f"ERROR: Permission denied listing Delta log '{delta_log_prefix}'.")
        raise PermissionError(f"Delta log prefix '{delta_log_prefix}' inaccessible.") from e


    if not log_files_raw_objects:
         try: # Check if base table path exists at least
             _, prefixes = list_storage_items(client, storage_type, bucket_name, table_base_key, delimiter='/', max_keys=1)
             # If we can list the base path but _delta_log is empty, it's likely an issue
             raise FileNotFoundError(f"Delta log prefix '{delta_log_prefix}' is empty or inaccessible.")
         except (FileNotFoundError, PermissionError) as head_err: # Bucket or base path check failed
             raise FileNotFoundError(f"Base table path or Delta log not found/accessible: {storage_type}://{bucket_name}/{table_base_key}") from head_err


    print(f"DEBUG: Found {len(log_files_raw_objects)} total objects under delta log prefix (pre-filter).")

    # --- Filter and Identify Relevant Files ---
    json_commits = {}
    checkpoint_files = {}
    json_pattern = re.compile(r"(\d+)\.json$")
    checkpoint_pattern = re.compile(r"(\d+)\.checkpoint(?:\.(\d+)\.(\d+))?\.parquet$")

    for obj in log_files_raw_objects:
        key = obj['Key']
        filename = os.path.basename(key)
        if (json_match := json_pattern.match(filename)):
            v = int(json_match.group(1))
            if v <= version_id: json_commits[v] = {'key': key}
        elif (cp_match := checkpoint_pattern.match(filename)):
            v, part_num, num_parts = int(cp_match.group(1)), cp_match.group(2), cp_match.group(3)
            if v <= version_id: # Only consider checkpoints at or before the target version
                part_num = int(part_num) if part_num else 1
                num_parts = int(num_parts) if num_parts else 1
                if v not in checkpoint_files: checkpoint_files[v] = {'num_parts': num_parts, 'parts': {}}
                checkpoint_files[v]['parts'][part_num] = {'key': key}
                if checkpoint_files[v]['num_parts'] != num_parts and cp_match.group(3):
                    checkpoint_files[v]['num_parts'] = max(checkpoint_files[v]['num_parts'], num_parts)

    # --- Determine starting point (Closest Checkpoint <= version_id) ---
    start_process_version = 0
    checkpoint_version_used = -1
    relevant_checkpoint_versions = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']], reverse=True)

    if relevant_checkpoint_versions:
        checkpoint_version_used = relevant_checkpoint_versions[0]
        print(f"INFO: Found usable checkpoint version {checkpoint_version_used} <= {version_id}")
        start_process_version = checkpoint_version_used + 1
    else:
        print("INFO: No usable checkpoint found <= target version. Processing JSON logs from version 0.")

    # --- Process Checkpoint and JSON Commits Incrementally ---
    active_metadata = None # Keep track of the last seen metadata

    # --- Load State from Checkpoint if applicable ---
    if checkpoint_version_used > -1:
        print(f"INFO: Reading state from checkpoint version {checkpoint_version_used}")
        cp_info = checkpoint_files[checkpoint_version_used]
        try:
            all_checkpoint_actions = []
            for part_num in sorted(cp_info['parts'].keys()):
                part_key = cp_info['parts'][part_num]['key']
                # Use abstracted checkpoint reader
                all_checkpoint_actions.extend(read_delta_checkpoint(client, storage_type, bucket_name, part_key, temp_dir))

            for action in all_checkpoint_actions:
                if 'metaData' in action and action['metaData']:
                    active_metadata = action['metaData']
                    print(f"DEBUG: Found metaData in checkpoint {checkpoint_version_used}")
                    break # Assume only one metaData per checkpoint/commit

        except Exception as cp_read_err:
            print(f"ERROR: Failed to read/process checkpoint {checkpoint_version_used}: {cp_read_err}. Trying from version 0.")
            start_process_version = 0 # Reset start version
            active_metadata = None    # Reset metadata

    # --- Process JSON Commits Incrementally ---
    versions_to_process = sorted([v for v in json_commits if v >= start_process_version and v <= version_id])
    print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} up to {version_id}...")

    for version in versions_to_process:
        commit_file_info = json_commits[version]
        commit_key = commit_file_info['key']
        print(f"DEBUG: Processing version {version} ({commit_key})...")
        try:
            # Use abstracted JSON reader
            actions = read_delta_json_lines(client, storage_type, bucket_name, commit_key, temp_dir)
            found_meta_in_commit = False
            for action in actions:
                if 'metaData' in action and action['metaData']:
                    active_metadata = action['metaData'] # Update with the latest metadata
                    found_meta_in_commit = True
                    print(f"DEBUG: Updated active metaData from version {version}")
                    break # Assume only one metaData per commit

        except Exception as json_proc_err:
            print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
            raise ValueError(f"Failed to process required commit file for version {version}. Cannot determine schema.") from json_proc_err

    # --- Parse the final active metadata ---
    if not active_metadata:
        raise ValueError(f"Could not find table metadata (metaData action) at or before version {version_id}.")

    schema_string = active_metadata.get("schemaString")
    if not schema_string:
        raise ValueError(f"Metadata found for version {version_id}, but it does not contain a 'schemaString'.")

    parsed_schema = _parse_delta_schema_string(schema_string)
    if not parsed_schema:
        raise ValueError(f"Failed to parse schema string found for version {version_id}.")

    parsed_schema["schema-id"] = active_metadata.get("id", f"delta-v{version_id}") # Use version as pseudo-id if needed

    print(f"DEBUG: Successfully obtained and parsed schema for version_id {version_id}")
    return parsed_schema


# --- Schema Comparison (No changes needed) ---
def compare_schemas(schema1, schema2, version1_label="version1", version2_label="version2"):
    """Compares two schema dictionaries (Iceberg-like format) and returns differences."""
    # ... (logic remains the same) ...
    if not schema1 or not schema1.get("fields"):
       return {"error": f"Schema for {version1_label} is missing or invalid."}
    if not schema2 or not schema2.get("fields"):
       return {"error": f"Schema for {version2_label} is missing or invalid."}

    fields1 = schema1.get("fields", [])
    fields2 = schema2.get("fields", [])

    fields1_map = {f['name']: f for f in fields1}
    fields2_map = {f['name']: f for f in fields2}

    added = []
    removed = []
    modified = []

    # Check for added fields (in schema2 but not in schema1)
    for name, field2 in fields2_map.items():
        if name not in fields1_map:
            added.append(field2)

    # Check for removed and modified fields (in schema1)
    for name, field1 in fields1_map.items():
        if name not in fields2_map:
            removed.append(field1)
        else:
            field2 = fields2_map[name]
            diff_details = {}
            if field1.get('required') != field2.get('required'):
                diff_details['required'] = {'from': field1.get('required'), 'to': field2.get('required')}
            if str(field1.get('type')).strip().lower() != str(field2.get('type')).strip().lower(): # Case-insensitive compare
                diff_details['type'] = {'from': field1.get('type'), 'to': field2.get('type')}
            # if field1.get('doc') != field2.get('doc'):
            #     diff_details['doc'] = {'from': field1.get('doc'), 'to': field2.get('doc')}

            if diff_details:
                modified.append({
                    "name": name,
                    "changes": diff_details,
                })

    return {
        "added": added,
        "removed": removed,
        "modified": modified
    }


# --- Iceberg Endpoint (Updated) ---

@app.route('/Iceberg', methods=['GET'])
def iceberg_details():
    storage_url = request.args.get('s3_url') # Keep param name
    if not storage_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Iceberg Request for {storage_url} ---")

    storage_type = None
    bucket_name = None
    table_base_key = None
    client = None
    endpoint_url = None # S3 specific
    use_ssl = True # S3 specific

    try:
        # Parse URL and create client
        storage_type, bucket_name, table_base_key, endpoint_url, use_ssl = parse_storage_url(storage_url)
        client = create_storage_client(storage_type, endpoint_url, use_ssl, AWS_REGION)

        metadata_prefix = os.path.join(table_base_key, "metadata/").replace("\\", "/")
        print(f"DEBUG: Iceberg metadata prefix: {metadata_prefix}")

        with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory: {temp_dir}")
            iceberg_manifest_files_info = [] # Stores info about manifest list/files

            # 1. Find latest metadata.json (use abstracted list)
            try:
                # Use abstracted list_storage_items
                all_meta_objects, _ = list_storage_items(client, storage_type, bucket_name, metadata_prefix)
            except FileNotFoundError: # Bucket not found
                 return jsonify({"error": f"{storage_type.upper()} bucket not found: {bucket_name}"}), 404
            except PermissionError:
                 return jsonify({"error": f"Permission denied listing metadata path: {storage_type}://{bucket_name}/{metadata_prefix}"}), 403
            # Catch other potential errors from list_storage_items if needed

            if not all_meta_objects:
                # Check if base path exists before declaring metadata empty
                try:
                    # Use abstracted list to check base path existence
                    _, base_prefixes = list_storage_items(client, storage_type, bucket_name, table_base_key, delimiter='/', max_keys=1)
                    # Base path exists, but no metadata files
                    return jsonify({"error": f"No objects found under metadata prefix: {metadata_prefix}"}), 404
                except (FileNotFoundError, PermissionError):
                     # Can't even access base path
                     return jsonify({"error": f"Base table path or metadata prefix not found/accessible: {storage_type}://{bucket_name}/{table_base_key}"}), 404

            metadata_files = []
            for obj in all_meta_objects:
                obj_key = obj['Key']
                if obj_key.endswith('.metadata.json'):
                    match = re.search(r'(v?\d+)-[a-f0-9-]+\.metadata\.json$', os.path.basename(obj_key))
                    version_num = 0
                    if match:
                        try: version_num = int(match.group(1).lstrip('v'))
                        except ValueError: pass
                    metadata_files.append({'key': obj_key, 'version': version_num, 'last_modified': obj['LastModified'], 'size': obj['Size']})

            if not metadata_files:
                return jsonify({"error": f"No *.metadata.json files found under {metadata_prefix}"}), 404

            metadata_files.sort(key=lambda x: (x['version'], x['last_modified']), reverse=True)
            latest_metadata_key = metadata_files[0]['key']
            latest_metadata_size = metadata_files[0]['size']
            print(f"INFO: Using latest metadata file: {latest_metadata_key}")

            local_latest_metadata_path = os.path.join(temp_dir, re.sub(r'[^\w\-.]', '_', os.path.basename(latest_metadata_key)))

            # 2. Parse latest metadata.json (use abstracted download)
            download_file(client, storage_type, bucket_name, latest_metadata_key, local_latest_metadata_path)
            with open(local_latest_metadata_path, 'r') as f: latest_meta = json.load(f)

            # --- Extract metadata fields (logic remains the same) ---
            table_uuid = latest_meta.get("table-uuid")
            current_snapshot_id = latest_meta.get("current-snapshot-id")
            all_snapshots_in_meta = latest_meta.get("snapshots", [])
            current_schema_id = latest_meta.get("current-schema-id", 0)
            default_schema = next((s for s in latest_meta.get("schemas", []) if s.get("schema-id") == current_schema_id), latest_meta.get("schema"))
            current_spec_id = latest_meta.get("current-spec-id", 0)
            partition_spec = next((s for s in latest_meta.get("partition-specs", []) if s.get("spec-id") == current_spec_id), latest_meta.get("partition-spec"))
            current_sort_order_id = latest_meta.get("current-sort-order-id", 0)
            sort_order = next((s for s in latest_meta.get("sort-orders", []) if s.get("order-id") == current_sort_order_id), latest_meta.get("sort-order"))
            properties = latest_meta.get("properties", {})
            format_version = latest_meta.get("format-version", 1)
            snapshot_log = latest_meta.get("snapshot-log", [])
            # --- Schema Map (logic remains same) ---
            schemas_by_id = {s['schema-id']: s for s in latest_meta.get("schemas", []) if 'schema-id' in s}
            top_level_schema = latest_meta.get("schema")
            if top_level_schema:
                top_level_schema_id = top_level_schema.get("schema-id")
                if top_level_schema_id is not None and top_level_schema_id not in schemas_by_id:
                    schemas_by_id[top_level_schema_id] = top_level_schema
                elif top_level_schema_id is None and 0 not in schemas_by_id:
                    top_level_schema["schema-id"] = 0
                    schemas_by_id[0] = top_level_schema
                    print("DEBUG: Assigned schema-id 0 to top-level schema definition.")
            format_configuration = {
               "format-version": format_version, "table-uuid": table_uuid,
               "location": storage_url, "properties": properties,
               "snapshot-log": snapshot_log,
            }

            if current_snapshot_id is None:
                return jsonify({
                    "message": "Table metadata found, but no current snapshot exists.",
                    "table_uuid": table_uuid, "location": storage_url,
                    "format_configuration": format_configuration,
                    "table_schema": default_schema, "partition_spec": partition_spec,
                    "version_history": {"total_snapshots": len(all_snapshots_in_meta), "snapshots_overview": []}
                }), 200

            # 3. Find current snapshot & manifest list (logic remains same)
            current_snapshot = next((s for s in all_snapshots_in_meta if s.get("snapshot-id") == current_snapshot_id), None)
            if not current_snapshot:
                return jsonify({"error": f"Current Snapshot ID {current_snapshot_id} referenced not found."}), 404
            print(f"DEBUG: Current snapshot summary: {current_snapshot.get('summary', {})}")

            manifest_list_path = current_snapshot.get("manifest-list")
            if not manifest_list_path:
                return jsonify({"error": f"Manifest list path missing in current snapshot {current_snapshot_id}"}), 404

            # --- Parse manifest list path (logic remains same, handles s3:// or relative) ---
            manifest_list_key = ""
            manifest_list_bucket = bucket_name # Assume same bucket unless full path given
            manifest_list_storage_type = storage_type # Assume same storage type unless path indicates different (less common)

            try:
                parsed_manifest_list_url = urlparse(unquote(manifest_list_path))
                # Check if manifest list URL points to a *different* storage type or bucket
                # This is less common but possible in Iceberg. For simplicity, we assume it's the same for now.
                # A more robust solution would re-parse and potentially create a new client.
                if parsed_manifest_list_url.scheme.lower() in ["s3", "gs"] or parsed_manifest_list_url.scheme in ["http", "https"]:
                     # If full URI, potentially re-parse to get bucket/key/type
                     ml_type, ml_bucket, manifest_list_key, _, _ = parse_storage_url(manifest_list_path)
                     if ml_type != storage_type:
                          print(f"WARNING: Manifest list type '{ml_type}' differs from table type '{storage_type}'. Using original client, this might fail.")
                          # Ideally, create a new client for ml_type here if needed.
                          manifest_list_bucket = ml_bucket
                     elif ml_bucket != bucket_name:
                          print(f"Warning: Manifest list bucket '{ml_bucket}' differs from table bucket '{bucket_name}'. Using manifest list bucket.")
                          manifest_list_bucket = ml_bucket
                     # Use the same client assuming credentials work across buckets/types if they differ
                elif not parsed_manifest_list_url.scheme and parsed_manifest_list_url.path:
                     # Relative path: construct relative to the metadata file's directory
                     relative_path = parsed_manifest_list_url.path
                     manifest_list_key = os.path.normpath(os.path.join(os.path.dirname(latest_metadata_key), relative_path)).replace("\\", "/")
                     manifest_list_bucket = bucket_name # Assume same bucket for relative paths
                     manifest_list_storage_type = storage_type
                else: raise ValueError(f"Cannot parse manifest list path format: {manifest_list_path}")

                # Get Manifest List Size (use abstracted metadata fetch)
                try:
                    manifest_list_meta = get_storage_object_metadata(client, manifest_list_storage_type, manifest_list_bucket, manifest_list_key)
                    manifest_list_size = manifest_list_meta.get('ContentLength')
                    iceberg_manifest_files_info.append({
                         "file_path": f"{manifest_list_storage_type}://{manifest_list_bucket}/{manifest_list_key}",
                         "size_bytes": manifest_list_size, "size_human": format_bytes(manifest_list_size),
                         "type": "Manifest List" })
                except (FileNotFoundError, PermissionError, Exception) as head_err: # Catch potential errors getting metadata
                    print(f"Warning: Could not get size for manifest list {manifest_list_key}: {head_err}")
                    iceberg_manifest_files_info.append({
                         "file_path": f"{manifest_list_storage_type}://{manifest_list_bucket}/{manifest_list_key}", "size_bytes": None,
                         "size_human": "N/A", "type": "Manifest List (Error getting size)"})

            except ValueError as e: return jsonify({"error": f"Error processing manifest list path '{manifest_list_path}': {e}"}), 400
            except (FileNotFoundError, PermissionError) as e: # Error during potential re-parse
                 return jsonify({"error": f"Cannot access manifest list path '{manifest_list_path}': {e}"}), 404


            local_manifest_list_path = os.path.join(temp_dir, re.sub(r'[^\w\-.]', '_', os.path.basename(manifest_list_key)))

            # 4. Download and parse manifest list (use abstracted download)
            try:
                download_file(client, manifest_list_storage_type, manifest_list_bucket, manifest_list_key, local_manifest_list_path)
                manifest_list_entries = parse_avro_file(local_manifest_list_path)
                print(f"DEBUG: Number of manifest files listed: {len(manifest_list_entries)}")
            except FileNotFoundError as e:
                 return jsonify({"error": f"Manifest list file not found: {manifest_list_storage_type}://{manifest_list_bucket}/{manifest_list_key}"}), 404
            except Exception as e: # Catch Avro parsing errors etc.
                 return jsonify({"error": f"Failed to download or parse manifest list: {e}"}), 500

            # 5. Process Manifest Files (Avro)
            #    (Logic remains largely the same, but uses abstracted download/metadata calls inside loop)
            total_data_files, gross_records_in_data_files, total_delete_files, approx_deleted_records = 0, 0, 0, 0
            total_data_storage_bytes, total_delete_storage_bytes = 0, 0
            partition_stats = {}
            data_file_paths_sample = []

            print("\nINFO: Processing Manifest Files...")
            for i, entry in enumerate(manifest_list_entries):
                manifest_file_path_raw = entry.get("manifest_path")
                print(f"\nDEBUG: Manifest List Entry {i+1}/{len(manifest_list_entries)}: Path='{manifest_file_path_raw}'")
                if not manifest_file_path_raw:
                    print(f"Warning: Skipping manifest list entry {i+1} due to missing 'manifest_path'.")
                    continue

                # --- Resolve manifest file path (like manifest list path resolution) ---
                manifest_file_key = ""
                manifest_bucket = bucket_name # Default
                manifest_storage_type = storage_type # Default
                manifest_file_uri = "" # For display

                try:
                    parsed_manifest_url = urlparse(unquote(manifest_file_path_raw))
                    if parsed_manifest_url.scheme.lower() in ["s3", "gs"] or parsed_manifest_url.scheme in ["http", "https"]:
                         mf_type, mf_bucket, manifest_file_key, _, _ = parse_storage_url(manifest_file_path_raw)
                         manifest_file_uri = manifest_file_path_raw
                         if mf_type != storage_type:
                              print(f"WARNING: Manifest file type '{mf_type}' differs from table type '{storage_type}'. Using original client.")
                              # Assume same client works
                              manifest_storage_type = mf_type
                              manifest_bucket = mf_bucket
                         elif mf_bucket != bucket_name:
                              print(f"Warning: Manifest file bucket '{mf_bucket}' differs. Using manifest file bucket.")
                              manifest_bucket = mf_bucket
                    elif not parsed_manifest_url.scheme and parsed_manifest_url.path:
                         # Relative path: resolve relative to the manifest *list* file's directory
                         relative_path = parsed_manifest_url.path
                         manifest_file_key = os.path.normpath(os.path.join(os.path.dirname(manifest_list_key), relative_path)).replace("\\", "/")
                         manifest_bucket = manifest_list_bucket # Manifest files live relative to list
                         manifest_storage_type = manifest_list_storage_type
                         manifest_file_uri = f"{manifest_storage_type}://{manifest_bucket}/{manifest_file_key}"
                    else: raise ValueError("Cannot parse manifest file path format")

                    # Get Manifest File Size
                    manifest_file_size = entry.get('manifest_length')
                    if manifest_file_size is None:
                        try:
                            mf_meta = get_storage_object_metadata(client, manifest_storage_type, manifest_bucket, manifest_file_key)
                            manifest_file_size = mf_meta.get('ContentLength')
                        except (FileNotFoundError, PermissionError, Exception) as head_err:
                            print(f"Warning: Could not get size for manifest file {manifest_file_key}: {head_err}")
                            manifest_file_size = None

                    iceberg_manifest_files_info.append({
                         "file_path": manifest_file_uri, "size_bytes": manifest_file_size,
                         "size_human": format_bytes(manifest_file_size), "type": "Manifest File"})

                    # Download and parse manifest file (abstracted)
                    local_manifest_path = os.path.join(temp_dir, f"manifest_{i}_" + re.sub(r'[^\w\-.]', '_', os.path.basename(manifest_file_key)))
                    download_file(client, manifest_storage_type, manifest_bucket, manifest_file_key, local_manifest_path)
                    manifest_records = parse_avro_file(local_manifest_path)
                    print(f"DEBUG: Processing {len(manifest_records)} entries in manifest: {os.path.basename(manifest_file_key)}")

                    # --- Process entries within manifest (logic remains same) ---
                    for j, manifest_entry in enumerate(manifest_records):
                        status = manifest_entry.get('status', 0)
                        if status == 2: continue # Skip DELETED entries

                        record_count, file_size, file_path_in_manifest, partition_data = 0, 0, "", None
                        content = 0 # 0: data, 1: position deletes, 2: equality deletes

                        # Extract fields based on Format Version
                        if format_version == 2:
                             if 'data_file' in manifest_entry and manifest_entry['data_file'] is not None:
                                 nested_info = manifest_entry['data_file']
                                 content=0; record_count = nested_info.get("record_count", 0) or 0; file_size = nested_info.get("file_size_in_bytes", 0) or 0; file_path_in_manifest = nested_info.get("file_path", ""); partition_data = nested_info.get("partition")
                             elif 'delete_file' in manifest_entry and manifest_entry['delete_file'] is not None:
                                 nested_info = manifest_entry['delete_file']
                                 content = nested_info.get("content", 1); record_count = nested_info.get("record_count", 0) or 0; file_size = nested_info.get("file_size_in_bytes", 0) or 0; file_path_in_manifest = nested_info.get("file_path", "")
                             else: continue
                        elif format_version == 1:
                             content=0; record_count = manifest_entry.get("record_count", 0) or 0; file_size = manifest_entry.get("file_size_in_bytes", 0) or 0; file_path_in_manifest = manifest_entry.get("file_path", ""); partition_data = manifest_entry.get("partition")
                        else: continue

                        # Resolve data/delete file path (relative to table base or absolute)
                        full_file_key = ""
                        file_bucket = bucket_name # Default to table bucket
                        file_storage_type = storage_type # Default to table type
                        try:
                             parsed_file_path = urlparse(unquote(file_path_in_manifest))
                             if parsed_file_path.scheme.lower() in ["s3", "gs"] or parsed_file_path.scheme in ["http", "https"]:
                                 df_type, df_bucket, full_file_key, _, _ = parse_storage_url(file_path_in_manifest)
                                 file_storage_type = df_type
                                 file_bucket = df_bucket
                             elif not parsed_file_path.scheme and parsed_file_path.path:
                                 # Usually absolute paths from bucket root in manifest, even without scheme
                                 if parsed_file_path.path.startswith('/'):
                                      full_file_key = parsed_file_path.path.lstrip('/')
                                 else: # Or assume relative to table base path if not absolute-looking
                                      full_file_key = os.path.join(table_base_key, parsed_file_path.path).replace("\\","/")
                                 file_bucket = bucket_name
                                 file_storage_type = storage_type
                             else:
                                 print(f"Warning: Skipping file with unparseable path format: {file_path_in_manifest}")
                                 continue

                        except ValueError as path_err:
                            print(f"Warning: Error parsing file path '{file_path_in_manifest}': {path_err}. Skipping accumulation.")
                            continue

                        # Accumulate stats (logic remains same)
                        if content == 0: # Data File
                            total_data_files += 1
                            gross_records_in_data_files += record_count
                            total_data_storage_bytes += file_size
                            partition_key_string = "<unpartitioned>"
                            partition_values_repr = None
                            if partition_data is not None and partition_spec and partition_spec.get('fields'):
                                try:
                                    field_names = [f['name'] for f in partition_spec['fields']]
                                    if format_version == 2 and isinstance(partition_data, dict):
                                        partition_values_repr = {name: partition_data.get(name) for name in field_names if name in partition_data}
                                    elif format_version == 1 and isinstance(partition_data, (list, tuple)) and len(partition_data) == len(field_names):
                                        partition_values_repr = dict(zip(field_names, partition_data))
                                    else: partition_values_repr = {'_raw': str(partition_data)}
                                    partition_key_string = json.dumps(dict(sorted(convert_bytes(partition_values_repr).items())), default=str)
                                except Exception as part_err:
                                    print(f"Warning: Error processing partition data {partition_data}: {part_err}")
                                    partition_key_string = f"<error: {part_err}>"; partition_values_repr = {'_error': str(part_err)}
                            elif partition_data is None and partition_spec and not partition_spec.get('fields'): partition_key_string = "<unpartitioned>"
                            elif partition_data is not None: partition_key_string = str(partition_data); partition_values_repr = {'_raw': partition_data}

                            if partition_key_string not in partition_stats: partition_stats[partition_key_string] = {"gross_record_count": 0, "size_bytes": 0, "num_data_files": 0, "partition_values": partition_values_repr}
                            partition_stats[partition_key_string]["gross_record_count"] += record_count
                            partition_stats[partition_key_string]["size_bytes"] += file_size
                            partition_stats[partition_key_string]["num_data_files"] += 1

                            # Get sample path (abstracted path info)
                            if full_file_key and len(data_file_paths_sample) < 1 and full_file_key.lower().endswith(".parquet"):
                                data_file_paths_sample.append({'type': file_storage_type, 'bucket': file_bucket, 'key': full_file_key})

                        elif content == 1 or content == 2: # Delete File
                            total_delete_files += 1
                            approx_deleted_records += record_count
                            total_delete_storage_bytes += file_size

                except Exception as manifest_err:
                    print(f"ERROR: Failed to process manifest file {manifest_file_uri or manifest_file_path_raw}: {manifest_err}")
                    traceback.print_exc()
                    iceberg_manifest_files_info.append({
                         "file_path": manifest_file_uri or manifest_file_path_raw, "size_bytes": None,
                         "size_human": "N/A", "type": "Manifest File (Error Processing)"})
                finally:
                    if 'local_manifest_path' in locals() and os.path.exists(local_manifest_path):
                        try: os.remove(local_manifest_path); # print(f"DEBUG: Cleaned up {local_manifest_path}")
                        except Exception as rm_err: print(f"Warning: Could not remove temp manifest file {local_manifest_path}: {rm_err}")

            print("INFO: Finished processing manifest files.")

            # 6. Get Sample Data (use abstracted reader)
            sample_data = []
            if data_file_paths_sample:
                sample_file_info = data_file_paths_sample[0]
                print(f"INFO: Attempting to get sample data from: {sample_file_info['type']}://{sample_file_info['bucket']}/{sample_file_info['key']}")
                try:
                    # Use abstracted parquet reader
                    sample_data = read_parquet_sample(
                        client, sample_file_info['type'], sample_file_info['bucket'],
                        sample_file_info['key'], temp_dir, num_rows=10
                    )
                except Exception as sample_err:
                    sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
            else: print("INFO: No suitable Parquet data file found in manifests for sampling.")

            # 7. Assemble the final result (logic remains the same)
            print("\nINFO: Assembling final Iceberg result...")
            # ... (calculations for approx_live_records, avg sizes, partition_explorer_data remain same) ...
            approx_live_records = max(0, gross_records_in_data_files - approx_deleted_records)
            avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
            avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024)) if total_data_files > 0 else 0

            partition_explorer_data = []
            for k, v in partition_stats.items():
                partition_explorer_data.append({
                    "partition_values": v["partition_values"], "partition_key_string": k,
                    "gross_record_count": v["gross_record_count"], "size_bytes": v["size_bytes"],
                    "size_human": format_bytes(v["size_bytes"]), "num_data_files": v["num_data_files"] })
            partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))

            # --- Assemble Version History (logic remains same) ---
            snapshots_overview_with_schema = []
            history_limit = 20
            snapshots_to_process = sorted(all_snapshots_in_meta, key=lambda x: x.get('timestamp-ms', 0), reverse=True)

            for snapshot in snapshots_to_process[:min(len(snapshots_to_process), history_limit)]:
                snapshot_schema_id = snapshot.get('schema-id')
                schema_definition = None
                if snapshot_schema_id is not None:
                    schema_definition = schemas_by_id.get(snapshot_schema_id)
                    if not schema_definition:
                        print(f"Warning: Schema definition for schema-id {snapshot_schema_id} (snapshot {snapshot.get('snapshot-id')}) not found.")
                elif default_schema:
                    schema_definition = default_schema
                    print(f"Warning: Snapshot {snapshot.get('snapshot-id')} missing schema-id, using default schema (ID: {default_schema.get('schema-id')}).")

                overview_entry = {
                    "snapshot-id": snapshot.get("snapshot-id"), "timestamp-ms": snapshot.get("timestamp-ms"),
                    "sequence-number": snapshot.get("sequence-number"), "summary": snapshot.get("summary", {}),
                    "manifest-list": snapshot.get("manifest-list"), "parent-snapshot-id": snapshot.get("parent-snapshot-id"),
                    "schema_definition": schema_definition # Embed the schema
                }
                snapshots_overview_with_schema.append(overview_entry)

            current_snapshot_summary_for_history = next((s for s in snapshots_overview_with_schema if s['snapshot-id'] == current_snapshot_id), None)

            # Final Result Structure (logic remains same)
            result = {
                "table_type": "Iceberg", "table_uuid": table_uuid, "location": storage_url,
                "format_configuration": format_configuration, "iceberg_manifest_files": iceberg_manifest_files_info,
                "format_version": format_version, "current_snapshot_id": current_snapshot_id,
                "table_schema": default_schema, "partition_spec": partition_spec, "sort_order": sort_order,
                "version_history": {
                    "total_snapshots": len(all_snapshots_in_meta),
                    "current_snapshot_summary": current_snapshot_summary_for_history,
                    "snapshots_overview": snapshots_overview_with_schema },
                "key_metrics": {
                    "total_data_files": total_data_files, "total_delete_files": total_delete_files,
                    "gross_records_in_data_files": gross_records_in_data_files,
                    "approx_deleted_records_in_manifests": approx_deleted_records,
                    "approx_live_records": approx_live_records,
                    "metrics_note": "Live record count is approximate based on manifest metadata. Partition record counts are gross counts.",
                    "total_data_storage_bytes": total_data_storage_bytes, "total_data_storage_human": format_bytes(total_data_storage_bytes),
                    "total_delete_storage_bytes": total_delete_storage_bytes, "total_delete_storage_human": format_bytes(total_delete_storage_bytes),
                    "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                    "avg_data_file_size_mb": round(avg_data_file_size_mb, 4), },
                "partition_explorer": partition_explorer_data, "sample_data": sample_data,
            }

            result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))

            end_time = time.time()
            print(f"--- Iceberg Request Completed in {end_time - start_time:.2f} seconds ---")
            # Clean up temp files
            if os.path.exists(local_latest_metadata_path):
                try: os.remove(local_latest_metadata_path); print(f"DEBUG: Cleaned up {local_latest_metadata_path}")
                except Exception as rm_err: print(f"Warning: Could not remove temp meta file {local_latest_metadata_path}: {rm_err}")
            if 'local_manifest_list_path' in locals() and os.path.exists(local_manifest_list_path):
                try: os.remove(local_manifest_list_path); print(f"DEBUG: Cleaned up {local_manifest_list_path}")
                except Exception as rm_err: print(f"Warning: Could not remove temp man list file {local_manifest_list_path}: {rm_err}")

            return jsonify(result_serializable), 200

    # --- Exception Handling (Updated for GCS) ---
    except NoCredentialsError as e: # S3/MinIO specific
        return jsonify({"error": f"S3/MinIO credentials not found or invalid: {e}"}), 401
    except gcs_auth_exceptions.DefaultCredentialsError as e: # GCS specific
        return jsonify({"error": f"GCS credentials not found or invalid: {e}"}), 401
    except ClientError as e: # Catch other Boto3 errors
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"ERROR: S3 ClientError in Iceberg endpoint: {error_code} - {e}")
        traceback.print_exc()
        if error_code == 'NoSuchBucket': return jsonify({"error": f"S3 bucket not found: {bucket_name}"}), 404
        elif error_code == 'AccessDenied': return jsonify({"error": f"S3 access denied for s3://{bucket_name}/{table_base_key}"}), 403
        else: return jsonify({"error": f"S3 ClientError ({error_code}): {e}"}), 500
    except gcs_exceptions.NotFound as e: # GCS general not found (could be bucket)
         print(f"ERROR: GCS resource not found: {e}")
         # Check if it's likely the bucket vs. an object later in the process
         if "bucket" in str(e).lower():
              return jsonify({"error": f"GCS bucket not found: {bucket_name}"}), 404
         else: # Assume object not found during processing
              return jsonify({"error": f"Required GCS object not found: {e}"}), 404
    except gcs_exceptions.Forbidden as e: # GCS permissions
         print(f"ERROR: GCS access denied: {e}")
         return jsonify({"error": f"GCS access denied for gs://{bucket_name}/{table_base_key}"}), 403
    except FileNotFoundError as e:
        print(f"ERROR: FileNotFoundError in Iceberg endpoint: {e}")
        return jsonify({"error": f"Required file or resource not found: {e}"}), 404
    except ValueError as e:
        print(f"ERROR: ValueError in Iceberg endpoint: {e}")
        return jsonify({"error": f"Input value or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred in Iceberg endpoint: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


# --- Delta Lake Endpoint (Updated) ---

@app.route('/Delta', methods=['GET'])
def delta_details():
    storage_url = request.args.get('s3_url') # Keep param name
    if not storage_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Delta Request for {storage_url} ---")

    storage_type = None
    bucket_name = None
    table_base_key = None
    delta_log_prefix = None
    client = None
    endpoint_url = None
    use_ssl = True

    try:
        # Parse URL and create client
        storage_type, bucket_name, table_base_key, endpoint_url, use_ssl = parse_storage_url(storage_url)
        client = create_storage_client(storage_type, endpoint_url, use_ssl, AWS_REGION)

        delta_log_prefix = os.path.join(table_base_key, "_delta_log/").replace("\\", "/")
        print(f"INFO: Processing Delta Lake table at: {storage_type}://{bucket_name}/{table_base_key}")
        print(f"DEBUG: Delta log prefix: {delta_log_prefix}")

        with tempfile.TemporaryDirectory(prefix="delta_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory: {temp_dir}")

            # --- 1. Find Delta Log Files (use abstracted list) ---
            try:
                 log_files_raw_objects, _ = list_storage_items(client, storage_type, bucket_name, delta_log_prefix)
            except FileNotFoundError: # Bucket not found
                 return jsonify({"error": f"{storage_type.upper()} bucket not found: {bucket_name}"}), 404
            except PermissionError:
                 return jsonify({"error": f"Permission denied listing Delta log path: {storage_type}://{bucket_name}/{delta_log_prefix}"}), 403


            if not log_files_raw_objects:
                 try: # Check if base table path exists
                     _, base_prefixes = list_storage_items(client, storage_type, bucket_name, table_base_key, delimiter='/', max_keys=1)
                     return jsonify({"error": f"Delta log prefix '{delta_log_prefix}' is empty."}), 404
                 except (FileNotFoundError, PermissionError):
                      return jsonify({"error": f"Base table path or Delta log not found/accessible: {storage_type}://{bucket_name}/{table_base_key}"}), 404

            print(f"DEBUG: Found {len(log_files_raw_objects)} total objects under delta log prefix.")

            # --- Collect Metadata File Info ---
            delta_log_files_info = []
            json_commits = {}
            checkpoint_files = {}
            last_checkpoint_info = None
            json_pattern = re.compile(r"(\d+)\.json$")
            checkpoint_pattern = re.compile(r"(\d+)\.checkpoint(?:\.(\d+)\.(\d+))?\.parquet$")

            for obj in log_files_raw_objects:
                key = obj['Key']
                filename = os.path.basename(key)
                size = obj.get('Size')
                last_modified = obj.get('LastModified')

                if filename == "_last_checkpoint" or json_pattern.match(filename) or checkpoint_pattern.match(filename):
                    delta_log_files_info.append({
                        "file_path": f"{storage_type}://{bucket_name}/{key}",
                        "relative_path": key.replace(table_base_key, "", 1).lstrip('/'), # Ensure relative path is clean
                        "size_bytes": size, "size_human": format_bytes(size) })

                if filename == "_last_checkpoint":
                    local_last_cp_path = os.path.join(temp_dir, "_last_checkpoint")
                    try:
                        # Use abstracted download
                        download_file(client, storage_type, bucket_name, key, local_last_cp_path)
                        with open(local_last_cp_path, 'r') as f: last_checkpoint_data = json.load(f)
                        last_checkpoint_info = {'version': last_checkpoint_data['version'], 'parts': last_checkpoint_data.get('parts'), 'key': key, 'size': size}
                        if os.path.exists(local_last_cp_path): os.remove(local_last_cp_path) # Clean up immediately
                    except Exception as cp_err: print(f"Warning: Failed to read/parse _last_checkpoint {key}: {cp_err}")
                elif (json_match := json_pattern.match(filename)):
                    json_commits[int(json_match.group(1))] = {'key': key, 'last_modified': last_modified, 'size': size}
                elif (cp_match := checkpoint_pattern.match(filename)):
                    version, part_num, num_parts = int(cp_match.group(1)), cp_match.group(2), cp_match.group(3)
                    part_num = int(part_num) if part_num else 1
                    num_parts = int(num_parts) if num_parts else 1
                    if version not in checkpoint_files: checkpoint_files[version] = {'num_parts': num_parts, 'parts': {}}
                    checkpoint_files[version]['parts'][part_num] = {'key': key, 'last_modified': last_modified, 'size': size}
                    if checkpoint_files[version]['num_parts'] != num_parts and cp_match.group(3):
                         checkpoint_files[version]['num_parts'] = max(checkpoint_files[version]['num_parts'], num_parts)

            delta_log_files_info.sort(key=lambda x: x.get('relative_path', ''))

            # Determine latest version ID (logic remains same)
            current_snapshot_id = -1
            if json_commits: current_snapshot_id = max(json_commits.keys())
            elif last_checkpoint_info: current_snapshot_id = last_checkpoint_info['version']
            elif checkpoint_files:
                complete_cp_versions = [v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']]
                if complete_cp_versions: current_snapshot_id = max(complete_cp_versions)
                elif checkpoint_files: current_snapshot_id = max(checkpoint_files.keys())

            if current_snapshot_id == -1:
                return jsonify({"error": "No Delta commit JSON files or checkpoint files found."}), 404
            print(f"INFO: Latest Delta version (snapshot ID) identified: {current_snapshot_id}")

            # --- 2/3. Process Checkpoint and JSON Commits Incrementally ---
            #    (Uses abstracted download/read functions inside)
            active_files = {}
            metadata_from_log = None
            protocol_from_log = None
            all_commit_info = {}
            processed_versions = set()
            checkpoint_version_used = None
            start_process_version = 0
            effective_checkpoint_version = -1
            loaded_schema_from_checkpoint = None

            # Determine starting checkpoint (logic remains same)
            cp_version_candidate = -1
            if last_checkpoint_info: cp_version_candidate = last_checkpoint_info['version']
            elif checkpoint_files:
                 available_complete_checkpoints = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']], reverse=True)
                 if available_complete_checkpoints: cp_version_candidate = available_complete_checkpoints[0]

            if cp_version_candidate > -1:
                 if cp_version_candidate in checkpoint_files and len(checkpoint_files[cp_version_candidate]['parts']) == checkpoint_files[cp_version_candidate]['num_parts']:
                     effective_checkpoint_version = cp_version_candidate
                 else:
                     available_complete_checkpoints = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts'] and v < cp_version_candidate], reverse=True)
                     if available_complete_checkpoints: effective_checkpoint_version = available_complete_checkpoints[0]

            # Load State and Initial Schema from Checkpoint (uses abstracted read_delta_checkpoint)
            if effective_checkpoint_version > -1:
                print(f"INFO: Reading state from checkpoint version {effective_checkpoint_version}")
                checkpoint_version_used = effective_checkpoint_version
                cp_info = checkpoint_files[effective_checkpoint_version]
                try:
                    all_checkpoint_actions = []
                    for part_num in sorted(cp_info['parts'].keys()):
                        part_key = cp_info['parts'][part_num]['key']
                        # Use abstracted reader
                        all_checkpoint_actions.extend(read_delta_checkpoint(client, storage_type, bucket_name, part_key, temp_dir))

                    # Process checkpoint actions (logic remains same)
                    for action in all_checkpoint_actions:
                        if 'add' in action and action['add']:
                             add_info = action['add']; path = add_info['path']
                             stats_parsed = json.loads(add_info['stats']) if isinstance(add_info.get('stats'), str) else add_info.get('stats')
                             active_files[path] = { 'size': add_info.get('size'), 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': add_info.get('modificationTime', 0), 'stats': stats_parsed, 'tags': add_info.get('tags') }
                        elif 'metaData' in action and action['metaData']:
                             metadata_from_log = action['metaData']
                             schema_str = metadata_from_log.get("schemaString")
                             if schema_str:
                                 parsed_schema = _parse_delta_schema_string(schema_str)
                                 if parsed_schema:
                                     loaded_schema_from_checkpoint = parsed_schema
                                     print(f"DEBUG: Loaded schema definition from checkpoint {effective_checkpoint_version}")
                        elif 'protocol' in action and action['protocol']:
                             protocol_from_log = action['protocol']

                    start_process_version = effective_checkpoint_version + 1
                    processed_versions.add(effective_checkpoint_version)

                    # Add checkpoint info to history (logic remains same)
                    cp_total_files = len(active_files)
                    cp_total_bytes = sum(f['size'] for f in active_files.values() if f.get('size'))
                    cp_total_records = sum(int(f['stats']['numRecords']) for f in active_files.values() if f.get('stats') and f['stats'].get('numRecords') is not None)
                    all_commit_info[effective_checkpoint_version] = {
                         'version': effective_checkpoint_version, 'timestamp': None, 'operation': 'CHECKPOINT_LOAD', 'operationParameters': {},
                         'num_added_files': cp_total_files, 'num_removed_files': 0, 'added_bytes': cp_total_bytes, 'removed_bytes': 0,
                         'metrics': {'numOutputFiles': str(cp_total_files), 'numOutputBytes': str(cp_total_bytes), 'numOutputRows': str(cp_total_records)},
                         'total_files_at_version': cp_total_files, 'total_bytes_at_version': cp_total_bytes, 'total_records_at_version': cp_total_records,
                         'schema_definition': loaded_schema_from_checkpoint }
                    print(f"INFO: Checkpoint processing complete. Starting JSON processing from version {start_process_version}")

                except Exception as cp_read_err:
                    print(f"ERROR: Failed to read/process checkpoint {effective_checkpoint_version}: {cp_read_err}. Falling back.")
                    active_files = {}; metadata_from_log = None; protocol_from_log = None; loaded_schema_from_checkpoint = None
                    start_process_version = 0; checkpoint_version_used = None; processed_versions = set(); all_commit_info = {}
            else:
                print("INFO: No usable checkpoint found. Processing JSON logs from version 0.")
                start_process_version = 0

            # Process JSON Commits (uses abstracted read_delta_json_lines)
            versions_to_process = sorted([v for v in json_commits if v >= start_process_version])
            print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} up to {current_snapshot_id}...")
            removed_file_sizes_by_commit = {}

            for version in versions_to_process:
                if version in processed_versions: continue
                commit_file_info = json_commits[version]
                commit_key = commit_file_info['key']
                print(f"DEBUG: Processing version {version} ({commit_key})...")
                removed_file_sizes_by_commit[version] = 0
                commit_schema_definition = None

                try:
                    # Use abstracted reader
                    actions = read_delta_json_lines(client, storage_type, bucket_name, commit_key, temp_dir)
                    commit_summary = { 'version': version, 'timestamp': None, 'operation': 'Unknown', 'num_actions': len(actions), 'operationParameters': {}, 'num_added_files': 0, 'num_removed_files': 0, 'added_bytes': 0, 'removed_bytes': 0, 'metrics': {} }
                    op_metrics = {}

                    # Process actions (logic remains same)
                    for action in actions:
                         if 'commitInfo' in action and action['commitInfo']:
                             ci = action['commitInfo']; commit_summary['timestamp'] = ci.get('timestamp'); commit_summary['operation'] = ci.get('operation', 'Unknown'); commit_summary['operationParameters'] = ci.get('operationParameters', {}); op_metrics = ci.get('operationMetrics', {}); commit_summary['metrics'] = op_metrics
                             commit_summary['num_added_files'] = int(op_metrics.get('numOutputFiles', 0))
                             commit_summary['num_removed_files'] = int(op_metrics.get('numTargetFilesRemoved', op_metrics.get('numRemovedFiles', 0)))
                             commit_summary['added_bytes'] = int(op_metrics.get('numOutputBytes', 0))
                             commit_summary['removed_bytes'] = int(op_metrics.get('numTargetBytesRemoved', op_metrics.get('numRemovedBytes', 0)))
                         elif 'add' in action and action['add']:
                             add_info = action['add']; path = add_info['path']; file_size = add_info.get('size', 0)
                             stats_parsed = json.loads(add_info['stats']) if isinstance(add_info.get('stats'), str) else add_info.get('stats')
                             active_files[path] = { 'size': file_size, 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': add_info.get('modificationTime', 0), 'stats': stats_parsed, 'tags': add_info.get('tags') }
                             if 'numOutputFiles' not in op_metrics: commit_summary['num_added_files'] += 1
                             if 'numOutputBytes' not in op_metrics: commit_summary['added_bytes'] += file_size
                         elif 'remove' in action and action['remove']:
                             remove_info = action['remove']; path = remove_info['path']
                             if remove_info.get('dataChange', True):
                                 removed_file_info = active_files.pop(path, None)
                                 if 'numTargetFilesRemoved' not in op_metrics and 'numRemovedFiles' not in op_metrics: commit_summary['num_removed_files'] += 1
                                 if removed_file_info and removed_file_info.get('size'): removed_file_sizes_by_commit[version] += removed_file_info.get('size',0)
                         elif 'metaData' in action and action['metaData']:
                             metadata_from_log = action['metaData']
                             schema_str = metadata_from_log.get("schemaString")
                             if schema_str:
                                 parsed_schema = _parse_delta_schema_string(schema_str)
                                 if parsed_schema:
                                     commit_schema_definition = parsed_schema
                                     print(f"DEBUG: Found schema update in version {version}")
                         elif 'protocol' in action and action['protocol']:
                             protocol_from_log = action['protocol']

                    # Calculate removed bytes if needed (logic same)
                    if commit_summary['removed_bytes'] == 0 and removed_file_sizes_by_commit.get(version, 0) > 0:
                         if 'numTargetBytesRemoved' not in op_metrics and 'numRemovedBytes' not in op_metrics: commit_summary['removed_bytes'] = removed_file_sizes_by_commit[version]

                    # Calculate and store cumulative state (logic same)
                    current_total_files = len(active_files)
                    current_total_bytes = sum(f['size'] for f in active_files.values() if f.get('size'))
                    current_total_records = sum(int(f['stats']['numRecords']) for f in active_files.values() if f.get('stats') and f['stats'].get('numRecords') is not None)
                    commit_summary['total_files_at_version'] = current_total_files
                    commit_summary['total_bytes_at_version'] = current_total_bytes
                    commit_summary['total_records_at_version'] = current_total_records
                    commit_summary['schema_definition'] = commit_schema_definition

                    all_commit_info[version] = commit_summary
                    processed_versions.add(version)

                except Exception as json_proc_err:
                    print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
                    traceback.print_exc()
                    all_commit_info[version] = {'version': version, 'error': str(json_proc_err)}
                    processed_versions.add(version) # Mark as processed even with error

            print(f"INFO: Finished incremental processing. Propagating schemas...")

            # --- Propagate Schema Definitions Forward (logic remains same) ---
            last_known_schema = loaded_schema_from_checkpoint
            sorted_processed_versions = sorted([v for v in processed_versions if v in all_commit_info and 'error' not in all_commit_info[v]])
            for v in sorted_processed_versions:
                current_commit = all_commit_info[v]
                if 'schema_definition' in current_commit and current_commit['schema_definition']:
                    last_known_schema = current_commit['schema_definition']
                elif last_known_schema:
                    current_commit['schema_definition'] = last_known_schema
                else:
                     if v == min(sorted_processed_versions): print(f"Warning: Schema definition missing for initial processed version {v}.")
                     else: print(f"Warning: Inherited schema is missing for version {v}.")
                     current_commit['schema_definition'] = None

            # Find Definitive Metadata & Protocol (logic remains same)
            definitive_metadata = metadata_from_log
            definitive_protocol = protocol_from_log
            if not definitive_metadata:
                latest_successful_v = max(sorted_processed_versions) if sorted_processed_versions else -1
                if latest_successful_v > -1 and all_commit_info[latest_successful_v].get('schema_definition'):
                     print("Warning: Final metadata action missing, relying on schema from last successful version.")
                     print("ERROR: Cannot reconstruct full final metadata. Table properties might be incomplete.")
                     definitive_metadata = {} # Incomplete
                else: return jsonify({"error": "Could not determine final table metadata (metaData action missing)."}), 500
            if not definitive_protocol:
                 print("Warning: Protocol action not found in recent history. Using default.")
                 definitive_protocol = {"minReaderVersion": 1, "minWriterVersion": 2}


            # Assemble Format Configuration, Final Schema, Partition Spec (logic remains same)
            format_configuration = {**(definitive_protocol), **(definitive_metadata.get("configuration", {}) if definitive_metadata else {})}
            final_table_schema = all_commit_info.get(current_snapshot_id, {}).get('schema_definition')
            if not final_table_schema:
                if last_known_schema:
                    print(f"Warning: Schema for latest version {current_snapshot_id} missing, using schema from last known.")
                    final_table_schema = last_known_schema
                elif definitive_metadata and definitive_metadata.get("schemaString"):
                    print(f"Warning: Schema for latest version {current_snapshot_id} missing, parsing from last seen metadata action.")
                    final_table_schema = _parse_delta_schema_string(definitive_metadata.get("schemaString"))
                if not final_table_schema: return jsonify({"error": "Failed to determine final table schema."}), 500

            partition_cols = definitive_metadata.get("partitionColumns", []) if definitive_metadata else []
            partition_spec_fields = []
            schema_fields_map = {f['name']: f for f in final_table_schema.get('fields', [])}
            for i, col_name in enumerate(partition_cols):
                source_field = schema_fields_map.get(col_name)
                if source_field: partition_spec_fields.append({ "name": col_name, "transform": "identity", "source-id": source_field.get('id', i+1000), "field-id": 1000 + i })
            partition_spec = {"spec-id": 0, "fields": partition_spec_fields}

            # Calculate FINAL State Metrics (logic remains same)
            final_commit_details = all_commit_info.get(current_snapshot_id)
            if not final_commit_details or final_commit_details.get('error'):
                 latest_successful_v = max(sorted_processed_versions) if sorted_processed_versions else -1
                 if latest_successful_v > -1:
                     final_commit_details = all_commit_info[latest_successful_v]
                     print(f"Warning: Using metrics from last successful version {latest_successful_v} as latest ({current_snapshot_id}) failed.")
                 else: final_commit_details = {}; print("Error: Could not retrieve metrics for any version.")

            total_data_files = final_commit_details.get('total_files_at_version', 0)
            total_data_storage_bytes = final_commit_details.get('total_bytes_at_version', 0)
            approx_live_records = final_commit_details.get('total_records_at_version', 0)
            gross_records_in_data_files = approx_live_records; total_delete_files = 0; total_delete_storage_bytes = 0; approx_deleted_records_in_manifests = 0
            avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
            avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024)) if total_data_files > 0 else 0
            metrics_note = f"Live record count ({approx_live_records}) estimated from Delta log stats. Delete metrics are N/A."


            # Calculate Partition Stats (logic remains same)
            partition_stats = {}
            for path, file_info in active_files.items():
                part_values = file_info.get('partitionValues', {})
                part_key_string = json.dumps(dict(sorted(part_values.items())), default=str) if part_values else "<unpartitioned>"
                if part_key_string not in partition_stats: partition_stats[part_key_string] = { "partition_values": part_values, "partition_key_string": part_key_string, "num_data_files": 0, "size_bytes": 0, "gross_record_count": 0 }
                partition_stats[part_key_string]["num_data_files"] += 1
                partition_stats[part_key_string]["size_bytes"] += file_info.get('size', 0)
                if file_info.get('stats') and file_info['stats'].get('numRecords') is not None:
                    try: partition_stats[part_key_string]["gross_record_count"] += int(file_info['stats']['numRecords'])
                    except (ValueError, TypeError): pass
            partition_explorer_data = list(partition_stats.values())
            for p_data in partition_explorer_data: p_data["size_human"] = format_bytes(p_data["size_bytes"])
            partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))

            # Get Sample Data (use abstracted reader)
            sample_data = []
            if active_files:
                sample_file_path_relative = next((p for p in active_files if p.lower().endswith('.parquet')), list(active_files.keys())[0] if active_files else None)
                if sample_file_path_relative:
                    full_sample_s3_key = os.path.join(table_base_key, sample_file_path_relative).replace("\\", "/")
                    print(f"INFO: Attempting sample from: {storage_type}://{bucket_name}/{full_sample_s3_key}")
                    try:
                        if sample_file_path_relative.lower().endswith('.parquet'):
                            # Use abstracted reader
                            sample_data = read_parquet_sample(client, storage_type, bucket_name, full_sample_s3_key, temp_dir, num_rows=10)
                        else: sample_data = [{"error": "Sampling only implemented for Parquet files"}]
                    except Exception as sample_err: sample_data = [{"error": "Failed to read sample data", "details": str(sample_err)}]
                else: sample_data = [{"error": "No active files found for sampling"}]
            else: sample_data = [{"error": "No active files found for sampling"}]

            # --- Assemble Final Result (logic remains same) ---
            print("\nINFO: Assembling final Delta result...")
            final_commit_details_for_display = all_commit_info.get(current_snapshot_id, {})
            current_snapshot_details = {
                 "version": current_snapshot_id, "timestamp_ms": final_commit_details_for_display.get('timestamp'),
                 "timestamp_iso": format_timestamp_ms(final_commit_details_for_display.get('timestamp')),
                 "operation": final_commit_details_for_display.get('operation', 'N/A'),
                 "operation_parameters": final_commit_details_for_display.get('operationParameters', {}),
                 "num_files_total_snapshot": total_data_files, "total_data_files_snapshot": total_data_files,
                 "total_delete_files_snapshot": total_delete_files, "total_data_storage_bytes_snapshot": total_data_storage_bytes,
                 "total_records_snapshot": approx_live_records,
                 "num_added_files_commit": final_commit_details_for_display.get('num_added_files'),
                 "num_removed_files_commit": final_commit_details_for_display.get('num_removed_files'),
                 "commit_added_bytes": final_commit_details_for_display.get('added_bytes'),
                 "commit_removed_bytes": final_commit_details_for_display.get('removed_bytes'),
                 "commit_metrics_raw": final_commit_details_for_display.get('metrics', {}),
                 "error": final_commit_details_for_display.get('error') }

            snapshots_overview_with_schema = []
            known_versions_sorted_hist = sorted(all_commit_info.keys(), reverse=True)
            history_limit = 20; versions_in_history = known_versions_sorted_hist[:min(len(known_versions_sorted_hist), history_limit)]
            current_snapshot_summary_for_history = None
            for v in versions_in_history:
                commit_details = all_commit_info.get(v);
                if not commit_details: continue
                summary = { "operation": commit_details.get('operation', 'Unknown'), "added-data-files": str(commit_details.get('num_added_files', 'N/A')), "removed-data-files": str(commit_details.get('num_removed_files', 'N/A')), "added-files-size": str(commit_details.get('added_bytes', 'N/A')), "removed-files-size": str(commit_details.get('removed_bytes', 'N/A')), "operation-parameters": commit_details.get('operationParameters', {}), "total-data-files": str(commit_details.get('total_files_at_version', 'N/A')), "total-files-size": str(commit_details.get('total_bytes_at_version', 'N/A')), "total-records": str(commit_details.get('total_records_at_version', 'N/A')), "total-delete-files": "0" }
                snapshot_entry = { "snapshot-id": v, "timestamp-ms": commit_details.get('timestamp'), "summary": summary, "schema_definition": commit_details.get('schema_definition'), "error": commit_details.get('error') }
                snapshots_overview_with_schema.append(snapshot_entry)
                if v == current_snapshot_id: current_snapshot_summary_for_history = snapshot_entry

            # Final Result Structure
            result = {
                "table_type": "Delta", "table_uuid": definitive_metadata.get("id") if definitive_metadata else None,
                "location": storage_url, "format_configuration": format_configuration,
                "format_version": definitive_protocol.get('minReaderVersion', 1),
                "delta_log_files": delta_log_files_info, "current_snapshot_id": current_snapshot_id,
                "current_snapshot_details": current_snapshot_details, "table_schema": final_table_schema,
                "table_properties": definitive_metadata.get("configuration", {}) if definitive_metadata else {},
                "partition_spec": partition_spec, "sort_order": {"order-id": 0, "fields": []},
                "version_history": {
                    "total_snapshots": len(known_versions_sorted_hist),
                    "current_snapshot_summary": current_snapshot_summary_for_history,
                    "snapshots_overview": snapshots_overview_with_schema },
                "key_metrics": {
                    "total_data_files": total_data_files, "total_delete_files": total_delete_files,
                    "total_data_storage_bytes": total_data_storage_bytes, "total_data_storage_human": format_bytes(total_data_storage_bytes),
                    "total_delete_storage_bytes": total_delete_storage_bytes, "total_delete_storage_human": format_bytes(total_delete_storage_bytes),
                    "gross_records_in_data_files": gross_records_in_data_files, "approx_deleted_records_in_manifests": approx_deleted_records_in_manifests,
                    "approx_live_records": approx_live_records, "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                    "avg_data_file_size_mb": round(avg_data_file_size_mb, 4), "metrics_note": metrics_note, },
                "partition_explorer": partition_explorer_data, "sample_data": sample_data,
            }

            result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))

            end_time = time.time()
            print(f"--- Delta Request Completed in {end_time - start_time:.2f} seconds ---")
            return jsonify(result_serializable), 200

    # --- Exception Handling (Updated for GCS) ---
    except NoCredentialsError as e:
        return jsonify({"error": f"S3/MinIO credentials not found or invalid: {e}"}), 401
    except gcs_auth_exceptions.DefaultCredentialsError as e:
        return jsonify({"error": f"GCS credentials not found or invalid: {e}"}), 401
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"ERROR: S3 ClientError in Delta endpoint: {error_code} - {e}")
        traceback.print_exc()
        if error_code == 'NoSuchBucket': return jsonify({"error": f"S3 bucket not found: {bucket_name}"}), 404
        elif error_code == 'AccessDenied': return jsonify({"error": f"S3 access denied for s3://{bucket_name}/{table_base_key}"}), 403
        else: return jsonify({"error": f"S3 ClientError ({error_code}): {e}"}), 500
    except gcs_exceptions.NotFound as e:
         print(f"ERROR: GCS resource not found: {e}")
         if "bucket" in str(e).lower(): return jsonify({"error": f"GCS bucket not found: {bucket_name}"}), 404
         # Check if it's the delta log specifically
         elif delta_log_prefix and delta_log_prefix in str(e): return jsonify({"error": f"Delta log not found: {storage_type}://{bucket_name}/{delta_log_prefix}"}), 404
         else: return jsonify({"error": f"Required GCS object not found: {e}"}), 404
    except gcs_exceptions.Forbidden as e:
         print(f"ERROR: GCS access denied: {e}")
         return jsonify({"error": f"GCS access denied for gs://{bucket_name}/{table_base_key}"}), 403
    except FileNotFoundError as e:
        print(f"ERROR: FileNotFoundError in Delta endpoint: {e}")
        if "_delta_log" in str(e) or (delta_log_prefix and delta_log_prefix in str(e)):
            return jsonify({"error": f"Delta log not found or inaccessible for table: {storage_url}"}), 404
        return jsonify({"error": f"Required file or resource not found: {e}"}), 404
    except ValueError as e:
        print(f"ERROR: ValueError in Delta endpoint: {e}")
        return jsonify({"error": f"Input value or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred in Delta endpoint: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


# --- Schema Comparison Endpoints (Updated) ---

@app.route('/compare_schema/Iceberg', methods=['GET'])
def compare_iceberg_schema_endpoint():
    storage_url = request.args.get('s3_url') # Keep param name
    seq1_str = request.args.get('seq1')
    seq2_str = request.args.get('seq2')

    if not storage_url: return jsonify({"error": "s3_url parameter is missing"}), 400
    if not seq1_str: return jsonify({"error": "seq1 parameter is missing"}), 400
    if not seq2_str: return jsonify({"error": "seq2 parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Iceberg Schema Comparison Request for {storage_url} (seq {seq1_str} vs seq {seq2_str}) ---")

    storage_type = None
    bucket_name = None
    table_base_key = None
    client = None
    temp_dir_obj = None
    endpoint_url = None
    use_ssl = True

    try:
        seq1 = int(seq1_str)
        seq2 = int(seq2_str)
    except ValueError:
        return jsonify({"error": "seq1 and seq2 must be integers."}), 400

    try:
        # Parse URL and create client
        storage_type, bucket_name, table_base_key, endpoint_url, use_ssl = parse_storage_url(storage_url)
        client = create_storage_client(storage_type, endpoint_url, use_ssl, AWS_REGION)

        temp_dir_obj = tempfile.TemporaryDirectory(prefix="iceberg_schema_comp_")
        temp_dir = temp_dir_obj.name
        print(f"DEBUG: Using temporary directory: {temp_dir}")

        # --- Get Schemas (use abstracted function) ---
        version1_label = f"sequence_number {seq1}"
        version2_label = f"sequence_number {seq2}"
        schema1 = get_iceberg_schema_for_version(client, storage_type, bucket_name, table_base_key, seq1, temp_dir)
        schema2 = get_iceberg_schema_for_version(client, storage_type, bucket_name, table_base_key, seq2, temp_dir)

        # --- Compare Schemas (logic same) ---
        schema_diff = compare_schemas(schema1, schema2, version1_label, version2_label)

        # --- Assemble Result (logic same) ---
        result = {
            "table_type": "Iceberg", "location": storage_url,
            "version1": {"identifier_type": "sequence_number", "identifier": seq1, "schema": schema1 },
            "version2": {"identifier_type": "sequence_number", "identifier": seq2, "schema": schema2 },
            "schema_comparison": schema_diff
        }
        result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))

        end_time = time.time()
        print(f"--- Iceberg Schema Comparison Request Completed in {end_time - start_time:.2f} seconds ---")
        return jsonify(result_serializable), 200

    # --- Exception Handling (Updated for GCS) ---
    except NoCredentialsError as e: return jsonify({"error": f"S3/MinIO credentials error: {e}"}), 401
    except gcs_auth_exceptions.DefaultCredentialsError as e: return jsonify({"error": f"GCS credentials error: {e}"}), 401
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"ERROR: S3 ClientError: {error_code} - {e}")
        traceback.print_exc()
        if error_code == 'NoSuchBucket': return jsonify({"error": f"S3 bucket not found: {bucket_name}"}), 404
        elif error_code == 'AccessDenied': return jsonify({"error": f"S3 access denied for s3://{bucket_name}/{table_base_key}"}), 403
        else: return jsonify({"error": f"S3 ClientError ({error_code}): {e}"}), 500
    except gcs_exceptions.NotFound as e:
         print(f"ERROR: GCS resource not found: {e}")
         if "bucket" in str(e).lower(): return jsonify({"error": f"GCS bucket not found: {bucket_name}"}), 404
         else: return jsonify({"error": f"Required GCS object not found: {e}"}), 404
    except gcs_exceptions.Forbidden as e:
         print(f"ERROR: GCS access denied: {e}")
         return jsonify({"error": f"GCS access denied for gs://{bucket_name}/{table_base_key}"}), 403
    except FileNotFoundError as e:
        print(f"ERROR: FileNotFoundError: {e}")
        return jsonify({"error": f"Required file or directory not found: {e}"}), 404
    except ValueError as e:
        print(f"ERROR: ValueError: {e}")
        return jsonify({"error": f"Input or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    finally:
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                print(f"DEBUG: Cleaned up temporary directory: {temp_dir_obj.name}")
            except Exception as cleanup_err:
                print(f"ERROR: Failed to cleanup temporary directory {temp_dir_obj.name}: {cleanup_err}")


@app.route('/compare_schema/Delta', methods=['GET'])
def compare_delta_schema_endpoint():
    storage_url = request.args.get('s3_url') # Keep param name
    v1_str = request.args.get('v1')
    v2_str = request.args.get('v2')

    if not storage_url: return jsonify({"error": "s3_url parameter is missing"}), 400
    if not v1_str: return jsonify({"error": "v1 parameter is missing"}), 400
    if not v2_str: return jsonify({"error": "v2 parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Delta Schema Comparison Request for {storage_url} (v{v1_str} vs v{v2_str}) ---")

    storage_type = None
    bucket_name = None
    table_base_key = None
    client = None
    temp_dir_obj = None
    endpoint_url = None
    use_ssl = True

    try:
        v1 = int(v1_str)
        v2 = int(v2_str)
    except ValueError:
        return jsonify({"error": "v1 and v2 must be integers."}), 400

    try:
        # Parse URL and create client
        storage_type, bucket_name, table_base_key, endpoint_url, use_ssl = parse_storage_url(storage_url)
        client = create_storage_client(storage_type, endpoint_url, use_ssl, AWS_REGION)

        temp_dir_obj = tempfile.TemporaryDirectory(prefix="delta_schema_comp_")
        temp_dir = temp_dir_obj.name
        print(f"DEBUG: Using temporary directory: {temp_dir}")

        # --- Get Schemas (use abstracted function) ---
        version1_label = f"version_id {v1}"
        version2_label = f"version_id {v2}"
        schema1 = get_delta_schema_for_version(client, storage_type, bucket_name, table_base_key, v1, temp_dir)
        schema2 = get_delta_schema_for_version(client, storage_type, bucket_name, table_base_key, v2, temp_dir)

        # --- Compare Schemas (logic same) ---
        schema_diff = compare_schemas(schema1, schema2, version1_label, version2_label)

        # --- Assemble Result (logic same) ---
        result = {
            "table_type": "Delta", "location": storage_url,
            "version1": {"identifier_type": "version_id", "identifier": v1, "schema": schema1 },
            "version2": {"identifier_type": "version_id", "identifier": v2, "schema": schema2 },
            "schema_comparison": schema_diff
        }
        result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))

        end_time = time.time()
        print(f"--- Delta Schema Comparison Request Completed in {end_time - start_time:.2f} seconds ---")
        return jsonify(result_serializable), 200

    # --- Exception Handling (Updated for GCS) ---
    except NoCredentialsError as e: return jsonify({"error": f"S3/MinIO credentials error: {e}"}), 401
    except gcs_auth_exceptions.DefaultCredentialsError as e: return jsonify({"error": f"GCS credentials error: {e}"}), 401
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"ERROR: S3 ClientError: {error_code} - {e}")
        traceback.print_exc()
        if error_code == 'NoSuchBucket': return jsonify({"error": f"S3 bucket not found: {bucket_name}"}), 404
        elif error_code == 'AccessDenied': return jsonify({"error": f"S3 access denied for s3://{bucket_name}/{table_base_key}"}), 403
        else: return jsonify({"error": f"S3 ClientError ({error_code}): {e}"}), 500
    except gcs_exceptions.NotFound as e:
         print(f"ERROR: GCS resource not found: {e}")
         if "bucket" in str(e).lower(): return jsonify({"error": f"GCS bucket not found: {bucket_name}"}), 404
         else: return jsonify({"error": f"Required GCS object not found: {e}"}), 404
    except gcs_exceptions.Forbidden as e:
         print(f"ERROR: GCS access denied: {e}")
         return jsonify({"error": f"GCS access denied for gs://{bucket_name}/{table_base_key}"}), 403
    except FileNotFoundError as e:
        print(f"ERROR: FileNotFoundError: {e}")
        if "_delta_log" in str(e): return jsonify({"error": f"Delta log not found for table: {storage_url}"}), 404
        return jsonify({"error": f"Required file or directory not found: {e}"}), 404
    except ValueError as e:
        print(f"ERROR: ValueError: {e}")
        return jsonify({"error": f"Input or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    finally:
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                print(f"DEBUG: Cleaned up temporary directory: {temp_dir_obj.name}")
            except Exception as cleanup_err:
                print(f"ERROR: Failed to cleanup temporary directory {temp_dir_obj.name}: {cleanup_err}")


# --- List Tables Endpoint (Updated) ---
@app.route('/list_tables', methods=['GET'])
def list_tables():
    s3_root_path_param = request.args.get('s3_root_path') # Keep param name
    if not s3_root_path_param:
        return jsonify({"error": "s3_root_path parameter is missing"}), 400

    print(f"\n--- Processing List Tables Request for {s3_root_path_param} ---")

    storage_type = None
    bucket_name = None
    root_prefix = None
    client = None
    endpoint_url = None
    use_ssl = True

    try:
        # Parse URL and create client
        storage_type, bucket_name, root_prefix, endpoint_url, use_ssl = parse_storage_url(s3_root_path_param)
        client = create_storage_client(storage_type, endpoint_url, use_ssl, AWS_REGION)

        discovered_tables = []
        print(f"DEBUG: Listing common prefixes under {storage_type}://{bucket_name}/{root_prefix}")

        # Use abstracted listing with delimiter
        try:
            _, common_prefixes = list_storage_items(client, storage_type, bucket_name, root_prefix, delimiter='/')
        except FileNotFoundError: # Bucket not found
            return jsonify({"error": f"{storage_type.upper()} bucket not found: {bucket_name}"}), 404
        except PermissionError:
            return jsonify({"error": f"Permission denied listing path: {s3_root_path_param}"}), 403
        # Catch other potential errors during listing

        if not common_prefixes:
            print(f"INFO: No common prefixes (subdirectories) found under {s3_root_path_param}")
            # Optionally: check if the root path itself is a table
            # For now, just return empty list if no subdirs found

        for prefix_key in common_prefixes:
            if not prefix_key: continue

            # Construct table path in standard format (s3:// or gs://)
            table_path_s3_format = f"s3://{bucket_name}/{prefix_key}" if storage_type == 's3' else f"gs://{bucket_name}/{prefix_key}"
            # Re-parse to get consistent type/bucket/key for checks
            tbl_storage_type, tbl_bucket_name, tbl_prefix, _, _ = parse_storage_url(table_path_s3_format)

            detected_type = "Unknown"
            print(f"DEBUG: Checking prefix {prefix_key} for table type...")

            # Check for Delta (_delta_log) using abstracted list
            try:
                delta_items, _ = list_storage_items(client, tbl_storage_type, tbl_bucket_name, f"{tbl_prefix}_delta_log/", max_keys=1)
                if delta_items: # Check if any items were returned
                    detected_type = "Delta"
                    print(f"DEBUG: Found Delta log for {prefix_key}")
                    discovered_tables.append({"path": table_path_s3_format, "type": detected_type})
                    continue
            except (PermissionError, Exception) as e: # Ignore permission denied on sub-checks
                if not isinstance(e, PermissionError):
                     print(f"Warning: Error checking Delta log for {prefix_key}: {e}")

            # Check for Iceberg (metadata/*.metadata.json) using abstracted list
            try:
                iceberg_items, _ = list_storage_items(client, tbl_storage_type, tbl_bucket_name, f"{tbl_prefix}metadata/", max_keys=10)
                if any(item.get('Key', '').endswith('.metadata.json') for item in iceberg_items):
                    detected_type = "Iceberg"
                    print(f"DEBUG: Found Iceberg metadata for {prefix_key}")
                    discovered_tables.append({"path": table_path_s3_format, "type": detected_type})
                    continue
            except (PermissionError, Exception) as e:
                 if not isinstance(e, PermissionError):
                    print(f"Warning: Error checking Iceberg metadata for {prefix_key}: {e}")

            # Check for Hudi (.hoodie) using abstracted list
            try:
                hudi_items, _ = list_storage_items(client, tbl_storage_type, tbl_bucket_name, f"{tbl_prefix}.hoodie/", max_keys=1)
                if hudi_items:
                    detected_type = "Hudi"
                    print(f"DEBUG: Found Hudi metadata for {prefix_key}")
                    discovered_tables.append({"path": table_path_s3_format, "type": detected_type})
                    continue
            except (PermissionError, Exception) as e:
                 if not isinstance(e, PermissionError):
                     print(f"Warning: Error checking Hudi metadata for {prefix_key}: {e}")

            # Optionally include unknown types
            # if detected_type == "Unknown":
            #     print(f"DEBUG: No specific table type found for {prefix_key}, listing as Unknown.")
            #     discovered_tables.append({"path": table_path_s3_format, "type": detected_type})

        print(f"INFO: Found {len(discovered_tables)} potential tables.")
        return jsonify(discovered_tables)

    # --- Exception Handling (Updated for GCS) ---
    except NoCredentialsError as e: return jsonify({"error": f"S3/MinIO credentials error: {e}"}), 401
    except gcs_auth_exceptions.DefaultCredentialsError as e: return jsonify({"error": f"GCS credentials error: {e}"}), 401
    except ClientError as e: # S3 specific error during initial list/parse
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"ERROR: S3 ClientError listing tables: {error_code} - {e}")
        traceback.print_exc()
        if error_code == 'NoSuchBucket': return jsonify({"error": f"S3 bucket not found: {bucket_name}"}), 404
        elif error_code == 'AccessDenied': return jsonify({"error": f"S3 Access denied listing path: {s3_root_path_param}"}), 403
        else: return jsonify({"error": f"S3 ClientError ({error_code}): {e}"}), 500
    except gcs_exceptions.NotFound as e: # GCS Bucket not found during initial list/parse
         print(f"ERROR: GCS Bucket not found: {e}")
         return jsonify({"error": f"GCS bucket not found: {bucket_name}"}), 404
    except gcs_exceptions.Forbidden as e: # GCS Permission error during initial list/parse
         print(f"ERROR: GCS access denied listing path: {e}")
         return jsonify({"error": f"GCS Access denied listing path: {s3_root_path_param}"}), 403
    except ValueError as e: # Catch invalid URL format from parser
        print(f"ERROR: ValueError listing tables: {e}")
        return jsonify({"error": f"Invalid input path: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred while listing tables: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected server error occurred: {str(e)}"}), 500


# --- Generate Summary Endpoint (No storage interaction, no changes) ---
@app.route("/generate-summary", methods=["POST"])
def generate():
    data = request.get_json()
    comparison_details_str = data.get("comparison", "")
    v1_label = data.get("v1_label", "Older Version")
    v2_label = data.get("v2_label", "Newer Version")

    if not comparison_details_str:
        return jsonify({"error": "Missing 'comparison' field in request body."}), 400

    prompt = (
     f"Summarize the key statistical changes between two table snapshots: '{v1_label}' and '{v2_label}', based on the details below.\n\n"
     "Focus on significant increases or decreases in records, files, size, and deletes mentioned in the details. "
     "Keep the summary concise (1 sentence, maximum 2). "
     "The final output must be *only* the HTML paragraph itself, starting with `<p>` and ending with `</p>`. "
     "Use `<strong></strong>` tags for the keywords.\n\n"
     "Change Details:\n"
     f"{comparison_details_str}"
     )

    try:
        summary = get_gemini_response(prompt)
        summary = summary.strip()
        # Basic validation/cleanup
        if not summary.startswith("<p>") or not summary.endswith("</p>"):
            if '<' not in summary and '>' not in summary:
                summary = f"<p>{summary}</p>"
            else:
                 print(f"Warning: Gemini response for summary wasn't a clean paragraph: {summary}")
                 match = re.search(r"<p>.*?</p>", summary, re.IGNORECASE | re.DOTALL)
                 if match: summary = match.group(0)
                 else: summary = f"<p>Summary generation issue: {summary}</p>" # Fallback

        return jsonify({"summary": summary})
    except Exception as e:
        print(f"Error generating summary: {e}")
        traceback.print_exc()
        return jsonify({"error": f"Failed to generate summary: {str(e)}"}), 500

# --- Root Endpoint (Updated Help Text) ---
@app.route('/', methods=['GET'])
def hello():
    return """
    Hello! Available endpoints:
      - /Iceberg?s3_url=&lt;s3|gs|http(s)&gt;://&lt;bucket_or_host&gt;/&lt;path/to/table/&gt;
      - /Delta?s3_url=&lt;s3|gs|http(s)&gt;://&lt;bucket_or_host&gt;/&lt;path/to/table/&gt;
      - /compare_schema/Iceberg?s3_url=...&seq1=...&seq2=...
      - /compare_schema/Delta?s3_url=...&v1=...&v2=...
      - /list_tables?s3_root_path=&lt;s3|gs|http(s)&gt;://&lt;bucket_or_host&gt;/&lt;path/prefix/&gt;
      - /generate-summary (POST with comparison data)
    """

if __name__ == '__main__':
    # Ensure GCS credentials environment variable is mentioned if needed
    if not GCS_SERVICE_ACCOUNT_KEY_PATH:
         print("INFO: GOOGLE_APPLICATION_CREDENTIALS environment variable not set. GCS access will rely on Application Default Credentials (ADC).")
    app.run(debug=True, host='0.0.0.0', port=5000)