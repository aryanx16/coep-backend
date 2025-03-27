# from flask import Flask, request, jsonify
# import boto3
# import os
# import json
# import tempfile
# import traceback
# import datetime
# from dotenv import load_dotenv
# from fastavro import reader as avro_reader
# import pyarrow.parquet as pq
# from urllib.parse import urlparse

# # Load environment variables from .env file (optional, good practice)
# load_dotenv()

# app = Flask(__name__)

# # --- Configuration ---
# # It's highly recommended to use IAM roles or instance profiles in production
# # instead of hardcoding keys. These are loaded from environment variables
# # or a .env file for demonstration.
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_REGION = os.getenv("AWS_REGION", "us-east-1") # Default region if not set

# # --- Helper Functions ---

# def extract_bucket_and_key(s3_url):
#     """Extract bucket name and key/prefix from an S3 URL."""
#     parsed_url = urlparse(s3_url)
#     if parsed_url.scheme != "s3":
#         raise ValueError("Invalid S3 URL format, must start with s3://")
#     bucket = parsed_url.netloc
#     key = parsed_url.path.lstrip('/')
#     if not bucket or not key:
#         raise ValueError("S3 URL must include both bucket and key/prefix")
#     return bucket, key

# def download_s3_file(s3_client, bucket, key, local_path):
#     """Download a file from S3 to a local path."""
#     try:
#         os.makedirs(os.path.dirname(local_path), exist_ok=True)
#         print(f"Downloading s3://{bucket}/{key} to {local_path}")
#         s3_client.download_file(bucket, key, local_path)
#         print(f"Successfully downloaded s3://{bucket}/{key}")
#     except Exception as e:
#         print(f"Error downloading s3://{bucket}/{key}: {e}")
#         raise

# def parse_avro_file(file_path):
#     """Read an Avro file and return its records as a list of dictionaries."""
#     records = []
#     try:
#         with open(file_path, 'rb') as fo:
#             avro_file_reader = avro_reader(fo)
#             for record in avro_file_reader:
#                 records.append(record)
#         print(f"Successfully parsed Avro file: {file_path}, Records: {len(records)}")
#         return records
#     except Exception as e:
#         print(f"Error parsing Avro file {file_path}: {e}")
#         raise

# def read_parquet_sample(file_path, num_rows=5):
#     """Read a sample number of rows from a Parquet file."""
#     try:
#         table = pq.read_table(file_path)
#         # Limit the number of rows, take the first 'num_rows'
#         sample_table = table.slice(length=min(num_rows, len(table)))
#         # Convert to list of dictionaries for JSON serialization
#         sample_data = sample_table.to_pylist()
#         print(f"Successfully read {len(sample_data)} sample rows from Parquet: {file_path}")
#         return sample_data
#     except Exception as e:
#         print(f"Error reading Parquet file {file_path}: {e}")
#         # Return empty list or re-raise depending on desired behavior
#         return [] # Return empty list on error to avoid breaking the entire response

# def convert_bytes(obj):
#     """
#     Recursively convert bytes in the given object to strings for JSON serialization.
#     Handles potential decoding errors.
#     """
#     if isinstance(obj, bytes):
#         try:
#             return obj.decode('utf-8')
#         except UnicodeDecodeError:
#             # Handle cases where bytes are not valid UTF-8 (e.g., serialized Java objects)
#              # Return a placeholder or representation
#             return f"<bytes len={len(obj)}>"
#         except Exception as e:
#              # Log other unexpected errors during decoding
#             print(f"Unexpected error decoding bytes: {e}")
#             return f"<bytes len={len(obj)} error: {e}>"
#     elif isinstance(obj, dict):
#         return {convert_bytes(k): convert_bytes(v) for k, v in obj.items()}
#     elif isinstance(obj, list):
#         return [convert_bytes(item) for item in obj]
#     # Handle pyarrow Timestamp objects which are not directly JSON serializable
#     elif hasattr(obj, 'isoformat'): # Check if it behaves like a datetime object
#          try:
#             return obj.isoformat()
#          except Exception:
#              return str(obj) # Fallback to string representation
#     else:
#         return obj

# # --- Main Endpoint ---

# @app.route('/iceberg-details', methods=['GET'])
# def iceberg_details():
#     """
#     Endpoint to extract metadata and details from an Iceberg table location on S3.
#     """
#     s3_url = request.args.get('s3_url')
#     if not s3_url:
#         return jsonify({"error": "s3_url parameter is missing"}), 400

#     try:
#         bucket_name, table_base_key = extract_bucket_and_key(s3_url)
#     except ValueError as e:
#         return jsonify({"error": str(e)}), 400

#     # Ensure table_base_key ends with a '/' if it points to a directory-like structure
#     if not table_base_key.endswith('/'):
#         table_base_key += '/'

#     metadata_prefix = table_base_key + "metadata/"
#     data_prefix = table_base_key + "data/" # Needed for constructing data file paths

#     print(f"Processing Iceberg table at: s3://{bucket_name}/{table_base_key}")
#     print(f"Metadata prefix: {metadata_prefix}")

#     try:
#         s3_client = boto3.client(
#             's3',
#             aws_access_key_id=AWS_ACCESS_KEY_ID,
#             aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#             region_name=AWS_REGION
#         )

#         # Use a temporary directory that cleans itself up
#         with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
#             print(f"Using temporary directory: {temp_dir}")

#             # 1. Find the latest metadata.json file
#             list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
#             if 'Contents' not in list_response:
#                 return jsonify({"error": "No metadata files found under the specified prefix."}), 404

#             metadata_files = sorted([
#                 obj['Key'] for obj in list_response['Contents']
#                 if obj['Key'].endswith('.metadata.json')
#             ], reverse=True) # Sort descending, highest number (latest) first

#             if not metadata_files:
#                 return jsonify({"error": f"No *.metadata.json files found under {metadata_prefix}"}), 404

#             latest_metadata_key = metadata_files[0]
#             latest_metadata_filename = os.path.basename(latest_metadata_key)
#             local_latest_metadata_path = os.path.join(temp_dir, latest_metadata_filename)

#             # 2. Download and parse the latest metadata.json
#             download_s3_file(s3_client, bucket_name, latest_metadata_key, local_latest_metadata_path)
#             with open(local_latest_metadata_path, 'r') as f:
#                 latest_meta = json.load(f)

#             # Basic table info
#             table_uuid = latest_meta.get("table-uuid")
#             current_snapshot_id = latest_meta.get("current-snapshot-id")
#             snapshots = latest_meta.get("snapshots", [])
#             schema = latest_meta.get("schemas", [latest_meta.get("schema")])[0] # Handle older/newer formats
#             properties = latest_meta.get("properties", {})
#             partition_spec = latest_meta.get("partition-specs", [latest_meta.get("partition-spec")])[0] # Handle older/newer formats
#             sort_order = latest_meta.get("sort-orders", [latest_meta.get("sort-order")])[0]

#             if not current_snapshot_id:
#                  # Handle case where table might be empty or metadata incomplete
#                 return jsonify({
#                     "message": "Table exists but has no current snapshot (might be empty or in creation).",
#                     "table_uuid": table_uuid,
#                     "location": s3_url,
#                     "schema": convert_bytes(schema),
#                     "properties": convert_bytes(properties),
#                     "partition_spec": convert_bytes(partition_spec),
#                     "sort_order": convert_bytes(sort_order),
#                     "snapshots": convert_bytes(snapshots)
#                 }), 200 # Or maybe 404/204? 200 is okay if partial info is useful


#             # 3. Find the current snapshot and its manifest list
#             current_snapshot = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
#             if not current_snapshot:
#                 return jsonify({"error": f"Current snapshot ID {current_snapshot_id} not found in metadata."}), 404

#             manifest_list_path = current_snapshot.get("manifest-list")
#             if not manifest_list_path or not manifest_list_path.startswith("s3://"):
#                  # Manifest list path might be relative in some writers, construct full path
#                 manifest_list_key = urlparse(manifest_list_path).path.lstrip('/')
#                 # return jsonify({"error": f"Invalid or missing manifest list path in current snapshot: {manifest_list_path}"}), 500
#             else:
#                  # Extract key from the absolute S3 path
#                  manifest_list_bucket, manifest_list_key = extract_bucket_and_key(manifest_list_path)
#                  if manifest_list_bucket != bucket_name:
#                      return jsonify({"error": f"Manifest list bucket '{manifest_list_bucket}' does not match table bucket '{bucket_name}'"}), 400


#             manifest_list_filename = os.path.basename(manifest_list_key)
#             local_manifest_list_path = os.path.join(temp_dir, manifest_list_filename)

#             # 4. Download and parse the manifest list Avro file
#             download_s3_file(s3_client, bucket_name, manifest_list_key, local_manifest_list_path)
#             manifest_list_entries = parse_avro_file(local_manifest_list_path)

#             # --- Aggregation Variables ---
#             total_data_files = 0
#             total_records = 0
#             total_storage_bytes = 0
#             partition_stats = {} # { "partition_tuple_str": {"count": N, "size": S, "files": F}}
#             data_file_paths_sample = [] # Store some paths for potential data sampling

#             # 5. Process each manifest file listed in the manifest list
#             for entry in manifest_list_entries:
#                 manifest_file_path = entry.get("manifest_path")
#                 if not manifest_file_path:
#                     print("Skipping manifest entry with no path")
#                     continue

#                 # Construct full S3 key for the manifest file (might be relative)
#                 if not manifest_file_path.startswith("s3://"):
#                      manifest_file_key = urlparse(manifest_file_path).path.lstrip('/')
#                 else:
#                     manifest_file_path_bucket, manifest_file_key = extract_bucket_and_key(manifest_file_path)
#                     if manifest_file_path_bucket != bucket_name:
#                          print(f"Warning: Manifest file bucket '{manifest_file_path_bucket}' differs from table bucket '{bucket_name}'. Skipping.")
#                          continue


#                 manifest_filename = os.path.basename(manifest_file_key)
#                 local_manifest_path = os.path.join(temp_dir, manifest_filename)

#                 try:
#                     # Download and parse the manifest Avro file
#                     download_s3_file(s3_client, bucket_name, manifest_file_key, local_manifest_path)
#                     manifest_records = parse_avro_file(local_manifest_path)
                    
#                     # print("MANIFEST")
#                     # print(manifest_records)
#                     # print("MANIFEST")
#                     # 6. Aggregate metrics from data file entries within the manifest
#                     for data_file_record in manifest_records:
#                         # V1 vs V2 manifest entry structure
#                         df_info = data_file_record.get('data_file')
#                         if df_info is None:
#                            # Potentially older format or different record type, skip for now
#                            # print(f"Skipping record in manifest {manifest_filename}, missing 'data_file': {data_file_record}")
#                            continue

#                         status = data_file_record.get('status', 0) # 0: EXISTING, 1: ADDED, 2: DELETED
#                         if status == 2: # Skip deleted files for current stats
#                             continue

#                         record_count = df_info.get("record_count", 0)
#                         file_size = df_info.get("file_size_in_bytes", 0)
#                         file_path = df_info.get("file_path", "") # Path relative to data_prefix or absolute s3:// path

#                         total_data_files += 1
#                         total_records += record_count
#                         total_storage_bytes += file_size

#                         # Store a few data file paths for sampling later
#                         if len(data_file_paths_sample) < 5 and file_path.endswith(".parquet"): # Prioritize parquet
#                             if not file_path.startswith("s3://"):
#                                 # Assume relative to table data path
#                                 data_file_s3_key = data_prefix + file_path.lstrip('/')
#                                 data_file_paths_sample.append(data_file_s3_key)
#                             else:
#                                 # Already an absolute path
#                                 data_file_paths_sample.append(file_path)


#                         # Aggregate partition stats
#                         partition_data = df_info.get("partition") # This is usually a struct/dict
#                         # Create a stable string representation for the partition tuple
#                         partition_key_parts = {}
#                         if isinstance(partition_data, dict):
#                             # Sort items by key to ensure consistent key string regardless of original order
#                             for k, v in sorted(partition_data.items()):
#                                 if isinstance(v, (datetime.date, datetime.datetime)):
#                                     partition_key_parts[k] = v.isoformat()
#                                 else:
#                                     # Assume other types (int, str, bool, float) are JSON serializable for the key
#                                     partition_key_parts[k] = v
#                         else:
#                             # Handle cases where partition_data isn't a dict (unlikely for structured partitions)
#                             partition_key_parts = {'_value': str(partition_data)} # Use a placeholder key

#                         # Now use the serializable parts to create the dictionary key string
#                         partition_key_str = json.dumps(partition_key_parts)
#                         # --- END FIX ---

#                         if partition_key_str not in partition_stats:
#                             partition_stats[partition_key_str] = {
#                                 "record_count": 0,
#                                 "size_bytes": 0,
#                                 "num_files": 0,
#                                 # Store the original partition_data here; convert_bytes will handle it later for output
#                                 "repr": partition_data
#                             }
#                         partition_stats[partition_key_str]["record_count"] += record_count
#                         partition_stats[partition_key_str]["size_bytes"] += file_size
#                         partition_stats[partition_key_str]["num_files"] += 1

#                 except Exception as manifest_err:
#                     # Print the specific error when processing a manifest
#                     print(f"Failed to process manifest {manifest_file_key}: {manifest_err}")
#                     # Optionally print stack trace for debugging:
#                     # traceback.print_exc()
#                     # Continue processing other manifests if one fails
#                     # Continue processing other manifests if one fails

#             # 7. Get Sample Data from a Parquet File
#             sample_data = []
#             if data_file_paths_sample:
#                 # Try reading from the first sampled Parquet file path
#                 sample_file_s3_path = data_file_paths_sample[0]
#                 sample_file_bucket, sample_file_key = extract_bucket_and_key(sample_file_s3_path)
#                 if sample_file_bucket != bucket_name:
#                      print(f"Warning: Sample data file bucket '{sample_file_bucket}' differs from table bucket '{bucket_name}'. Skipping sample.")
#                 else:
#                     sample_filename = os.path.basename(sample_file_key)
#                     local_sample_parquet_path = os.path.join(temp_dir, "sample_" + sample_filename)
#                     try:
#                         download_s3_file(s3_client, bucket_name, sample_file_key, local_sample_parquet_path)
#                         sample_data = read_parquet_sample(local_sample_parquet_path, num_rows=10) # Read up to 10 rows
#                     except Exception as sample_err:
#                         print(f"Could not read sample data from {sample_file_s3_path}: {sample_err}")
#                         sample_data = [{"error": f"Failed to read sample data from {sample_file_s3_path}", "details": str(sample_err)}]


#             # 8. Assemble the final result
#             avg_records_per_file = (total_records / total_data_files) if total_data_files > 0 else 0
#             avg_file_size_mb = (total_storage_bytes / total_data_files / (1024*1024)) if total_data_files > 0 else 0

#             partition_explorer_data = [
#                 {
#                     "partition_values": v["repr"],
#                     "partition_key_string": k,
#                     "record_count": v["record_count"],
#                     "size_bytes": v["size_bytes"],
#                     "num_files": v["num_files"]
#                  }
#                 for k, v in partition_stats.items()
#             ]

#             result = {
#                 "table_uuid": table_uuid,
#                 "location": s3_url,
#                 "current_snapshot_id": current_snapshot_id,
#                 "table_schema": schema,
#                 "table_properties": properties,
#                 "partition_spec": partition_spec,
#                 "sort_order": sort_order,
#                 "version_history": { # Provide summary, full list can be large
#                     "total_snapshots": len(snapshots),
#                     "current_snapshot_summary": current_snapshot,
#                      # Optionally include first few/last few snapshots
#                     "snapshots_overview": snapshots[-5:] # Example: last 5 snapshots
#                 },
#                 "key_metrics": {
#                     "total_data_files": total_data_files,
#                     "total_records": total_records,
#                     "total_storage_bytes": total_storage_bytes,
#                     "avg_records_per_file": avg_records_per_file,
#                     "avg_file_size_mb": avg_file_size_mb,
#                     # You could add file size distribution histograms here if needed
#                 },
#                 "partition_explorer": partition_explorer_data,
#                 "sample_data": sample_data, # Data read from Parquet
#                  # Maybe list manifest files?
#                  # "manifest_list_details": manifest_list_entries
#             }

#             # Convert bytes and complex objects before returning JSON
#             result_serializable = convert_bytes(result)

#             return jsonify(result_serializable), 200

#     except boto3.exceptions.NoCredentialsError:
#         return jsonify({"error": "AWS credentials not found. Configure credentials (environment variables, ~/.aws/credentials, or IAM role)."}), 401
#     except s3_client.exceptions.NoSuchBucket:
#          return jsonify({"error": f"S3 bucket '{bucket_name}' not found or access denied."}), 404
#     except s3_client.exceptions.ClientError as e:
#         if e.response['Error']['Code'] == 'AccessDenied':
#              return jsonify({"error": f"Access Denied when trying to access S3 path: s3://{bucket_name}/{table_base_key}. Check permissions."}), 403
#         else:
#              print(f"An AWS ClientError occurred: {e}")
#              print(traceback.format_exc())
#              return jsonify({"error": f"AWS ClientError: {e}"}), 500
#     except FileNotFoundError as e:
#          # This might happen if a download fails before the file is accessed
#          print(f"FileNotFoundError occurred: {e}")
#          print(traceback.format_exc())
#          return jsonify({"error": f"A required file was not found or downloaded: {e}"}), 500
#     except Exception as e:
#         # General error catch-all
#         print(f"An unexpected error occurred: {e}")
#         print(traceback.format_exc()) # Print detailed stack trace to server logs
#         return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500


# @app.route('/', methods=['GET'])
# def hello():
#     return "Hello! Use /iceberg-details?s3_url=<s3://your-bucket/path/to/iceberg-table/> to fetch detailed table metadata."

# if __name__ == '__main__':
#     # Set debug=False for production
#     # Use host='0.0.0.0' to make it accessible externally (e.g., within Docker)
#     app.run(debug=True, host='0.0.0.0', port=5000)

# !pip install Flask boto3 python-dotenv fastavro pyarrow # Ensure these are installed# !pip install Flask boto3 python-dotenv fastavro pyarrow # Ensure these are installed

# !pip install Flask boto3 python-dotenv fastavro pyarrow # Ensure these are installed

# !pip install Flask boto3 python-dotenv fastavro pyarrow # Ensure these are installed

# !pip install Flask boto3 python-dotenv fastavro pyarrow # Ensure these are installed

from flask import Flask, request, jsonify
import boto3
import os
import json
import tempfile
import traceback
import datetime
from dotenv import load_dotenv
from fastavro import reader as avro_reader
import pyarrow.parquet as pq
from urllib.parse import urlparse, unquote

# Load environment variables from .env file (optional, good practice)
load_dotenv()

app = Flask(__name__)

# --- Configuration ---
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# --- Helper Functions ---
# [ Previous helper functions remain unchanged: extract_bucket_and_key, download_s3_file, parse_avro_file, read_parquet_sample, convert_bytes ]
def extract_bucket_and_key(s3_url):
    parsed_url = urlparse(s3_url); bucket = parsed_url.netloc; key = unquote(parsed_url.path).lstrip('/')
    if not s3_url.startswith("s3://") or not bucket or not key: raise ValueError("Invalid S3 URL")
    return bucket, key
def download_s3_file(s3_client, bucket, key, local_path):
    try: os.makedirs(os.path.dirname(local_path), exist_ok=True); s3_client.download_file(bucket, key, local_path)
    except Exception as e: print(f"Error downloading s3://{bucket}/{key}: {e}"); raise
def parse_avro_file(file_path):
    records = []; print(f"DEBUG: Attempting to parse Avro file: {file_path}")
    try:
        with open(file_path, 'rb') as fo: records = list(avro_reader(fo))
        print(f"DEBUG: Successfully parsed Avro file: {file_path}, Records found: {len(records)}")
        return records
    except Exception as e: print(f"Error parsing Avro file {file_path}: {e}"); traceback.print_exc(); raise
def read_parquet_sample(file_path, num_rows=5):
    try:
        print(f"DEBUG: Reading Parquet sample from: {file_path}"); table = pq.read_table(file_path)
        sample_table = table.slice(length=min(num_rows, len(table))); sample_data = sample_table.to_pylist()
        print(f"DEBUG: Successfully read {len(sample_data)} sample rows from Parquet: {file_path}"); return sample_data
    except Exception as e: print(f"Error reading Parquet file {file_path}: {e}"); return []
def convert_bytes(obj):
    if isinstance(obj, bytes):
        try: return obj.decode('utf-8')
        except UnicodeDecodeError: return f"<bytes len={len(obj)}>"
        except Exception: return f"<bytes len={len(obj)} error>"
    elif isinstance(obj, dict): return {convert_bytes(k): convert_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list): return [convert_bytes(item) for item in obj]
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        try: return obj.isoformat()
        except Exception: return str(obj)
    elif type(obj).__module__ == 'numpy' and hasattr(obj, 'item'): return obj.item()
    elif hasattr(obj, 'isoformat'):
         try: return obj.isoformat()
         except Exception: return str(obj)
    else:
        if not isinstance(obj, (str, int, float, bool, type(None))):
            try: json.dumps(obj)
            except TypeError: return str(obj)
        return obj
# --- Main Endpoint ---

@app.route('/Iceberg', methods=['GET'])
def iceberg_details():
    s3_url = request.args.get('s3_url')
    if not s3_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    try: bucket_name, table_base_key = extract_bucket_and_key(s3_url)
    except ValueError as e: return jsonify({"error": str(e)}), 400
    if not table_base_key.endswith('/'): table_base_key += '/'
    metadata_prefix = table_base_key + "metadata/"
    data_prefix = table_base_key + "data/"
    print(f"INFO: Processing Iceberg table at: s3://{bucket_name}/{table_base_key}")

    try:
        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)
        with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory: {temp_dir}")

            # 1. Find latest metadata.json
            list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
            if 'Contents' not in list_response: return jsonify({"error": f"No objects found under metadata prefix: {metadata_prefix}"}), 404
            metadata_files = sorted([obj['Key'] for obj in list_response['Contents'] if obj['Key'].endswith('.metadata.json')], reverse=True)
            if not metadata_files: return jsonify({"error": f"No *.metadata.json files found under {metadata_prefix}"}), 404
            latest_metadata_key = metadata_files[0]
            local_latest_metadata_path = os.path.join(temp_dir, os.path.basename(latest_metadata_key))

            # 2. Parse latest metadata.json
            download_s3_file(s3_client, bucket_name, latest_metadata_key, local_latest_metadata_path)
            with open(local_latest_metadata_path, 'r') as f: latest_meta = json.load(f)

            table_uuid = latest_meta.get("table-uuid")
            current_snapshot_id = latest_meta.get("current-snapshot-id")
            snapshots = latest_meta.get("snapshots", [])
            current_schema_id = latest_meta.get("current-schema-id", 0)
            schema = next((s for s in latest_meta.get("schemas", []) if s.get("schema-id") == current_schema_id), latest_meta.get("schema"))
            current_spec_id = latest_meta.get("current-spec-id", 0)
            partition_spec = next((s for s in latest_meta.get("partition-specs", []) if s.get("spec-id") == current_spec_id), latest_meta.get("partition-spec"))
            current_sort_order_id = latest_meta.get("current-sort-order-id", 0)
            sort_order = next((s for s in latest_meta.get("sort-orders", []) if s.get("order-id") == current_sort_order_id), latest_meta.get("sort-order"))
            properties = latest_meta.get("properties", {})
            format_version = latest_meta.get("format-version", 1)
            print(f"DEBUG: Table format version: {format_version}")
            print(f"DEBUG: Current Snapshot ID: {current_snapshot_id}")

            if not current_snapshot_id: return jsonify({"message": "Table exists but has no current snapshot."}), 200

            # 3. Find current snapshot & manifest list
            current_snapshot = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
            if not current_snapshot: return jsonify({"error": f"Snapshot ID {current_snapshot_id} not found."}), 404
            print(f"DEBUG: Current snapshot summary: {current_snapshot.get('summary', {})}") # Log snapshot summary

            manifest_list_path = current_snapshot.get("manifest-list")
            manifest_list_key = ""
            try:
                if not manifest_list_path: raise ValueError("Manifest list path missing")
                parsed_manifest_list_url = urlparse(manifest_list_path)
                if parsed_manifest_list_url.scheme == "s3":
                    manifest_list_bucket, manifest_list_key = extract_bucket_and_key(manifest_list_path)
                    if manifest_list_bucket != bucket_name: raise ValueError(f"Manifest list bucket mismatch")
                elif not parsed_manifest_list_url.scheme and parsed_manifest_list_url.path:
                    relative_path = unquote(parsed_manifest_list_url.path)
                    manifest_list_key = os.path.join(os.path.dirname(latest_metadata_key), relative_path).replace("\\", "/")
                else: raise ValueError(f"Cannot parse manifest list path format")
            except ValueError as e: return jsonify({"error": f"Error processing manifest list path '{manifest_list_path}': {e}"}), 400
            local_manifest_list_path = os.path.join(temp_dir, os.path.basename(manifest_list_key).replace('%', '_'))

            # 4. Download and parse manifest list
            download_s3_file(s3_client, bucket_name, manifest_list_key, local_manifest_list_path)
            manifest_list_entries = parse_avro_file(local_manifest_list_path)
            print(f"DEBUG: Number of manifest files listed: {len(manifest_list_entries)}")

            # Aggregation Variables
            total_data_files, gross_records_in_data_files, total_delete_files, approx_deleted_records = 0, 0, 0, 0
            total_data_storage_bytes, total_delete_storage_bytes = 0, 0
            partition_stats = {}
            data_file_paths_sample = []

            print("\nINFO: Processing Manifest Files...")
            # --- START Manifest File Loop ---
            printed_delete_entry = False # Flag to print only the first delete entry overall
            for i, entry in enumerate(manifest_list_entries):
                manifest_file_path = entry.get("manifest_path")
                print(f"\nDEBUG: Manifest List Entry {i+1}/{len(manifest_list_entries)}: Path='{manifest_file_path}'")
                if not manifest_file_path: continue
                manifest_file_key = ""
                try:
                    parsed_manifest_url = urlparse(manifest_file_path)
                    if parsed_manifest_url.scheme == "s3":
                        m_bucket, manifest_file_key = extract_bucket_and_key(manifest_file_path)
                        if m_bucket != bucket_name: raise ValueError("Bucket mismatch")
                    elif not parsed_manifest_url.scheme and parsed_manifest_url.path:
                        relative_path = unquote(parsed_manifest_url.path)
                        manifest_file_key = os.path.join(os.path.dirname(latest_metadata_key), relative_path).replace("\\", "/")
                    else: raise ValueError("Cannot parse manifest path format")
                except ValueError as e: print(f"Warning: Skipping manifest path '{manifest_file_path}' due to error: {e}"); continue
                local_manifest_path = os.path.join(temp_dir, os.path.basename(manifest_file_key).replace('%', '_'))

                try:
                    download_s3_file(s3_client, bucket_name, manifest_file_key, local_manifest_path)
                    manifest_records = parse_avro_file(local_manifest_path)
                    print(f"DEBUG: Processing {len(manifest_records)} entries in manifest: {os.path.basename(manifest_file_key)}")

                    # --- START Manifest Entry Loop ---
                    for j, manifest_entry in enumerate(manifest_records):

                        # !!!!!!!!!! START DEBUG BLOCK !!!!!!!!!!
                        current_content_for_debug = manifest_entry.get('content', 0)
                        # Print the raw structure of the FIRST DATA entry processed overall
                        if i == 0 and j == 0 and current_content_for_debug == 0:
                            print("\nDEBUG: >>>>>>> RAW First DATA Manifest Entry <<<<<<<")
                            try: print(json.dumps(manifest_entry, indent=2, default=str))
                            except Exception as dump_e: print(f"Could not JSON dump raw entry: {dump_e}"); print(f"Raw Entry (repr): {repr(manifest_entry)}")
                            print("DEBUG: >>>>>>> End RAW First DATA Manifest Entry <<<<<<<\n")
                        # Print the raw structure of the FIRST DELETE entry processed overall
                        if (current_content_for_debug == 1 or current_content_for_debug == 2) and not printed_delete_entry:
                            print("\nDEBUG: >>>>>>> RAW First DELETE Manifest Entry <<<<<<<")
                            try: print(json.dumps(manifest_entry, indent=2, default=str))
                            except Exception as dump_e: print(f"Could not JSON dump raw entry: {dump_e}"); print(f"Raw Entry (repr): {repr(manifest_entry)}")
                            print("DEBUG: >>>>>>> End RAW First DELETE Manifest Entry <<<<<<<\n")
                            printed_delete_entry = True
                        # !!!!!!!!!! END DEBUG BLOCK !!!!!!!!!!

                        status = manifest_entry.get('status', 0)
                        if status == 2: continue

                        # --- Extract fields, trying nested V2 struct first, then top-level ---
                        record_count, file_size, file_path, partition_data = 0, 0, "", None
                        content = manifest_entry.get('content', 0)
                        nested_info = manifest_entry.get('data_file') if content == 0 else manifest_entry.get('delete_file')

                        if nested_info: # Use nested V2 info
                            record_count = nested_info.get("record_count", 0) or 0
                            file_size = nested_info.get("file_size_in_bytes", 0) or 0
                            file_path = nested_info.get("file_path", "")
                            if content == 0: partition_data = nested_info.get("partition")
                        else: # Fallback to top-level V1 or flat V2
                            record_count = manifest_entry.get("record_count", 0) or 0
                            file_size = manifest_entry.get("file_size_in_bytes", 0) or 0
                            file_path = manifest_entry.get("file_path", "")
                            if content == 0 and format_version == 1: pass # V1 partition needs spec info - skipping detail for now

                        # Log extracted values for THIS entry
                        print(f"DEBUG:     Entry {j+1}: Content={content}, Path='{file_path}', Recs={record_count}, Size={file_size}")

                        # --- Accumulate ---
                        if content == 0: # Data File
                            total_data_files += 1
                            gross_records_in_data_files += record_count
                            total_data_storage_bytes += file_size
                            # Partition stats
                            if partition_data is not None:
                                partition_key_parts = {}
                                if isinstance(partition_data, dict):
                                    for k, v in sorted(partition_data.items()):
                                        if isinstance(v, (datetime.date, datetime.datetime)): partition_key_parts[k] = v.isoformat()
                                        else: partition_key_parts[k] = v
                                else: partition_key_parts = {'_value': str(partition_data)}
                                partition_key_str = json.dumps(partition_key_parts)
                                if partition_key_str not in partition_stats: partition_stats[partition_key_str] = {"gross_record_count": 0, "size_bytes": 0, "num_data_files": 0, "repr": partition_data}
                                partition_stats[partition_key_str]["gross_record_count"] += record_count
                                partition_stats[partition_key_str]["size_bytes"] += file_size
                                partition_stats[partition_key_str]["num_data_files"] += 1
                            # Sample path
                            if file_path and len(data_file_paths_sample) < 1 and file_path.lower().endswith(".parquet"):
                                full_data_file_path = ""; parsed_data_path = urlparse(file_path)
                                if parsed_data_path.scheme == "s3": full_data_file_path = file_path
                                elif not parsed_data_path.scheme and parsed_data_path.path:
                                    relative_data_path = unquote(parsed_data_path.path).lstrip('/')
                                    full_data_file_path = f"s3://{bucket_name}/{data_prefix}{relative_data_path}"
                                if full_data_file_path: data_file_paths_sample.append(full_data_file_path)
                        elif content == 1 or content == 2: # Delete File
                            total_delete_files += 1
                            approx_deleted_records += record_count
                            total_delete_storage_bytes += file_size
                    # --- END Manifest Entry Loop ---
                except Exception as manifest_err: print(f"ERROR: Failed to process manifest file {manifest_file_key}: {manifest_err}"); traceback.print_exc()
            # --- END Manifest File Loop ---
            print("INFO: Finished processing manifest files.")

            # 7. Get Sample Data
            # [ Sample data logic remains the same ]
            sample_data = []
            if data_file_paths_sample:
                sample_file_s3_path = data_file_paths_sample[0]; print(f"INFO: Attempting to get sample data from: {sample_file_s3_path}")
                try:
                     sample_file_bucket, sample_file_key = extract_bucket_and_key(sample_file_s3_path)
                     if sample_file_bucket != bucket_name: print(f"Warning: Sample data file bucket mismatch.")
                     else:
                        local_sample_filename = "sample_" + os.path.basename(sample_file_key).replace('%', '_')
                        local_sample_parquet_path = os.path.join(temp_dir, local_sample_filename)
                        download_s3_file(s3_client, bucket_name, sample_file_key, local_sample_parquet_path)
                        sample_data = read_parquet_sample(local_sample_parquet_path, num_rows=10)
                except Exception as sample_err: print(f"ERROR: Could not read sample data from {sample_file_s3_path}: {sample_err}"); sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
            else: print("INFO: No suitable parquet data file found in manifests for sampling.")

            # 8. Assemble the final result
            print("\nINFO: Assembling final result...")
            approx_live_records = max(0, gross_records_in_data_files - approx_deleted_records)
            avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
            avg_data_file_size_mb = (total_data_storage_bytes / total_data_files / (1024*1024)) if total_data_files > 0 else 0
            partition_explorer_data = [{"partition_values": v["repr"], "partition_key_string": k, "gross_record_count": v["gross_record_count"], "size_bytes": v["size_bytes"], "num_data_files": v["num_data_files"]} for k, v in partition_stats.items()]

            result = {
                "table_uuid": table_uuid, "location": s3_url, "format_version": format_version,
                "current_snapshot_id": current_snapshot_id, "table_schema": schema, "table_properties": properties,
                "partition_spec": partition_spec, "sort_order": sort_order,
                "version_history": {"total_snapshots": len(snapshots), "current_snapshot_summary": current_snapshot, "snapshots_overview": snapshots[-5:]},
                "key_metrics": {
                    "total_data_files": total_data_files, "total_delete_files": total_delete_files,
                    "gross_records_in_data_files": gross_records_in_data_files,
                    "approx_deleted_records_in_manifests": approx_deleted_records,
                    "approx_live_records": approx_live_records,
                    "metrics_note": "Live record count is an approximation based on manifest metadata counts for data and delete files. It may differ from query engine results. Partition record counts are gross counts from data files.",
                    "total_data_storage_bytes": total_data_storage_bytes, "total_delete_storage_bytes": total_delete_storage_bytes,
                    "avg_live_records_per_data_file": avg_live_records_per_data_file, "avg_data_file_size_mb": avg_data_file_size_mb,
                },
                "partition_explorer": partition_explorer_data, "sample_data": sample_data,
            }
            # print(f"DEBUG: Final key_metrics: {result['key_metrics']}")

            result_serializable = convert_bytes(result)
            return jsonify(result_serializable), 200

    # --- Error Handling ---
    # [ Error handling remains the same ]
    except boto3.exceptions.NoCredentialsError: print("ERROR: AWS credentials not found."); return jsonify({"error": "AWS credentials not found."}), 401
    except s3_client.exceptions.NoSuchBucket: print(f"ERROR: S3 bucket '{bucket_name}' not found or access denied."); return jsonify({"error": f"S3 bucket '{bucket_name}' not found or access denied."}), 404
    except s3_client.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code'); print(f"ERROR: AWS ClientError occurred: {error_code} - {e}")
        if error_code == 'AccessDenied': return jsonify({"error": f"Access Denied for S3 path: s3://{bucket_name}/{table_base_key}."}), 403
        elif error_code == 'NoSuchKey': return jsonify({"error": f"A required Iceberg metadata file was not found (NoSuchKey): {e}"}), 404
        else: traceback.print_exc(); return jsonify({"error": f"AWS ClientError: {e}"}), 500
    except FileNotFoundError as e: print(f"ERROR: FileNotFoundError occurred (likely download issue): {e}"); traceback.print_exc(); return jsonify({"error": f"A required local file was not found: {e}"}), 500
    except Exception as e: print(f"ERROR: An unexpected error occurred: {e}"); traceback.print_exc(); return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

@app.route('/', methods=['GET'])
def hello(): return "Hello! Use /iceberg-details?s3_url=<s3://your-bucket/path/to/iceberg-table/> to fetch detailed table metadata."

if __name__ == '__main__': app.run(debug=True, host='0.0.0.0', port=5000)