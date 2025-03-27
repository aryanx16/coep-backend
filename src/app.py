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
import time # Needed for Delta

load_dotenv()

app = Flask(__name__)

CORS(app)

# --- Configuration ---

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# --- Helper Functions ---

def extract_bucket_and_key(s3_url):
    """Extracts bucket and key from s3:// URL."""
    parsed_url = urlparse(s3_url)
    bucket = parsed_url.netloc
    key = unquote(parsed_url.path).lstrip('/')
    if not s3_url.startswith("s3://") or not bucket or not key:
        raise ValueError(f"Invalid S3 URL: {s3_url}")
    return bucket, key

def download_s3_file(s3_client, bucket, key, local_path):
    """Downloads a file from S3 to a local path."""
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(f"DEBUG: Downloading s3://{bucket}/{key} to {local_path}")
        s3_client.download_file(bucket, key, local_path)
        print(f"DEBUG: Download complete for s3://{bucket}/{key}")
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"ERROR: S3 object not found: s3://{bucket}/{key}")
            raise FileNotFoundError(f"S3 object not found: s3://{bucket}/{key}") from e
        else:
            print(f"ERROR: S3 ClientError downloading s3://{bucket}/{key}: {e}")
            raise
    except Exception as e:
        print(f"ERROR: Unexpected error downloading s3://{bucket}/{key}: {e}")
        traceback.print_exc()
        raise

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

def read_parquet_sample(s3_client, bucket, key, local_dir, num_rows=10):
    """Downloads a Parquet file from S3 and reads a sample."""
    local_filename = "sample_" + os.path.basename(key).replace('%', '_').replace(':', '_')
    local_path = os.path.join(local_dir, local_filename)
    try:
        download_s3_file(s3_client, bucket, key, local_path)
        print(f"DEBUG: Reading Parquet sample from: {local_path}")
        table = pq.read_table(local_path)
        sample_table = table.slice(length=min(num_rows, len(table)))
        sample_data = sample_table.to_pylist()
        print(f"DEBUG: Successfully read {len(sample_data)} sample rows from Parquet: {key}")
        return sample_data
    except FileNotFoundError:
        print(f"ERROR: Sample Parquet file not found on S3: s3://{bucket}/{key}")
        return [{"error": f"Sample Parquet file not found on S3", "details": f"s3://{bucket}/{key}"}]
    except Exception as e:
        print(f"Error reading Parquet file s3://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        return [{"error": f"Failed to read sample Parquet data", "details": str(e)}]
    finally:
        # Clean up downloaded sample file
        if os.path.exists(local_path):
            try: os.remove(local_path); print(f"DEBUG: Cleaned up {local_path}")
            except Exception as rm_err: print(f"Warning: Could not remove temp sample file {local_path}: {rm_err}")


def read_delta_checkpoint(s3_client, bucket, key, local_dir):
    """Downloads and reads a Delta checkpoint Parquet file."""
    local_filename = os.path.basename(key).replace('%', '_').replace(':', '_')
    local_path = os.path.join(local_dir, local_filename)
    actions = []
    try:
        download_s3_file(s3_client, bucket, key, local_path)
        print(f"DEBUG: Reading Delta checkpoint file: {local_path}")
        table = pq.read_table(local_path)
        # Checkpoint files have columns like 'txn', 'add', 'remove', 'metaData', 'protocol'
        # Each row represents one action, only one action column per row is non-null
        for batch in table.to_batches():
            batch_dict = batch.to_pydict()
            num_rows = len(batch_dict[list(batch_dict.keys())[0]]) # Get length from any column
            for i in range(num_rows):
                action = {}
                # Find which action column is not None for this row
                if batch_dict.get('add') and batch_dict['add'][i] is not None:
                    action['add'] = batch_dict['add'][i]
                elif batch_dict.get('remove') and batch_dict['remove'][i] is not None:
                    action['remove'] = batch_dict['remove'][i]
                elif batch_dict.get('metaData') and batch_dict['metaData'][i] is not None:
                    action['metaData'] = batch_dict['metaData'][i]
                elif batch_dict.get('protocol') and batch_dict['protocol'][i] is not None:
                    action['protocol'] = batch_dict['protocol'][i]
                elif batch_dict.get('txn') and batch_dict['txn'][i] is not None:
                    action['txn'] = batch_dict['txn'][i] # Less common in checkpoints, but possible
                # Add other action types if needed (commitInfo etc)

                if action: actions.append(action)

        print(f"DEBUG: Successfully read {len(actions)} actions from checkpoint: {key}")
        return actions
    except FileNotFoundError:
        print(f"ERROR: Checkpoint file not found on S3: s3://{bucket}/{key}")
        raise # Re-raise to be caught by caller
    except Exception as e:
        print(f"Error reading Delta checkpoint file s3://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        raise
    finally:
        # Clean up downloaded checkpoint file
        if os.path.exists(local_path):
            try: os.remove(local_path); print(f"DEBUG: Cleaned up {local_path}")
            except Exception as rm_err: print(f"Warning: Could not remove temp checkpoint file {local_path}: {rm_err}")

def read_delta_json_lines(s3_client, bucket, key, local_dir):
    """Downloads and reads a Delta JSON commit file line by line."""
    local_filename = os.path.basename(key).replace('%', '_').replace(':', '_')
    local_path = os.path.join(local_dir, local_filename)
    actions = []
    try:
        download_s3_file(s3_client, bucket, key, local_path)
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
        print(f"ERROR: JSON commit file not found on S3: s3://{bucket}/{key}")
        raise # Re-raise to be caught by caller
    except Exception as e:
        print(f"Error reading Delta JSON file s3://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        raise
    finally:
        # Clean up downloaded JSON file
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
    elif isinstance(obj, (datetime.datetime, datetime.date, time.struct_time)):
        try: return datetime.datetime.fromtimestamp(obj).isoformat() if isinstance(obj, time.struct_time) else obj.isoformat()
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
                print(f"DEBUG: Converting unknown type {type(obj)} to string.")
                return str(obj)
        return obj # Return basic types as is
    
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


# --- Iceberg Endpoint ---

@app.route('/Iceberg', methods=['GET'])
def iceberg_details():
    s3_url = request.args.get('s3_url')
    if not s3_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Iceberg Request for {s3_url} ---")

    try: bucket_name, table_base_key = extract_bucket_and_key(s3_url)
    except ValueError as e: return jsonify({"error": str(e)}), 400

    if not table_base_key.endswith('/'): table_base_key += '/'
    metadata_prefix = table_base_key + "metadata/"
    data_prefix = table_base_key + "data/" # Used for constructing sample data paths if relative

    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

    with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
        print(f"DEBUG: Using temporary directory: {temp_dir}")

        # 1. Find latest metadata.json
        list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
        if 'Contents' not in list_response: return jsonify({"error": f"No objects found under metadata prefix: {metadata_prefix}"}), 404

        # Filter and sort metadata files (handle potential version number variations)
        metadata_files = []
        for obj in list_response['Contents']:
            key = obj['Key']
            if key.endswith('.metadata.json'):
                 # Extract version number (e.g., v1.metadata.json, 00000-....metadata.json)
                 match = re.search(r'(v?\d+)-[a-f0-9-]+\.metadata\.json$', os.path.basename(key))
                 version_num = 0
                 if match:
                     try: version_num = int(match.group(1).lstrip('v'))
                     except ValueError: pass # Use 0 if parsing fails
                 # Use modification time as secondary sort key for safety
                 metadata_files.append({'key': key, 'version': version_num, 'last_modified': obj['LastModified']})

        if not metadata_files: return jsonify({"error": f"No *.metadata.json files found under {metadata_prefix}"}), 404

        # Sort by version descending, then by last modified descending
        metadata_files.sort(key=lambda x: (x['version'], x['last_modified']), reverse=True)
        latest_metadata_key = metadata_files[0]['key']
        print(f"INFO: Using latest metadata file: {latest_metadata_key}")
        local_latest_metadata_path = os.path.join(temp_dir, os.path.basename(latest_metadata_key).replace('%', '_'))

        # 2. Parse latest metadata.json
        download_s3_file(s3_client, bucket_name, latest_metadata_key, local_latest_metadata_path)
        with open(local_latest_metadata_path, 'r') as f: latest_meta = json.load(f)

        table_uuid = latest_meta.get("table-uuid")
        current_snapshot_id = latest_meta.get("current-snapshot-id")
        snapshots = latest_meta.get("snapshots", [])
        current_schema_id = latest_meta.get("current-schema-id", 0) # Default to 0 if missing
        schema = next((s for s in latest_meta.get("schemas", []) if s.get("schema-id") == current_schema_id), latest_meta.get("schema")) # Fallback to top-level schema
        current_spec_id = latest_meta.get("current-spec-id", 0)
        partition_spec = next((s for s in latest_meta.get("partition-specs", []) if s.get("spec-id") == current_spec_id), latest_meta.get("partition-spec"))
        current_sort_order_id = latest_meta.get("current-sort-order-id", 0)
        sort_order = next((s for s in latest_meta.get("sort-orders", []) if s.get("order-id") == current_sort_order_id), latest_meta.get("sort-order"))
        properties = latest_meta.get("properties", {})
        format_version = latest_meta.get("format-version", 1)
        print(f"DEBUG: Table format version: {format_version}")
        print(f"DEBUG: Current Snapshot ID: {current_snapshot_id}")

        if current_snapshot_id is None: # Check for None explicitly
             return jsonify({
                "message": "Table metadata found, but no current snapshot exists (table might be empty or in intermediate state).",
                "table_uuid": table_uuid, "location": s3_url, "format_version": format_version,
                "table_schema": schema, "table_properties": properties, "partition_spec": partition_spec,
                "version_history": {"total_snapshots": len(snapshots), "snapshots_overview": snapshots[-5:]} # Show history even if no current snapshot
             }), 200

        # 3. Find current snapshot & manifest list
        current_snapshot = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
        if not current_snapshot: return jsonify({"error": f"Current Snapshot ID {current_snapshot_id} referenced in metadata not found in snapshots list."}), 404
        print(f"DEBUG: Current snapshot summary: {current_snapshot.get('summary', {})}")

        manifest_list_path = current_snapshot.get("manifest-list")
        if not manifest_list_path: return jsonify({"error": f"Manifest list path missing in current snapshot {current_snapshot_id}"}), 404

        manifest_list_key = ""
        try:
            parsed_manifest_list_url = urlparse(manifest_list_path)
            if parsed_manifest_list_url.scheme == "s3":
                manifest_list_bucket, manifest_list_key = extract_bucket_and_key(manifest_list_path)
                if manifest_list_bucket != bucket_name:
                     print(f"Warning: Manifest list bucket '{manifest_list_bucket}' differs from table bucket '{bucket_name}'. Using manifest list bucket.")
                     # Allow cross-bucket manifest list, but use its bucket
                     # bucket_name = manifest_list_bucket # Uncomment if you want to force using the manifest list's bucket for subsequent operations
            elif not parsed_manifest_list_url.scheme and parsed_manifest_list_url.path:
                # Relative path - construct full path based on metadata location
                relative_path = unquote(parsed_manifest_list_url.path)
                manifest_list_key = os.path.normpath(os.path.join(os.path.dirname(latest_metadata_key), relative_path)).replace("\\", "/")
            else: raise ValueError(f"Cannot parse manifest list path format: {manifest_list_path}")
        except ValueError as e: return jsonify({"error": f"Error processing manifest list path '{manifest_list_path}': {e}"}), 400
        local_manifest_list_path = os.path.join(temp_dir, os.path.basename(manifest_list_key).replace('%', '_'))

        # 4. Download and parse manifest list (Avro)
        download_s3_file(s3_client, bucket_name, manifest_list_key, local_manifest_list_path)
        manifest_list_entries = parse_avro_file(local_manifest_list_path)
        print(f"DEBUG: Number of manifest files listed: {len(manifest_list_entries)}")

        # 5. Process Manifest Files (Avro)
        total_data_files, gross_records_in_data_files, total_delete_files, approx_deleted_records = 0, 0, 0, 0
        total_data_storage_bytes, total_delete_storage_bytes = 0, 0
        partition_stats = {}
        data_file_paths_sample = [] # Store potential sample file paths

        print("\nINFO: Processing Manifest Files...")
        for i, entry in enumerate(manifest_list_entries):
            manifest_file_path = entry.get("manifest_path")
            print(f"\nDEBUG: Manifest List Entry {i+1}/{len(manifest_list_entries)}: Path='{manifest_file_path}'")
            if not manifest_file_path:
                print(f"Warning: Skipping manifest list entry {i+1} due to missing 'manifest_path'.")
                continue

            manifest_file_key = ""
            manifest_bucket = bucket_name # Assume same bucket unless specified otherwise
            try:
                parsed_manifest_url = urlparse(manifest_file_path)
                if parsed_manifest_url.scheme == "s3":
                    m_bucket, manifest_file_key = extract_bucket_and_key(manifest_file_path)
                    manifest_bucket = m_bucket # Use the bucket specified in the manifest path
                elif not parsed_manifest_url.scheme and parsed_manifest_url.path:
                    relative_path = unquote(parsed_manifest_url.path)
                    # Manifest paths can be relative to the manifest list file's location
                    manifest_file_key = os.path.normpath(os.path.join(os.path.dirname(manifest_list_key), relative_path)).replace("\\", "/")
                else: raise ValueError("Cannot parse manifest file path format")

                local_manifest_path = os.path.join(temp_dir, f"manifest_{i}_" + os.path.basename(manifest_file_key).replace('%', '_'))
                download_s3_file(s3_client, manifest_bucket, manifest_file_key, local_manifest_path)
                manifest_records = parse_avro_file(local_manifest_path)
                print(f"DEBUG: Processing {len(manifest_records)} entries in manifest: {os.path.basename(manifest_file_key)}")

                for j, manifest_entry in enumerate(manifest_records):
                    status = manifest_entry.get('status', 0) # 0:EXISTING, 1:ADDED, 2:DELETED
                    if status == 2: continue # Skip DELETED entries in the manifest file itself

                    record_count, file_size, file_path_in_manifest, partition_data = 0, 0, "", None
                    content = 0 # 0: data, 1: position deletes, 2: equality deletes

                    # --- Extract fields based on Format Version ---
                    if format_version == 2:
                         # V2 uses nested structure: 'data_file' or 'delete_file'
                         if 'data_file' in manifest_entry and manifest_entry['data_file'] is not None:
                             content = 0
                             nested_info = manifest_entry['data_file']
                             record_count = nested_info.get("record_count", 0) or 0
                             file_size = nested_info.get("file_size_in_bytes", 0) or 0
                             file_path_in_manifest = nested_info.get("file_path", "")
                             partition_data = nested_info.get("partition") # This is the struct representation
                             # V2 partition struct example: {"tpep_pickup_datetime_day": 18993} (days since epoch)
                             # We need the spec to interpret this correctly if we wanted human-readable partitions
                         elif 'delete_file' in manifest_entry and manifest_entry['delete_file'] is not None:
                             nested_info = manifest_entry['delete_file']
                             content = nested_info.get("content", 1) # Default to position delete if missing
                             record_count = nested_info.get("record_count", 0) or 0
                             file_size = nested_info.get("file_size_in_bytes", 0) or 0
                             file_path_in_manifest = nested_info.get("file_path", "")
                             # Delete files might also have partition info, but we mainly care about counts/size here
                         else:
                             print(f"Warning: V2 manifest entry {j+1} in {manifest_file_key} has neither 'data_file' nor 'delete_file'. Skipping.")
                             continue
                    elif format_version == 1:
                         # V1 uses flat structure
                         content = 0 # V1 manifests only list data files
                         record_count = manifest_entry.get("record_count", 0) or 0
                         file_size = manifest_entry.get("file_size_in_bytes", 0) or 0
                         file_path_in_manifest = manifest_entry.get("file_path", "")
                         # V1 partition data is a tuple/list matching the partition spec's order
                         partition_data = manifest_entry.get("partition")
                         # Example: [18993]
                         # To make this comparable to V2, we'd need to map it using partition_spec
                    else:
                        print(f"Warning: Unsupported Iceberg format version {format_version}. Skipping manifest entry.")
                        continue

                    # --- Construct Full S3 Path for the data/delete file ---
                    full_file_s3_path = ""
                    file_bucket = bucket_name # Default to table bucket
                    try:
                        parsed_file_path = urlparse(file_path_in_manifest)
                        if parsed_file_path.scheme == "s3":
                            f_bucket, f_key = extract_bucket_and_key(file_path_in_manifest)
                            file_bucket = f_bucket
                            full_file_s3_path = file_path_in_manifest
                        elif not parsed_file_path.scheme and parsed_file_path.path:
                            # Path is relative to the table's base location usually
                            relative_data_path = unquote(parsed_file_path.path).lstrip('/')
                            # Ensure we don't add double slashes if table_base_key ends with / and relative starts with /
                            full_file_s3_path = f"s3://{bucket_name}/{table_base_key.rstrip('/')}/{relative_data_path}"
                        else:
                            print(f"Warning: Could not determine full S3 path for file '{file_path_in_manifest}'. Skipping accumulation for this file.")
                            continue
                    except ValueError as path_err:
                         print(f"Warning: Error parsing file path '{file_path_in_manifest}': {path_err}. Skipping accumulation.")
                         continue

                    # --- Accumulate ---
                    if content == 0: # Data File
                        total_data_files += 1
                        gross_records_in_data_files += record_count
                        total_data_storage_bytes += file_size
                        # Partition stats - Create a consistent string representation
                        partition_key_string = "<unpartitioned>"
                        partition_values_repr = None
                        if partition_data is not None and partition_spec and partition_spec.get('fields'):
                             try:
                                 # Try to create a {name: value} dict representation
                                 field_names = [f['name'] for f in partition_spec['fields']]
                                 if format_version == 2 and isinstance(partition_data, dict):
                                     # V2 - Already a dict, ensure keys match spec fields (usually do)
                                     partition_values_repr = {name: partition_data.get(name) for name in field_names if name in partition_data}
                                 elif format_version == 1 and isinstance(partition_data, (list, tuple)) and len(partition_data) == len(field_names):
                                     # V1 - List/Tuple, map to names by order
                                     partition_values_repr = dict(zip(field_names, partition_data))
                                 else:
                                      print(f"Warning: Mismatch between partition data type ({type(partition_data)}) and spec/format version. Treating as string.")
                                      partition_values_repr = {'_raw': str(partition_data)}

                                 # Convert values (like dates/timestamps if needed, though often ints/strings)
                                 # For simplicity, we'll use the raw values from the manifest directly here.
                                 # To display human-readable dates (like '2022-01-01'), more complex spec interpretation is needed.
                                 partition_key_string = json.dumps(dict(sorted(partition_values_repr.items())), default=str) # Sort for consistency

                             except Exception as part_err:
                                  print(f"Warning: Error processing partition data {partition_data} with spec {partition_spec}: {part_err}")
                                  partition_key_string = f"<error: {part_err}>"
                                  partition_values_repr = {'_error': str(part_err)}

                        elif partition_data is None and partition_spec and not partition_spec.get('fields'):
                             partition_key_string = "<unpartitioned>" # Explicitly unpartitioned
                        elif partition_data is not None:
                             partition_key_string = str(partition_data) # Fallback if spec is missing
                             partition_values_repr = {'_raw': partition_data}


                        if partition_key_string not in partition_stats: partition_stats[partition_key_string] = {"gross_record_count": 0, "size_bytes": 0, "num_data_files": 0, "partition_values": partition_values_repr}
                        partition_stats[partition_key_string]["gross_record_count"] += record_count
                        partition_stats[partition_key_string]["size_bytes"] += file_size
                        partition_stats[partition_key_string]["num_data_files"] += 1

                        # Store sample path if it's a Parquet file
                        if full_file_s3_path and len(data_file_paths_sample) < 1 and full_file_s3_path.lower().endswith(".parquet"):
                            data_file_paths_sample.append({'bucket': file_bucket, 'key': urlparse(full_file_s3_path).path.lstrip('/')})

                    elif content == 1 or content == 2: # Delete File (Position or Equality)
                        total_delete_files += 1
                        approx_deleted_records += record_count # This is an approximation
                        total_delete_storage_bytes += file_size

            except Exception as manifest_err:
                 print(f"ERROR: Failed to process manifest file {manifest_file_key} from bucket {manifest_bucket}: {manifest_err}")
                 traceback.print_exc()
                 # Continue to the next manifest file if one fails
        # --- END Manifest File Loop ---
        print("INFO: Finished processing manifest files.")

        # 6. Get Sample Data (if a suitable file was found)
        sample_data = []
        if data_file_paths_sample:
            sample_file_info = data_file_paths_sample[0]
            print(f"INFO: Attempting to get sample data from: s3://{sample_file_info['bucket']}/{sample_file_info['key']}")
            try:
                # Pass s3_client, bucket, key, and temp_dir to the sampling function
                sample_data = read_parquet_sample(s3_client, sample_file_info['bucket'], sample_file_info['key'], temp_dir, num_rows=10)
            except Exception as sample_err:
                 # Error is already printed inside read_parquet_sample
                 sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
        else: print("INFO: No suitable Parquet data file found in manifests for sampling.")

        # 7. Assemble the final result
        print("\nINFO: Assembling final Iceberg result...")
        approx_live_records = max(0, gross_records_in_data_files - approx_deleted_records)
        avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
        avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024))

        # Format partition explorer data
        partition_explorer_data = []
        for k, v in partition_stats.items():
             partition_explorer_data.append({
                 "partition_values": v["partition_values"], # The dict representation
                 "partition_key_string": k, # The JSON string representation used as key
                 "gross_record_count": v["gross_record_count"],
                 "size_bytes": v["size_bytes"],
                 "num_data_files": v["num_data_files"]
            })
        # Sort partitions for consistent display (optional)
        partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))


        result = {
            "table_uuid": table_uuid, "location": s3_url, "format_version": format_version,
            "current_snapshot_id": current_snapshot_id, "table_schema": schema, "table_properties": properties,
            "partition_spec": partition_spec, "sort_order": sort_order,
            "version_history": {
                "total_snapshots": len(snapshots),
                "current_snapshot_summary": current_snapshot,
                "snapshots_overview": snapshots # Maybe limit this: snapshots[-min(len(snapshots), 10):]
            },
            "key_metrics": {
                "total_data_files": total_data_files, "total_delete_files": total_delete_files,
                "gross_records_in_data_files": gross_records_in_data_files,
                "approx_deleted_records_in_manifests": approx_deleted_records, # Renamed for clarity
                "approx_live_records": approx_live_records, # Approximation based on manifest counts
                "metrics_note": "Live record count is an approximation based on manifest metadata counts for data and delete files. It may differ from query engine results. Partition record counts are gross counts from data files.",
                "total_data_storage_bytes": total_data_storage_bytes, "total_delete_storage_bytes": total_delete_storage_bytes,
                "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                "avg_data_file_size_mb": round(avg_data_file_size_mb, 4),
            },
            "partition_explorer": partition_explorer_data,
            "sample_data": sample_data,
        }

        result_serializable = convert_bytes(result)
        end_time = time.time()
        print(f"--- Iceberg Request Completed in {end_time - start_time:.2f} seconds ---")
        return jsonify(result_serializable), 200


# --- Delta Lake Endpoint (Modified) ---

@app.route('/Delta', methods=['GET'])
def delta_details():
    s3_url = request.args.get('s3_url')
    if not s3_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Delta Request for {s3_url} ---")

    # Initialize variables that might be used in error handling before assignment
    bucket_name = None
    table_base_key = None
    delta_log_prefix = None

    try:
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)

        if not table_base_key.endswith('/'): table_base_key += '/'
        delta_log_prefix = table_base_key + "_delta_log/"
        print(f"INFO: Processing Delta Lake table at: s3://{bucket_name}/{table_base_key}")
        print(f"INFO: Delta log prefix: {delta_log_prefix}")

        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

        with tempfile.TemporaryDirectory(prefix="delta_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory: {temp_dir}")

            # --- 1. Find Delta Log Files ---
            log_files = []
            continuation_token = None
            print(f"DEBUG: Listing objects under {delta_log_prefix}")
            while True:
                list_kwargs = {'Bucket': bucket_name, 'Prefix': delta_log_prefix}
                if continuation_token: list_kwargs['ContinuationToken'] = continuation_token
                list_response = s3_client.list_objects_v2(**list_kwargs)

                if 'Contents' not in list_response:
                    try:
                        # Use list_objects_v2 with MaxKeys=0 to check existence without retrieving objects
                        s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_base_key, MaxKeys=0)
                        # If no error, base exists, but _delta_log is missing/empty
                        return jsonify({"error": f"Delta log prefix '{delta_log_prefix}' not found or empty. Ensure the path points to a valid Delta table directory."}), 404
                    except s3_client.exceptions.ClientError as head_err:
                         print(f"ERROR: Could not access base table path {table_base_key}: {head_err}")
                         # Check if the error is AccessDenied
                         if head_err.response.get('Error', {}).get('Code') == 'AccessDenied':
                             return jsonify({"error": f"Access Denied for S3 path s3://{bucket_name}/{table_base_key}. Check permissions."}), 403
                         else:
                             # Assume other errors mean path doesn't exist or other issue
                             return jsonify({"error": f"Cannot access S3 path s3://{bucket_name}/{table_base_key}. Check path and permissions. Error: {head_err}"}), 404


                log_files.extend(list_response['Contents'])
                if list_response.get('IsTruncated'): continuation_token = list_response.get('NextContinuationToken')
                else: break
            print(f"DEBUG: Found {len(log_files)} total objects under delta log prefix.")

            json_commits = {}
            checkpoint_files = {}
            last_checkpoint_info = None

            json_pattern = re.compile(r"(\d+)\.json$")
            checkpoint_pattern = re.compile(r"(\d+)\.checkpoint(?:\.(\d+)\.(\d+))?\.parquet$")

            for obj in log_files:
                key = obj['Key']
                filename = os.path.basename(key)

                if filename == "_last_checkpoint":
                    local_last_cp_path = os.path.join(temp_dir, "_last_checkpoint")
                    try:
                        download_s3_file(s3_client, bucket_name, key, local_last_cp_path)
                        with open(local_last_cp_path, 'r') as f:
                            last_checkpoint_data = json.load(f)
                            last_checkpoint_info = {
                                'version': last_checkpoint_data['version'],
                                'parts': last_checkpoint_data.get('parts'),
                                'key': key
                            }
                            print(f"DEBUG: Found _last_checkpoint pointing to version {last_checkpoint_info['version']} (parts: {last_checkpoint_info['parts']})")
                    except Exception as cp_err:
                        print(f"Warning: Failed to read or parse _last_checkpoint file {key}: {cp_err}")
                    continue

                json_match = json_pattern.match(filename)
                if json_match:
                    version = int(json_match.group(1))
                    json_commits[version] = {'key': key, 'last_modified': obj['LastModified']}
                    continue

                cp_match = checkpoint_pattern.match(filename)
                if cp_match:
                    version = int(cp_match.group(1))
                    part_num = int(cp_match.group(2)) if cp_match.group(2) else 1
                    num_parts = int(cp_match.group(3)) if cp_match.group(3) else 1
                    if version not in checkpoint_files: checkpoint_files[version] = {'num_parts': num_parts, 'parts': {}}
                    checkpoint_files[version]['parts'][part_num] = {'key': key, 'last_modified': obj['LastModified']}
                    if checkpoint_files[version]['num_parts'] != num_parts and cp_match.group(2):
                        print(f"Warning: Inconsistent number of parts detected for checkpoint version {version}.")
                        checkpoint_files[version]['num_parts'] = max(checkpoint_files[version]['num_parts'], num_parts)


            if not json_commits:
                return jsonify({"error": "No Delta commit JSON files found in _delta_log. Invalid Delta table."}), 404

            current_snapshot_id = max(json_commits.keys()) # Renamed from latest_version
            print(f"INFO: Latest Delta version (snapshot ID): {current_snapshot_id}")

            # --- 2. Determine State Reconstruction Strategy ---
            active_files = {}
            metadata_from_log = None # Store metadata found during forward pass
            protocol_from_log = None # Store protocol found during forward pass
            start_process_version = 0
            checkpoint_version_used = None

            # Find the effective checkpoint to use
            effective_checkpoint_version = -1
            if last_checkpoint_info:
                 cp_version_candidate = last_checkpoint_info['version']
                 # Verify the checkpoint files actually exist and are complete
                 if cp_version_candidate in checkpoint_files and \
                    len(checkpoint_files[cp_version_candidate]['parts']) == checkpoint_files[cp_version_candidate]['num_parts']:
                    effective_checkpoint_version = cp_version_candidate
                 else:
                     print(f"Warning: _last_checkpoint points to version {cp_version_candidate}, but files are missing or incomplete. Trying to find latest complete checkpoint.")
                     # Fallback: Find the highest version checkpoint that IS complete
                     available_complete_checkpoints = sorted([
                         v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']
                     ], reverse=True)
                     if available_complete_checkpoints:
                         effective_checkpoint_version = available_complete_checkpoints[0]
                         print(f"INFO: Using latest complete checkpoint found at version {effective_checkpoint_version} instead.")
                     else:
                         print("Warning: No complete checkpoint files found. Will process all JSON logs.")
                         effective_checkpoint_version = -1

            elif checkpoint_files:
                 # No _last_checkpoint, find the latest complete checkpoint available
                 available_complete_checkpoints = sorted([
                     v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']
                 ], reverse=True)
                 if available_complete_checkpoints:
                     effective_checkpoint_version = available_complete_checkpoints[0]
                     print(f"INFO: No _last_checkpoint found. Using latest complete checkpoint found at version {effective_checkpoint_version}.")
                 else:
                     print("Warning: No complete checkpoint files found. Will process all JSON logs.")
                     effective_checkpoint_version = -1

            # Process Checkpoint if found
            if effective_checkpoint_version > -1:
                print(f"INFO: Reading state from checkpoint version {effective_checkpoint_version}")
                checkpoint_version_used = effective_checkpoint_version
                cp_info = checkpoint_files[effective_checkpoint_version]
                all_checkpoint_actions = []
                try:
                    for part_num in sorted(cp_info['parts'].keys()):
                        part_key = cp_info['parts'][part_num]['key']
                        all_checkpoint_actions.extend(read_delta_checkpoint(s3_client, bucket_name, part_key, temp_dir))

                    for action in all_checkpoint_actions:
                        if 'add' in action and action['add']:
                            add_info = action['add']
                            path = add_info['path']
                            mod_time = add_info.get('modificationTime', 0)
                            stats_parsed = None
                            if add_info.get('stats'):
                                try: stats_parsed = json.loads(add_info['stats'])
                                except: pass
                            active_files[path] = {
                                'size': add_info['size'],
                                'partitionValues': add_info.get('partitionValues', {}),
                                'modificationTime': mod_time,
                                'stats': stats_parsed,
                                'tags': add_info.get('tags')
                            }
                        elif 'metaData' in action and action['metaData']:
                            metadata_from_log = action['metaData']
                            print(f"DEBUG: Found metaData in checkpoint {effective_checkpoint_version}")
                        elif 'protocol' in action and action['protocol']:
                            protocol_from_log = action['protocol']
                            print(f"DEBUG: Found protocol in checkpoint {effective_checkpoint_version}")

                    start_process_version = effective_checkpoint_version + 1
                    print(f"INFO: Checkpoint processing complete. Starting JSON processing from version {start_process_version}")

                except Exception as cp_read_err:
                    print(f"ERROR: Failed to read or process checkpoint version {effective_checkpoint_version}: {cp_read_err}. Falling back to processing all JSON logs.")
                    active_files = {} # Reset state
                    metadata_from_log = None
                    protocol_from_log = None
                    start_process_version = 0
                    checkpoint_version_used = None
            else:
                 print("INFO: No usable checkpoint found. Processing all JSON logs from version 0.")
                 start_process_version = 0

            # --- 3. Process JSON Commits ---
            versions_to_process = sorted([v for v in json_commits if v >= start_process_version])
            if not versions_to_process and not active_files and checkpoint_version_used is None:
                if current_snapshot_id == 0 and 0 in json_commits:
                     print("INFO: Only version 0 found. Processing version 0.")
                     versions_to_process = [0]
                else:
                     return jsonify({"error": "No JSON commits found to process and no checkpoint loaded."}), 500
            elif not versions_to_process and checkpoint_version_used is not None:
                 print("INFO: No JSON files found after the checkpoint. State is as per checkpoint.")

            print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} to {current_snapshot_id}...")
            all_commit_info = {} # Store details per commit for history

            for version in versions_to_process:
                commit_file_info = json_commits[version]
                commit_key = commit_file_info['key']
                print(f"DEBUG: Processing version {version} ({commit_key})...")
                try:
                    actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
                    commit_summary_details = {'version': version, 'timestamp': None, 'operation': 'Unknown', 'num_actions': len(actions), 'operationParameters': {}, 'num_added_files': 0, 'num_removed_files': 0, 'added_bytes': 0, 'removed_bytes': 0}

                    for action in actions:
                        if 'commitInfo' in action and action['commitInfo']:
                             ci = action['commitInfo']
                             commit_summary_details['timestamp'] = ci.get('timestamp')
                             commit_summary_details['operation'] = ci.get('operation', 'Unknown')
                             commit_summary_details['operationParameters'] = ci.get('operationParameters', {})
                             # Try to get file counts from operationMetrics if available (e.g., for WRITE, MERGE)
                             op_metrics = ci.get('operationMetrics', {})
                             commit_summary_details['num_added_files'] = int(op_metrics.get('numOutputFiles', commit_summary_details['num_added_files']))
                             commit_summary_details['num_removed_files'] = int(op_metrics.get('numRemovedFiles', commit_summary_details['num_removed_files']))
                             commit_summary_details['added_bytes'] = int(op_metrics.get('numOutputBytes', commit_summary_details['added_bytes']))
                             # removed bytes often not present in metrics

                        elif 'add' in action and action['add']:
                            add_info = action['add']
                            path = add_info['path']
                            mod_time = add_info.get('modificationTime', 0)
                            stats_parsed = None
                            if add_info.get('stats'):
                                try: stats_parsed = json.loads(add_info['stats'])
                                except: pass
                            active_files[path] = {
                                'size': add_info['size'],
                                'partitionValues': add_info.get('partitionValues', {}),
                                'modificationTime': mod_time,
                                'stats': stats_parsed,
                                'tags': add_info.get('tags')
                            }
                            # Increment counts if not already captured by operationMetrics
                            if not commit_summary_details['operationParameters'].get('numOutputFiles'):
                                commit_summary_details['num_added_files'] += 1
                                commit_summary_details['added_bytes'] += add_info['size']


                        elif 'remove' in action and action['remove']:
                            remove_info = action['remove']
                            path = remove_info['path']
                            if remove_info.get('dataChange', True):
                               if path in active_files:
                                   removed_file_info = active_files.pop(path) # Simple removal
                                   # Increment counts if not already captured by operationMetrics
                                   if not commit_summary_details['operationParameters'].get('numRemovedFiles'):
                                       commit_summary_details['num_removed_files'] += 1
                                       commit_summary_details['removed_bytes'] += removed_file_info.get('size', 0)

                        elif 'metaData' in action and action['metaData']:
                            metadata_from_log = action['metaData']
                            print(f"DEBUG: Updated metaData from version {version}")
                        elif 'protocol' in action and action['protocol']:
                            protocol_from_log = action['protocol']
                            print(f"DEBUG: Updated protocol from version {version}")

                    all_commit_info[version] = commit_summary_details

                except Exception as json_proc_err:
                    print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
                    traceback.print_exc()

            print(f"INFO: Finished processing JSON logs.")

            # --- 3.5 Find Definitive Metadata & Protocol ---
            # Read backwards for the *most recent* instances
            definitive_metadata = None
            definitive_protocol = None
            if metadata_from_log: definitive_metadata = metadata_from_log # Start with latest found during forward pass
            if protocol_from_log: definitive_protocol = protocol_from_log

            # Only search backwards if needed (e.g., if not found during forward pass or to be absolutely sure)
            if not definitive_metadata or not definitive_protocol:
                 print("DEBUG: Searching backwards for definitive metadata and/or protocol...")
                 for v in range(current_snapshot_id, -1, -1):
                     if v not in json_commits: continue
                     if definitive_metadata and definitive_protocol: break

                     commit_key = json_commits[v]['key']
                     try:
                         actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
                         for action in reversed(actions): # Check actions within commit
                             if not definitive_metadata and 'metaData' in action and action['metaData']:
                                 definitive_metadata = action['metaData']
                                 print(f"DEBUG: Found definitive metaData in version {v}")
                             if not definitive_protocol and 'protocol' in action and action['protocol']:
                                 definitive_protocol = action['protocol']
                                 print(f"DEBUG: Found definitive protocol in version {v}")
                             if definitive_metadata and definitive_protocol: break
                     except Exception as bk_err:
                         print(f"Warning: Error reading version {v} while searching backwards: {bk_err}")


            if not definitive_metadata:
                 # If still not found, the table is likely invalid or in a strange state
                 return jsonify({"error": "Could not determine table metadata (schema, partitioning). Invalid Delta table state."}), 500
            if not definitive_protocol:
                 print("Warning: Could not find protocol information. Using defaults.")
                 # Use default protocol if none found (should be rare for valid tables)
                 definitive_protocol = {"minReaderVersion": 1, "minWriterVersion": 2}


            # --- 3.6 Parse Schema and Format Partition Spec ---
            table_schema = _parse_delta_schema_string(definitive_metadata.get("schemaString", "{}"))
            if not table_schema:
                 return jsonify({"error": "Failed to parse table schema from metadata."}), 500

            partition_cols = definitive_metadata.get("partitionColumns", [])
            partition_spec_fields = []
            schema_fields_map = {f['name']: f for f in table_schema.get('fields', [])}
            for i, col_name in enumerate(partition_cols):
                 source_field = schema_fields_map.get(col_name)
                 if source_field:
                     partition_spec_fields.append({
                         "name": col_name,
                         "transform": "identity", # Delta uses identity transform for partitioning
                         "source-id": source_field['id'],
                         "field-id": 1000 + i # Assign sequential field IDs starting from 1000
                     })
                 else:
                      print(f"Warning: Partition column '{col_name}' not found in table schema.")
                      # Add with minimal info if desired, or skip
                      # partition_spec_fields.append({"name": col_name, "transform": "identity"})

            partition_spec = {"spec-id": 0, "fields": partition_spec_fields}

            # --- 4. Calculate Metrics (Aligned with Iceberg) ---
            total_data_files = len(active_files) # Renamed from total_active_files
            total_delete_files = 0 # Delta doesn't have separate delete files like Iceberg V2 manifests
            total_data_storage_bytes = sum(f['size'] for f in active_files.values() if f.get('size')) # Renamed from total_size_bytes
            total_delete_storage_bytes = 0

            approx_live_records = 0 # Renamed from estimated_total_records
            files_missing_stats = 0
            for f in active_files.values():
                if f.get('stats') and 'numRecords' in f['stats']:
                    approx_live_records += f['stats']['numRecords']
                else:
                    files_missing_stats += 1

            # Use live records as proxy for gross records (best estimate available without full history scan)
            gross_records_in_data_files = approx_live_records
            # Deleted records difficult to track accurately without processing all 'remove' actions vs 'add' actions
            approx_deleted_records_in_manifests = 0

            avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
            avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024)) # Renamed

            metrics_note = f"Live record count ({approx_live_records}) is an estimate based on available 'numRecords' in Delta file stats."
            if files_missing_stats > 0:
                metrics_note += f" Stats were missing or unparseable for {files_missing_stats}/{total_data_files} active files."
            metrics_note += " Delta Lake does not track explicit delete files like Iceberg V2."


            # --- 5. Calculate Partition Stats (Aligned with Iceberg) ---
            partition_stats = {}
            for path, file_info in active_files.items():
                part_values = file_info.get('partitionValues', {})
                part_key_string = json.dumps(dict(sorted(part_values.items())), default=str) if part_values else "<unpartitioned>"

                if part_key_string not in partition_stats:
                     partition_stats[part_key_string] = {
                         "partition_values": part_values,
                         "partition_key_string": part_key_string, # Add the key string itself
                         "num_data_files": 0,
                         "size_bytes": 0,
                         "gross_record_count": 0 # Renamed from estimated_record_count
                     }

                partition_stats[part_key_string]["num_data_files"] += 1
                partition_stats[part_key_string]["size_bytes"] += file_info.get('size', 0)
                if file_info.get('stats') and 'numRecords' in file_info['stats']:
                     partition_stats[part_key_string]["gross_record_count"] += file_info['stats']['numRecords']

            partition_explorer_data = list(partition_stats.values())
            partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))

            # --- 6. Get Sample Data ---
            sample_data = []
            if active_files:
                 sample_file_path = list(active_files.keys())[0]
                 full_sample_s3_key = os.path.join(table_base_key.rstrip('/'), sample_file_path).replace("\\", "/") # Use os.path.join
                 print(f"INFO: Attempting to get sample data from: s3://{bucket_name}/{full_sample_s3_key}")
                 try:
                      sample_data = read_parquet_sample(s3_client, bucket_name, full_sample_s3_key, temp_dir, num_rows=5) # Fetch 5 rows
                 except Exception as sample_err:
                      sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
            else:
                 print("INFO: No active data files found to sample.")


            # --- 7. Assemble Final Result (Aligned with Iceberg) ---
            print("\nINFO: Assembling final Delta result...")

            # Format version history
            snapshots_overview = []
            # Iterate through versions found in json_commits, in reverse order, limit to ~10
            versions_in_history = sorted(json_commits.keys(), reverse=True)
            for v in versions_in_history[:min(len(versions_in_history), 10)]:
                 commit_details = all_commit_info.get(v) # Get processed details
                 summary = {}
                 if commit_details:
                     summary = {
                         "operation": commit_details.get('operation', 'Unknown'),
                         "added-data-files": str(commit_details.get('num_added_files', 'N/A')), # Keep as string like Iceberg?
                         "removed-data-files": str(commit_details.get('num_removed_files', 'N/A')),
                         "added-files-size": str(commit_details.get('added_bytes', 'N/A')),
                         # "removed-files-size": str(commit_details.get('removed_bytes', 'N/A')), # Often not available
                         "operation-parameters": commit_details.get('operationParameters', {}) # Add parameters
                     }

                 snapshots_overview.append({
                     "snapshot-id": v,
                     "timestamp-ms": commit_details.get('timestamp') if commit_details else None,
                     "summary": summary
                 })

            current_snapshot_summary = next((s for s in snapshots_overview if s["snapshot-id"] == current_snapshot_id), None)
            if current_snapshot_summary and current_snapshot_id > 0:
                current_snapshot_summary["parent-snapshot-id"] = current_snapshot_id - 1
            elif current_snapshot_summary:
                 current_snapshot_summary["parent-snapshot-id"] = None # No parent for version 0


            result = {
                "table_uuid": definitive_metadata.get("id"), # Renamed from table_id
                "location": s3_url,
                "format_version": definitive_protocol.get('minReaderVersion', 1), # Using minReaderVersion as format indicator
                "current_snapshot_id": current_snapshot_id, # Renamed from latest_version
                "table_schema": table_schema,
                "table_properties": definitive_metadata.get("configuration", {}), # Renamed from configuration
                "partition_spec": partition_spec,
                "sort_order": {"order-id": 0, "fields": []}, # Add empty sort order

                "version_history": {
                    "total_snapshots": current_snapshot_id + 1, # Versions are 0-based index
                    "current_snapshot_summary": current_snapshot_summary,
                    "snapshots_overview": snapshots_overview # Use the formatted list
                },
                "key_metrics": {
                    "total_data_files": total_data_files,
                    "total_delete_files": total_delete_files, # Placeholder
                    "gross_records_in_data_files": gross_records_in_data_files, # Approximation
                    "approx_deleted_records_in_manifests": approx_deleted_records_in_manifests, # Placeholder
                    "approx_live_records": approx_live_records,
                    "metrics_note": metrics_note,
                    "total_data_storage_bytes": total_data_storage_bytes,
                    "total_delete_storage_bytes": total_delete_storage_bytes, # Placeholder
                    "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                    "avg_data_file_size_mb": round(avg_data_file_size_mb, 4),
                },
                "partition_explorer": partition_explorer_data,
                "sample_data": sample_data,
            }

            result_serializable = convert_bytes(result)
            end_time = time.time()
            print(f"--- Delta Request Completed in {end_time - start_time:.2f} seconds ---")
            return jsonify(result_serializable), 200

    except boto3.exceptions.NoCredentialsError:
        print("ERROR: AWS credentials not found.")
        return jsonify({"error": "AWS credentials not found."}), 401
    except s3_client.exceptions.NoSuchBucket as e:
        print(f"ERROR: S3 bucket access error: {e}")
        bucket_name_for_error = bucket_name if bucket_name else '<unknown>' # Use bucket_name if defined
        return jsonify({"error": f"S3 bucket not found or access denied: {e.response.get('Error',{}).get('BucketName', bucket_name_for_error)}"}), 404
    except s3_client.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        print(f"ERROR: AWS ClientError occurred: {error_code} - {e}")
        if error_code == 'AccessDenied':
            return jsonify({"error": f"Access Denied for S3 operation: {e}"}), 403
        elif error_code == 'NoSuchKey':
             # Check if delta_log_prefix is defined before using it in the error message
            log_prefix_info = f" ({delta_log_prefix})" if delta_log_prefix else ""
            return jsonify({"error": f"A required Delta log file was not found{log_prefix_info} (NoSuchKey): {e}"}), 404
        else:
            traceback.print_exc()
            return jsonify({"error": f"AWS ClientError: {e}"}), 500
    except FileNotFoundError as e: # This might be triggered by local file ops if S3 download fails somehow
        print(f"ERROR: FileNotFoundError occurred (likely local issue after download attempt): {e}")
        traceback.print_exc()
        return jsonify({"error": f"A required local file was not found during processing: {e}"}), 500 # Keep 500 as it's internal server state? Or 404 if it implies missing source data? Let's stick to 500.
    except ValueError as e: # Catch URL parsing errors etc.
         print(f"ERROR: ValueError occurred: {e}")
         traceback.print_exc()
         return jsonify({"error": f"Input value error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during Delta processing: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    

@app.route('/', methods=['GET'])
def hello():
    return """
    Hello! Available endpoints:
     - /Iceberg?s3_url=&lt;s3://your-bucket/path/to/iceberg-table/&gt;
     - /Delta?s3_url=&lt;s3://your-bucket/path/to/delta-table/&gt;
    """

if __name__ == '__main__':
    # Set host='0.0.0.0' to allow connections from other machines on the network
    # Use debug=True for development (auto-reloads, provides debugger)
    # Set debug=False for production deployments
    app.run(debug=True, host='0.0.0.0', port=5000)