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
        # Divide by 1000 to convert ms to seconds
        dt_object = datetime.datetime.fromtimestamp(int(timestamp_ms) / 1000, tz=datetime.timezone.utc)
        return dt_object.isoformat().replace('+00:00', 'Z') # Standard ISO 8601 format
    except (ValueError, TypeError):
        return str(timestamp_ms) # Return original value if conversion fails

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
    # data_prefix = table_base_key + "data/" # Used for constructing sample data paths if relative

    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

    with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
        print(f"DEBUG: Using temporary directory: {temp_dir}")

        # --- NEW: List to store manifest file info ---
        iceberg_manifest_files_info = []

        # 1. Find latest metadata.json
        list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
        if 'Contents' not in list_response: return jsonify({"error": f"No objects found under metadata prefix: {metadata_prefix}"}), 404

        metadata_files = []
        # --- Store sizes from listing ---
        s3_object_details = {obj['Key']: {'size': obj['Size'], 'last_modified': obj['LastModified']} for obj in list_response.get('Contents', [])}

        for obj_key, obj_info in s3_object_details.items():
            if obj_key.endswith('.metadata.json'):
                match = re.search(r'(v?\d+)-[a-f0-9-]+\.metadata\.json$', os.path.basename(obj_key))
                version_num = 0
                if match:
                    try: version_num = int(match.group(1).lstrip('v'))
                    except ValueError: pass
                metadata_files.append({'key': obj_key, 'version': version_num, 'last_modified': obj_info['last_modified'], 'size': obj_info['size']}) # Store size

        if not metadata_files: return jsonify({"error": f"No *.metadata.json files found under {metadata_prefix}"}), 404

        metadata_files.sort(key=lambda x: (x['version'], x['last_modified']), reverse=True)
        latest_metadata_key = metadata_files[0]['key']
        latest_metadata_size = metadata_files[0]['size'] # Get size
        print(f"INFO: Using latest metadata file: {latest_metadata_key}")
        local_latest_metadata_path = os.path.join(temp_dir, os.path.basename(latest_metadata_key).replace('%', '_'))

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
        snapshot_log = latest_meta.get("snapshot-log", []) # Get snapshot log if present

        print(f"DEBUG: Table format version: {format_version}")
        print(f"DEBUG: Current Snapshot ID: {current_snapshot_id}")

        # --- NEW: Assemble Format Configuration ---
        format_configuration = {
            "format-version": format_version,
            "table-uuid": table_uuid,
            "location": s3_url,
            "properties": properties, # Include table properties here as well/instead?
            "snapshot-log": snapshot_log, # Add snapshot-log
            # Add other relevant top-level metadata if desired
        }

        if current_snapshot_id is None:
             return jsonify({
                 "message": "Table metadata found, but no current snapshot exists (table might be empty or in intermediate state).",
                 "table_uuid": table_uuid, "location": s3_url,
                 "format_configuration": format_configuration, # Add here
                 "table_schema": schema,
                 "partition_spec": partition_spec,
                 "version_history": {"total_snapshots": len(snapshots), "snapshots_overview": snapshots[-5:]}
             }), 200

        # 3. Find current snapshot & manifest list
        current_snapshot = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
        if not current_snapshot: return jsonify({"error": f"Current Snapshot ID {current_snapshot_id} referenced in metadata not found in snapshots list."}), 404
        print(f"DEBUG: Current snapshot summary: {current_snapshot.get('summary', {})}")

        manifest_list_path = current_snapshot.get("manifest-list")
        if not manifest_list_path: return jsonify({"error": f"Manifest list path missing in current snapshot {current_snapshot_id}"}), 404

        manifest_list_key = ""
        manifest_list_bucket = bucket_name # Default to table bucket
        try:
            parsed_manifest_list_url = urlparse(manifest_list_path)
            if parsed_manifest_list_url.scheme == "s3":
                manifest_list_bucket, manifest_list_key = extract_bucket_and_key(manifest_list_path)
                if manifest_list_bucket != bucket_name:
                    print(f"Warning: Manifest list bucket '{manifest_list_bucket}' differs from table bucket '{bucket_name}'. Using manifest list bucket.")
            elif not parsed_manifest_list_url.scheme and parsed_manifest_list_url.path:
                relative_path = unquote(parsed_manifest_list_url.path)
                manifest_list_key = os.path.normpath(os.path.join(os.path.dirname(latest_metadata_key), relative_path)).replace("\\", "/")
                manifest_list_bucket = bucket_name # Relative path uses table's bucket
            else: raise ValueError(f"Cannot parse manifest list path format: {manifest_list_path}")

            # --- NEW: Get Manifest List Size ---
            try:
                manifest_list_head = s3_client.head_object(Bucket=manifest_list_bucket, Key=manifest_list_key)
                manifest_list_size = manifest_list_head.get('ContentLength')
                iceberg_manifest_files_info.append({
                    "file_path": f"s3://{manifest_list_bucket}/{manifest_list_key}",
                    "size_bytes": manifest_list_size,
                    "size_human": format_bytes(manifest_list_size),
                    "type": "Manifest List"
                })
            except s3_client.exceptions.ClientError as head_err:
                print(f"Warning: Could not get size for manifest list {manifest_list_key}: {head_err}")
                iceberg_manifest_files_info.append({
                    "file_path": f"s3://{manifest_list_bucket}/{manifest_list_key}",
                    "size_bytes": None,
                    "size_human": "N/A",
                    "type": "Manifest List (Error getting size)"
                })

        except ValueError as e: return jsonify({"error": f"Error processing manifest list path '{manifest_list_path}': {e}"}), 400
        local_manifest_list_path = os.path.join(temp_dir, os.path.basename(manifest_list_key).replace('%', '_'))

        # 4. Download and parse manifest list (Avro)
        download_s3_file(s3_client, manifest_list_bucket, manifest_list_key, local_manifest_list_path)
        manifest_list_entries = parse_avro_file(local_manifest_list_path)
        print(f"DEBUG: Number of manifest files listed: {len(manifest_list_entries)}")

        # 5. Process Manifest Files (Avro)
        total_data_files, gross_records_in_data_files, total_delete_files, approx_deleted_records = 0, 0, 0, 0
        total_data_storage_bytes, total_delete_storage_bytes = 0, 0
        partition_stats = {}
        data_file_paths_sample = []

        print("\nINFO: Processing Manifest Files...")
        for i, entry in enumerate(manifest_list_entries):
            manifest_file_path = entry.get("manifest_path")
            print(f"\nDEBUG: Manifest List Entry {i+1}/{len(manifest_list_entries)}: Path='{manifest_file_path}'")
            if not manifest_file_path:
                print(f"Warning: Skipping manifest list entry {i+1} due to missing 'manifest_path'.")
                continue

            manifest_file_key = ""
            manifest_bucket = bucket_name # Default to table bucket unless path specifies otherwise
            manifest_file_s3_uri = "" # Store full S3 URI for reporting

            try:
                parsed_manifest_url = urlparse(manifest_file_path)
                if parsed_manifest_url.scheme == "s3":
                    m_bucket, manifest_file_key = extract_bucket_and_key(manifest_file_path)
                    manifest_bucket = m_bucket # Use the bucket specified in the manifest path
                    manifest_file_s3_uri = manifest_file_path
                elif not parsed_manifest_url.scheme and parsed_manifest_url.path:
                    relative_path = unquote(parsed_manifest_url.path)
                    # Manifest paths are relative to the manifest list file's location
                    manifest_file_key = os.path.normpath(os.path.join(os.path.dirname(manifest_list_key), relative_path)).replace("\\", "/")
                    manifest_bucket = manifest_list_bucket # Manifests are usually in the same bucket as the list
                    manifest_file_s3_uri = f"s3://{manifest_bucket}/{manifest_file_key}"
                else: raise ValueError("Cannot parse manifest file path format")

                # --- NEW: Get Manifest File Size ---
                manifest_file_size = entry.get('manifest_length') # Often included in manifest list entry
                if manifest_file_size is None: # Fallback to head_object if not in entry
                     try:
                         manifest_head = s3_client.head_object(Bucket=manifest_bucket, Key=manifest_file_key)
                         manifest_file_size = manifest_head.get('ContentLength')
                     except s3_client.exceptions.ClientError as head_err:
                         print(f"Warning: Could not get size for manifest file {manifest_file_key}: {head_err}")
                         manifest_file_size = None

                iceberg_manifest_files_info.append({
                     "file_path": manifest_file_s3_uri,
                     "size_bytes": manifest_file_size,
                     "size_human": format_bytes(manifest_file_size),
                     "type": "Manifest File"
                })


                local_manifest_path = os.path.join(temp_dir, f"manifest_{i}_" + os.path.basename(manifest_file_key).replace('%', '_'))
                download_s3_file(s3_client, manifest_bucket, manifest_file_key, local_manifest_path)
                manifest_records = parse_avro_file(local_manifest_path)
                print(f"DEBUG: Processing {len(manifest_records)} entries in manifest: {os.path.basename(manifest_file_key)}")

                # --- Existing logic to process manifest_records ---
                # ...(keep the loop processing manifest_records as it was)
                for j, manifest_entry in enumerate(manifest_records):
                    status = manifest_entry.get('status', 0) # 0:EXISTING, 1:ADDED, 2:DELETED
                    if status == 2: continue # Skip DELETED entries in the manifest file itself

                    record_count, file_size, file_path_in_manifest, partition_data = 0, 0, "", None
                    content = 0 # 0: data, 1: position deletes, 2: equality deletes

                    # Extract fields based on Format Version (V1 or V2)
                    # ...(keep existing V1/V2 field extraction logic)...
                    if format_version == 2:
                        if 'data_file' in manifest_entry and manifest_entry['data_file'] is not None:
                            content = 0
                            nested_info = manifest_entry['data_file']
                            record_count = nested_info.get("record_count", 0) or 0
                            file_size = nested_info.get("file_size_in_bytes", 0) or 0
                            file_path_in_manifest = nested_info.get("file_path", "")
                            partition_data = nested_info.get("partition")
                        elif 'delete_file' in manifest_entry and manifest_entry['delete_file'] is not None:
                            nested_info = manifest_entry['delete_file']
                            content = nested_info.get("content", 1)
                            record_count = nested_info.get("record_count", 0) or 0
                            file_size = nested_info.get("file_size_in_bytes", 0) or 0
                            file_path_in_manifest = nested_info.get("file_path", "")
                        else: continue # Skip invalid entry
                    elif format_version == 1:
                        content = 0
                        record_count = manifest_entry.get("record_count", 0) or 0
                        file_size = manifest_entry.get("file_size_in_bytes", 0) or 0
                        file_path_in_manifest = manifest_entry.get("file_path", "")
                        partition_data = manifest_entry.get("partition")
                    else: continue # Skip unsupported format

                    # Construct Full S3 Path for the data/delete file
                    # ...(keep existing path construction logic)...
                    full_file_s3_path = ""
                    file_bucket = bucket_name # Default to table bucket
                    try:
                        parsed_file_path = urlparse(file_path_in_manifest)
                        if parsed_file_path.scheme == "s3":
                            f_bucket, f_key = extract_bucket_and_key(file_path_in_manifest)
                            file_bucket = f_bucket
                            full_file_s3_path = file_path_in_manifest
                        elif not parsed_file_path.scheme and parsed_file_path.path:
                            relative_data_path = unquote(parsed_file_path.path).lstrip('/')
                            # Path is relative to the table's base location
                            full_file_s3_path = f"s3://{bucket_name}/{table_base_key.rstrip('/')}/{relative_data_path}"
                        else: continue # Skip accumulation if path cannot be determined
                    except ValueError as path_err:
                        print(f"Warning: Error parsing file path '{file_path_in_manifest}': {path_err}. Skipping accumulation.")
                        continue


                    # Accumulate stats
                    # ...(keep existing accumulation logic)...
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
                                else:
                                    partition_values_repr = {'_raw': str(partition_data)}
                                partition_key_string = json.dumps(dict(sorted(partition_values_repr.items())), default=str)
                            except Exception as part_err:
                                print(f"Warning: Error processing partition data {partition_data}: {part_err}")
                                partition_key_string = f"<error: {part_err}>"
                                partition_values_repr = {'_error': str(part_err)}
                        elif partition_data is None and partition_spec and not partition_spec.get('fields'):
                            partition_key_string = "<unpartitioned>"
                        elif partition_data is not None:
                            partition_key_string = str(partition_data)
                            partition_values_repr = {'_raw': partition_data}

                        if partition_key_string not in partition_stats: partition_stats[partition_key_string] = {"gross_record_count": 0, "size_bytes": 0, "num_data_files": 0, "partition_values": partition_values_repr}
                        partition_stats[partition_key_string]["gross_record_count"] += record_count
                        partition_stats[partition_key_string]["size_bytes"] += file_size
                        partition_stats[partition_key_string]["num_data_files"] += 1

                        if full_file_s3_path and len(data_file_paths_sample) < 1 and full_file_s3_path.lower().endswith(".parquet"):
                            data_file_paths_sample.append({'bucket': file_bucket, 'key': urlparse(full_file_s3_path).path.lstrip('/')})

                    elif content == 1 or content == 2: # Delete File
                        total_delete_files += 1
                        approx_deleted_records += record_count
                        total_delete_storage_bytes += file_size

            except Exception as manifest_err:
                 print(f"ERROR: Failed to process manifest file {manifest_file_key} from bucket {manifest_bucket}: {manifest_err}")
                 traceback.print_exc()
                 # Add info about the failed manifest file
                 iceberg_manifest_files_info.append({
                     "file_path": manifest_file_s3_uri or manifest_file_path, # Use URI if available
                     "size_bytes": None,
                     "size_human": "N/A",
                     "type": "Manifest File (Error Processing)"
                 })
        # --- END Manifest File Loop ---
        print("INFO: Finished processing manifest files.")

        # 6. Get Sample Data (if a suitable file was found)
        # ...(keep existing sample data logic)...
        sample_data = []
        if data_file_paths_sample:
            sample_file_info = data_file_paths_sample[0]
            print(f"INFO: Attempting to get sample data from: s3://{sample_file_info['bucket']}/{sample_file_info['key']}")
            try:
                sample_data = read_parquet_sample(s3_client, sample_file_info['bucket'], sample_file_info['key'], temp_dir, num_rows=10)
            except Exception as sample_err:
                sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
        else: print("INFO: No suitable Parquet data file found in manifests for sampling.")


        # 7. Assemble the final result
        print("\nINFO: Assembling final Iceberg result...")
        approx_live_records = max(0, gross_records_in_data_files - approx_deleted_records)
        avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
        avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024))

        partition_explorer_data = []
        for k, v in partition_stats.items():
              partition_explorer_data.append({
                  "partition_values": v["partition_values"],
                  "partition_key_string": k,
                  "gross_record_count": v["gross_record_count"],
                  "size_bytes": v["size_bytes"],
                  "size_human": format_bytes(v["size_bytes"]), # Add human readable size
                  "num_data_files": v["num_data_files"]
              })
        partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))

        # Final Result Structure
        result = {
            "table_type": "Iceberg", # Add type identifier
            "table_uuid": table_uuid,
            "location": s3_url,
            # --- NEW SECTIONS ---
            "format_configuration": format_configuration,
            "iceberg_manifest_files": iceberg_manifest_files_info, # Changed name for clarity
            # --- EXISTING SECTIONS (potentially keep or restructure) ---
            "format_version": format_version, # Redundant? Already in format_configuration
            "current_snapshot_id": current_snapshot_id, # Redundant? Already in current_snapshot_details
            "table_schema": schema,
            "partition_spec": partition_spec,
            "sort_order": sort_order,
            "version_history": {
                "total_snapshots": len(snapshots),
                "current_snapshot_summary": current_snapshot, # Redundant? Partially in current_snapshot_details
                "snapshots_overview": snapshots # Limit this? snapshots[-min(len(snapshots), 10):]
            },
            "key_metrics": {
                "total_data_files": total_data_files,
                "total_delete_files": total_delete_files,
                "gross_records_in_data_files": gross_records_in_data_files,
                "approx_deleted_records_in_manifests": approx_deleted_records,
                "approx_live_records": approx_live_records,
                "metrics_note": "Live record count is an approximation based on manifest metadata counts for data and delete files. It may differ from query engine results. Partition record counts are gross counts from data files.",
                "total_data_storage_bytes": total_data_storage_bytes,
                "total_delete_storage_bytes": total_delete_storage_bytes,
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
            log_files_raw = []
            continuation_token = None
            print(f"DEBUG: Listing objects under {delta_log_prefix}")
            while True:
                list_kwargs = {'Bucket': bucket_name, 'Prefix': delta_log_prefix}
                if continuation_token: list_kwargs['ContinuationToken'] = continuation_token
                try:
                    list_response = s3_client.list_objects_v2(**list_kwargs)
                except s3_client.exceptions.NoSuchKey: list_response = {}
                except s3_client.exceptions.ClientError as list_err:
                     print(f"ERROR: ClientError listing objects under {delta_log_prefix}: {list_err}")
                     return jsonify({"error": f"Error listing Delta log files: {list_err}"}), 500

                if 'Contents' not in list_response and not log_files_raw:
                    try:
                        # Check if base prefix exists (list_objects_v2 with delimiter might be better)
                        s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_base_key, Delimiter='/', MaxKeys=1)
                        # If base exists but log is empty:
                        return jsonify({"error": f"Delta log prefix '{delta_log_prefix}' found but is empty. May not be a valid Delta table or is empty."}), 404
                    except s3_client.exceptions.ClientError as head_err:
                         error_code = head_err.response.get('Error', {}).get('Code')
                         if error_code == '404' or error_code == 'NoSuchKey':
                              return jsonify({"error": f"Base table path s3://{bucket_name}/{table_base_key} or Delta log prefix '{delta_log_prefix}' not found."}), 404
                         elif error_code == 'AccessDenied':
                              return jsonify({"error": f"Access Denied for S3 path s3://{bucket_name}/{table_base_key} or log. Check permissions."}), 403
                         else:
                              return jsonify({"error": f"Cannot access S3 path s3://{bucket_name}/{table_base_key}. Check path and permissions. Error: {head_err}"}), 404

                log_files_raw.extend(list_response.get('Contents', []))
                if list_response.get('IsTruncated'): continuation_token = list_response.get('NextContinuationToken')
                else: break
            print(f"DEBUG: Found {len(log_files_raw)} total objects under delta log prefix.")

            # --- Collect Metadata File Info ---
            delta_log_files_info = []
            json_commits = {}
            checkpoint_files = {}
            last_checkpoint_info = None

            json_pattern = re.compile(r"(\d+)\.json$")
            checkpoint_pattern = re.compile(r"(\d+)\.checkpoint(?:\.(\d+)\.(\d+))?\.parquet$")

            for obj in log_files_raw:
                key = obj['Key']
                filename = os.path.basename(key)
                size = obj.get('Size')

                # Add relevant files to info list
                if filename == "_last_checkpoint" or json_pattern.match(filename) or checkpoint_pattern.match(filename):
                     delta_log_files_info.append({
                         "file_path": f"s3://{bucket_name}/{key}",
                         "relative_path": key.replace(table_base_key, "", 1),
                         "size_bytes": size,
                         "size_human": format_bytes(size)
                     })

                if filename == "_last_checkpoint":
                    local_last_cp_path = os.path.join(temp_dir, "_last_checkpoint")
                    try:
                        download_s3_file(s3_client, bucket_name, key, local_last_cp_path)
                        with open(local_last_cp_path, 'r') as f:
                            last_checkpoint_data = json.load(f)
                            last_checkpoint_info = {
                                'version': last_checkpoint_data['version'],
                                'parts': last_checkpoint_data.get('parts'),
                                'key': key, 'size': size
                            }
                            print(f"DEBUG: Found _last_checkpoint pointing to version {last_checkpoint_info['version']} (parts: {last_checkpoint_info['parts']})")
                    except Exception as cp_err:
                        print(f"Warning: Failed to read or parse _last_checkpoint file {key}: {cp_err}")
                    continue

                json_match = json_pattern.match(filename)
                if json_match:
                    version = int(json_match.group(1))
                    json_commits[version] = {'key': key, 'last_modified': obj['LastModified'], 'size': size}
                    continue

                cp_match = checkpoint_pattern.match(filename)
                if cp_match:
                    version = int(cp_match.group(1))
                    part_num = int(cp_match.group(2)) if cp_match.group(2) else 1
                    num_parts = int(cp_match.group(3)) if cp_match.group(3) else 1
                    if version not in checkpoint_files: checkpoint_files[version] = {'num_parts': num_parts, 'parts': {}}
                    checkpoint_files[version]['parts'][part_num] = {'key': key, 'last_modified': obj['LastModified'], 'size': size}
                    if checkpoint_files[version]['num_parts'] != num_parts and cp_match.group(2):
                        print(f"Warning: Inconsistent number of parts detected for checkpoint version {version}.")
                        checkpoint_files[version]['num_parts'] = max(checkpoint_files[version]['num_parts'], num_parts)

            delta_log_files_info.sort(key=lambda x: x.get('relative_path', ''))

            # Determine latest version ID
            current_snapshot_id = -1
            if json_commits:
                current_snapshot_id = max(json_commits.keys())
            elif last_checkpoint_info:
                current_snapshot_id = last_checkpoint_info['version']
            elif checkpoint_files:
                # Find the highest version number among complete checkpoints
                complete_cp_versions = [v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']]
                if complete_cp_versions:
                     current_snapshot_id = max(complete_cp_versions)

            if current_snapshot_id == -1:
                 # If still -1, check for incomplete checkpoints
                 if checkpoint_files:
                     current_snapshot_id = max(checkpoint_files.keys()) # Use latest known, even if incomplete

            if current_snapshot_id == -1:
                 return jsonify({"error": "No Delta commit JSON files or checkpoint files found. Cannot determine table version."}), 404

            print(f"INFO: Latest Delta version (snapshot ID) identified: {current_snapshot_id}")

            # --- 2. Determine State Reconstruction Strategy ---
            active_files = {}
            metadata_from_log = None
            protocol_from_log = None
            start_process_version = 0
            checkpoint_version_used = None
            all_checkpoint_actions = [] # Store checkpoint actions if loaded
            effective_checkpoint_version = -1

            # Find effective checkpoint version (logic remains the same)
            cp_version_candidate = -1
            if last_checkpoint_info:
                cp_version_candidate = last_checkpoint_info['version']
            elif checkpoint_files:
                 available_complete_checkpoints = sorted([
                     v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']
                 ], reverse=True)
                 if available_complete_checkpoints:
                     cp_version_candidate = available_complete_checkpoints[0]

            if cp_version_candidate > -1:
                # Check if candidate is usable (exists and complete)
                if cp_version_candidate in checkpoint_files and \
                   len(checkpoint_files[cp_version_candidate]['parts']) == checkpoint_files[cp_version_candidate]['num_parts']:
                    effective_checkpoint_version = cp_version_candidate
                    # Optionally adjust log message based on _last_checkpoint consistency
                else:
                     # Fallback to find latest complete checkpoint *before* candidate
                     available_complete_checkpoints = sorted([
                         v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts'] and v < cp_version_candidate
                     ], reverse=True)
                     if available_complete_checkpoints:
                         effective_checkpoint_version = available_complete_checkpoints[0]
                         print(f"INFO: Using latest complete checkpoint found at version {effective_checkpoint_version} (candidate {cp_version_candidate} was incomplete/missing).")
                     else: effective_checkpoint_version = -1 # No usable earlier checkpoint

            # Process Checkpoint if found
            if effective_checkpoint_version > -1:
                print(f"INFO: Reading state from checkpoint version {effective_checkpoint_version}")
                checkpoint_version_used = effective_checkpoint_version
                cp_info = checkpoint_files[effective_checkpoint_version]
                try:
                    for part_num in sorted(cp_info['parts'].keys()):
                        part_key = cp_info['parts'][part_num]['key']
                        all_checkpoint_actions.extend(read_delta_checkpoint(s3_client, bucket_name, part_key, temp_dir))

                    # Process actions from checkpoint
                    for action in all_checkpoint_actions:
                        if 'add' in action and action['add']:
                            add_info = action['add']
                            path = add_info['path']
                            mod_time = add_info.get('modificationTime', 0)
                            stats_parsed = None
                            if add_info.get('stats'):
                                try: stats_parsed = json.loads(add_info['stats'])
                                except: pass
                            active_files[path] = { 'size': add_info.get('size'), 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': mod_time, 'stats': stats_parsed, 'tags': add_info.get('tags') }
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
                    active_files = {}
                    metadata_from_log = None
                    protocol_from_log = None
                    start_process_version = 0
                    checkpoint_version_used = None
                    all_checkpoint_actions = [] # Clear actions if fallback
            else:
                print("INFO: No usable checkpoint found or chosen. Processing JSON logs from version 0.")
                start_process_version = 0

            # --- 3. Process JSON Commits ---
            versions_to_process = sorted([v for v in json_commits if v >= start_process_version])
            if not versions_to_process and checkpoint_version_used is None and not active_files:
                 return jsonify({"error": "No JSON commits found to process and no checkpoint loaded."}), 500
            elif not versions_to_process and checkpoint_version_used is not None:
                 print("INFO: No JSON files found after the checkpoint. State is as per checkpoint.")

            print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} to {current_snapshot_id}...")
            all_commit_info = {} # Store details per commit

            # Needed to track removed file sizes if not in commitInfo
            removed_file_sizes_by_commit = {}

            for version in versions_to_process:
                commit_file_info = json_commits[version]
                commit_key = commit_file_info['key']
                print(f"DEBUG: Processing version {version} ({commit_key})...")
                removed_file_sizes_by_commit[version] = 0
                try:
                    actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
                    commit_summary_details = {
                        'version': version, 'timestamp': None, 'operation': 'Unknown',
                        'num_actions': len(actions), 'operationParameters': {},
                        'num_added_files': 0, 'num_removed_files': 0,
                        'added_bytes': 0, 'removed_bytes': 0,
                        'metrics': {} # Store raw op metrics
                    }
                    op_metrics = {} # Parsed metrics from commitInfo

                    for action in actions:
                         if 'commitInfo' in action and action['commitInfo']:
                             ci = action['commitInfo']
                             commit_summary_details['timestamp'] = ci.get('timestamp')
                             commit_summary_details['operation'] = ci.get('operation', 'Unknown')
                             commit_summary_details['operationParameters'] = ci.get('operationParameters', {})
                             op_metrics = ci.get('operationMetrics', {})
                             commit_summary_details['metrics'] = op_metrics # Store raw metrics

                             # Extract metrics if available, default to 0
                             commit_summary_details['num_added_files'] = int(op_metrics.get('numOutputFiles', 0))
                             commit_summary_details['num_removed_files'] = int(op_metrics.get('numRemovedFiles', 0))
                             commit_summary_details['added_bytes'] = int(op_metrics.get('numOutputBytes', 0))
                             commit_summary_details['removed_bytes'] = int(op_metrics.get('numTargetFilesRemoved', 0)) # Use Spark 3+ metric if available for removes


                         elif 'add' in action and action['add']:
                             add_info = action['add']
                             path = add_info['path']
                             mod_time = add_info.get('modificationTime', 0)
                             stats_parsed = None
                             if add_info.get('stats'):
                                 try: stats_parsed = json.loads(add_info['stats'])
                                 except: pass
                             active_files[path] = { 'size': add_info.get('size'), 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': mod_time, 'stats': stats_parsed, 'tags': add_info.get('tags') }
                             # Increment only if not already counted by commitInfo metrics
                             if 'numOutputFiles' not in op_metrics: commit_summary_details['num_added_files'] += 1
                             if 'numOutputBytes' not in op_metrics: commit_summary_details['added_bytes'] += add_info.get('size', 0)

                         elif 'remove' in action and action['remove']:
                             remove_info = action['remove']
                             path = remove_info['path']
                             if remove_info.get('dataChange', True):
                                 removed_file_info = active_files.pop(path, None) # Remove from active state
                                 # Increment only if not already counted by commitInfo metrics
                                 if 'numRemovedFiles' not in op_metrics and 'numTargetFilesRemoved' not in op_metrics:
                                     commit_summary_details['num_removed_files'] += 1
                                     if removed_file_info and removed_file_info.get('size'):
                                          removed_file_sizes_by_commit[version] += removed_file_info.get('size',0)


                         elif 'metaData' in action and action['metaData']:
                             metadata_from_log = action['metaData']
                             print(f"DEBUG: Updated metaData from version {version}")
                         elif 'protocol' in action and action['protocol']:
                             protocol_from_log = action['protocol']
                             print(f"DEBUG: Updated protocol from version {version}")

                    # If remove size wasn't in commitInfo, use the sum calculated from 'remove' actions
                    if 'numTargetBytesRemoved' not in op_metrics and removed_file_sizes_by_commit[version] > 0 :
                         commit_summary_details['removed_bytes'] = removed_file_sizes_by_commit[version]
                    # Handle older metric name if present and preferred one isn't
                    elif 'numRemovedBytes' in op_metrics and 'numTargetBytesRemoved' not in op_metrics:
                         commit_summary_details['removed_bytes'] = int(op_metrics.get('numRemovedBytes', 0))


                    all_commit_info[version] = commit_summary_details

                except Exception as json_proc_err:
                    print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
                    traceback.print_exc()
                    all_commit_info[version] = {'version': version, 'error': str(json_proc_err)}

            print(f"INFO: Finished processing JSON logs.")

            # --- 3.5 Find Definitive Metadata & Protocol ---
            # Logic remains the same - search backwards if needed
            definitive_metadata = metadata_from_log
            definitive_protocol = protocol_from_log

            if not definitive_metadata or not definitive_protocol:
                print("DEBUG: Searching backwards for definitive metadata and/or protocol...")
                start_search_version = current_snapshot_id
                processed_backward = set() # Avoid reprocessing files

                for v in range(start_search_version, -1, -1):
                    if definitive_metadata and definitive_protocol: break
                    if v in versions_to_process: continue # Already checked
                    if v not in json_commits and v != checkpoint_version_used: continue # No file

                    actions_to_check = []
                    if v in json_commits and v not in processed_backward:
                         commit_key = json_commits[v]['key']
                         try:
                             actions_to_check = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
                             processed_backward.add(v)
                         except Exception as bk_err: print(f"Warning: Error reading version {v} JSON backwards: {bk_err}")
                    elif v == checkpoint_version_used and all_checkpoint_actions:
                         actions_to_check = all_checkpoint_actions # Use already loaded checkpoint actions

                    for action in reversed(actions_to_check): # Check in reverse order within file
                        if not definitive_metadata and 'metaData' in action and action['metaData']:
                            definitive_metadata = action['metaData']
                            print(f"DEBUG: Found definitive metaData in version {v}")
                        if not definitive_protocol and 'protocol' in action and action['protocol']:
                            definitive_protocol = action['protocol']
                            print(f"DEBUG: Found definitive protocol in version {v}")
                        if definitive_metadata and definitive_protocol: break


            if not definitive_metadata:
                return jsonify({"error": "Could not determine table metadata (schema, partitioning). Invalid Delta table state."}), 500
            if not definitive_protocol:
                print("Warning: Could not find protocol information. Using defaults.")
                definitive_protocol = {"minReaderVersion": 1, "minWriterVersion": 2}

            # --- Assemble Format Configuration ---
            format_configuration = {
                 **definitive_protocol,
                 **(definitive_metadata.get("configuration", {}))
            }

            # --- 3.6 Parse Schema and Format Partition Spec ---
            # Logic remains the same
            table_schema = _parse_delta_schema_string(definitive_metadata.get("schemaString", "{}"))
            if not table_schema: return jsonify({"error": "Failed to parse table schema from metadata."}), 500
            partition_cols = definitive_metadata.get("partitionColumns", [])
            partition_spec_fields = []
            schema_fields_map = {f['name']: f for f in table_schema.get('fields', [])}
            for i, col_name in enumerate(partition_cols):
                source_field = schema_fields_map.get(col_name)
                if source_field:
                    partition_spec_fields.append({ "name": col_name, "transform": "identity", "source-id": source_field.get('id', i+1), "field-id": 1000 + i }) # Use generated ID if needed
                else: print(f"Warning: Partition column '{col_name}' not found in table schema.")
            partition_spec = {"spec-id": 0, "fields": partition_spec_fields}


            # --- 4. Calculate Final State Metrics ---
            # Logic remains the same
            total_data_files = len(active_files)
            total_delete_files = 0 # Delta specific
            total_data_storage_bytes = sum(f['size'] for f in active_files.values() if f.get('size'))
            total_delete_storage_bytes = 0 # Delta specific

            approx_live_records = 0
            gross_records_in_data_files = 0
            files_missing_stats = 0
            for f in active_files.values():
                num_recs = 0
                has_stats = False
                if f.get('stats') and 'numRecords' in f['stats']:
                    try:
                         num_recs = int(f['stats']['numRecords'])
                         has_stats = True
                         approx_live_records += num_recs # Live records estimate
                    except (ValueError, TypeError): files_missing_stats += 1
                else:
                    files_missing_stats += 1
                # Gross records include counts even if stats are partially missing/unparseable
                gross_records_in_data_files += num_recs if has_stats else 0

            # Note: approx_live_records == gross_records_in_data_files with this logic for Delta
            # as we don't have separate delete file record counts.
            approx_deleted_records_in_manifests = 0 # Delta specific

            avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
            avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024))

            metrics_note = f"Live record count ({approx_live_records}) is an estimate based on available 'numRecords' in Delta file stats."
            if files_missing_stats > 0: metrics_note += f" Stats were missing or unparseable for {files_missing_stats}/{total_data_files} active files."
            metrics_note += " Delta Lake does not track explicit delete files/records in metadata like Iceberg V2 (delete file counts/bytes are 0)."

            # --- 5. Calculate Partition Stats ---
            # Logic remains the same
            partition_stats = {}
            for path, file_info in active_files.items():
                part_values = file_info.get('partitionValues', {})
                part_key_string = json.dumps(dict(sorted(part_values.items())), default=str) if part_values else "<unpartitioned>"
                if part_key_string not in partition_stats:
                    partition_stats[part_key_string] = { "partition_values": part_values, "partition_key_string": part_key_string, "num_data_files": 0, "size_bytes": 0, "gross_record_count": 0 }
                partition_stats[part_key_string]["num_data_files"] += 1
                partition_stats[part_key_string]["size_bytes"] += file_info.get('size', 0)
                if file_info.get('stats') and 'numRecords' in file_info['stats']:
                     try: partition_stats[part_key_string]["gross_record_count"] += int(file_info['stats']['numRecords'])
                     except (ValueError, TypeError): pass

            partition_explorer_data = list(partition_stats.values())
            for p_data in partition_explorer_data:
                p_data["size_human"] = format_bytes(p_data["size_bytes"])
            partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))


            # --- 6. Get Sample Data ---
            # Logic remains the same
            sample_data = []
            if active_files:
                # Try to find a parquet file for sampling
                sample_file_relative_path = None
                for p in active_files.keys():
                    if p.lower().endswith('.parquet'): # Prefer parquet
                         sample_file_relative_path = p
                         break
                if not sample_file_relative_path: # Fallback to first file if no parquet
                    sample_file_relative_path = list(active_files.keys())[0]

                full_sample_s3_key = table_base_key.rstrip('/') + '/' + sample_file_relative_path.lstrip('/')
                print(f"INFO: Attempting to get sample data from: s3://{bucket_name}/{full_sample_s3_key}")
                try:
                     if sample_file_relative_path.lower().endswith('.parquet'):
                         sample_data = read_parquet_sample(s3_client, bucket_name, full_sample_s3_key, temp_dir, num_rows=10)
                     else:
                          sample_data = [{"error": "Sampling only implemented for Parquet files", "file_type": os.path.splitext(sample_file_relative_path)[1]}]
                except FileNotFoundError:
                     sample_data = [{"error": f"Sample file not found", "details": f"s3://{bucket_name}/{full_sample_s3_key}"}]
                except Exception as sample_err:
                    print(f"ERROR: Sampling failed - {sample_err}")
                    sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
            else: print("INFO: No active data files found to sample.")


            # --- 7. Assemble Final Result ---
            print("\nINFO: Assembling final Delta result...")

            # --- Assemble Current Snapshot Details (Adding total state metrics) ---
            current_commit_summary = all_commit_info.get(current_snapshot_id, {})
            current_snapshot_details = {
                 "version": current_snapshot_id, # Or snapshot-id
                 "timestamp_ms": current_commit_summary.get('timestamp'),
                 "timestamp_iso": format_timestamp_ms(current_commit_summary.get('timestamp')),
                 "operation": current_commit_summary.get('operation', 'N/A'),
                 "operation_parameters": current_commit_summary.get('operationParameters', {}),
                 # Total state metrics for THIS snapshot
                 "num_files_total_snapshot": total_data_files, # Total active files in this snapshot
                 "total_data_files_snapshot": total_data_files, # Alias for consistency
                 "total_delete_files_snapshot": total_delete_files, # Always 0 for Delta
                 "total_data_storage_bytes_snapshot": total_data_storage_bytes,
                 "total_records_snapshot": approx_live_records, # Estimated total records
                 # Metrics specific to the COMMIT that CREATED this snapshot
                 "num_added_files_commit": current_commit_summary.get('num_added_files'),
                 "num_removed_files_commit": current_commit_summary.get('num_removed_files'),
                 "commit_added_bytes": current_commit_summary.get('added_bytes'),
                 "commit_removed_bytes": current_commit_summary.get('removed_bytes'),
                 "commit_metrics_raw": current_commit_summary.get('metrics', {}), # Include raw op metrics
                 "error": current_commit_summary.get('error')
            }

            # --- Assemble Version History ---
            snapshots_overview = []
            # Combine known versions from JSON and Checkpoints, sort descending
            known_versions = sorted(list(set(list(json_commits.keys()) + ([checkpoint_version_used] if checkpoint_version_used is not None else []))), reverse=True)

            # Limit history length for performance/readability
            history_limit = 20
            versions_in_history = known_versions[:min(len(known_versions), history_limit)]

            current_snapshot_summary_for_history = None # Store the enhanced summary for the latest snapshot

            for v in versions_in_history:
                commit_details = all_commit_info.get(v)
                summary = {}

                if commit_details and not commit_details.get('error'):
                    summary = {
                        "operation": commit_details.get('operation', 'Unknown'),
                        "added-data-files": str(commit_details.get('num_added_files', 'N/A')),
                        "removed-data-files": str(commit_details.get('num_removed_files', 'N/A')),
                        "added-files-size": str(commit_details.get('added_bytes', 'N/A')),
                        "removed-files-size": str(commit_details.get('removed_bytes', 'N/A')), # Added removed size
                        "operation-parameters": commit_details.get('operationParameters', {})
                        # Add raw metrics if desired:
                        # "metrics": commit_details.get('metrics', {})
                    }
                    # --- ADD TOTALS FOR THE *CURRENT* SNAPSHOT SUMMARY ONLY ---
                    if v == current_snapshot_id:
                         summary["total-data-files"] = str(total_data_files)
                         summary["total-delete-files"] = str(total_delete_files) # 0
                         summary["total-equality-deletes"] = "0" # N/A for Delta
                         summary["total-position-deletes"] = "0" # N/A for Delta
                         summary["total-files-size"] = str(total_data_storage_bytes)
                         summary["total-records"] = str(approx_live_records) # Best estimate
                         current_snapshot_summary_for_history = { # Create the separate summary object
                              "snapshot-id": v,
                              "timestamp-ms": commit_details.get('timestamp'),
                              "summary": summary.copy() # Use a copy of the enhanced summary
                         }

                elif commit_details and commit_details.get('error'):
                     summary = {"error": commit_details.get('error')}
                elif v == checkpoint_version_used and not commit_details: # Checkpoint only load
                     summary = {"operation": "CHECKPOINT_LOAD", "info": f"State loaded from checkpoint {v}"}
                     # Add totals if this checkpoint *is* the latest version
                     if v == current_snapshot_id:
                           summary["total-data-files"] = str(total_data_files)
                           summary["total-delete-files"] = str(total_delete_files) # 0
                           summary["total-equality-deletes"] = "0"
                           summary["total-position-deletes"] = "0"
                           summary["total-files-size"] = str(total_data_storage_bytes)
                           summary["total-records"] = str(approx_live_records)
                           current_snapshot_summary_for_history = {
                               "snapshot-id": v,
                               "timestamp-ms": None, # Timestamp might be unknown if only CP
                               "summary": summary.copy()
                           }
                else: # Should not happen often if all known versions are processed
                     summary = {"operation": "Unknown", "info": "Commit details missing or not processed"}

                # Don't add totals to historical summaries
                if v != current_snapshot_id:
                     # Optionally add placeholders or note lack of totals for historical
                     summary["total-data-files"] = "N/A (Historical)"
                     summary["total-files-size"] = "N/A (Historical)"
                     summary["total-records"] = "N/A (Historical)"
                     # Add other total fields as "0" or "N/A" for clarity
                     summary["total-delete-files"] = "0"
                     summary["total-equality-deletes"] = "0"
                     summary["total-position-deletes"] = "0"


                snapshots_overview.append({
                    "snapshot-id": v, # Use 'snapshot-id' for consistency
                    "timestamp-ms": commit_details.get('timestamp') if commit_details else None,
                    "summary": summary
                })


            # Final Result Structure
            result = {
                "table_type": "Delta",
                "table_uuid": definitive_metadata.get("id"),
                "location": s3_url, # Use the input URL maybe? Or construct canonical?
                # "canonical_location": f"s3://{bucket_name}/{table_base_key}", # Optional: more precise location

                "format_configuration": format_configuration, # Reader/Writer versions, table props
                "format_version": definitive_protocol.get('minReaderVersion', 1), # Keep for potential direct access

                "delta_log_files": delta_log_files_info, # List of log/checkpoint files found

                "current_snapshot_id": current_snapshot_id, # Top level ID
                "current_snapshot_details": current_snapshot_details, # Detailed info about latest snapshot

                "table_schema": table_schema,
                "table_properties": definitive_metadata.get("configuration", {}), # Keep for direct access

                "partition_spec": partition_spec,
                "sort_order": {"order-id": 0, "fields": []}, # Placeholder for Delta

                "version_history": {
                    "total_snapshots": len(known_versions), # Total versions found
                    "current_snapshot_summary": current_snapshot_summary_for_history, # Use the enhanced one created earlier
                    "snapshots_overview": snapshots_overview # List of recent snapshots
                },
                "key_metrics": {
                    # Totals for the CURRENT state
                    "total_data_files": total_data_files,
                    "total_delete_files": total_delete_files, # 0
                    "total_data_storage_bytes": total_data_storage_bytes,
                    "total_delete_storage_bytes": total_delete_storage_bytes, # 0
                    # Record Counts (Estimates for Delta)
                    "gross_records_in_data_files": gross_records_in_data_files, # Sum of numRecords from stats
                    "approx_deleted_records_in_manifests": approx_deleted_records_in_manifests, # 0
                    "approx_live_records": approx_live_records, # Best estimate of live records
                    # Averages
                    "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                    "avg_data_file_size_mb": round(avg_data_file_size_mb, 4),
                    # Note explaining estimations/differences
                    "metrics_note": metrics_note,
                },
                "partition_explorer": partition_explorer_data, # Stats per partition
                "sample_data": sample_data, # Sample rows from one data file
            }

            # Convert any remaining bytes objects (e.g., in sample data if errors occur)
            result_serializable = convert_bytes(result)

            end_time = time.time()
            print(f"--- Delta Request Completed in {end_time - start_time:.2f} seconds ---")
            return jsonify(result_serializable), 200

    # --- Exception Handling (Keep Existing) ---
    except boto3.exceptions.NoCredentialsError:
        print("ERROR: AWS credentials not found.")
        return jsonify({"error": "AWS credentials not found."}), 401
    except s3_client.exceptions.NoSuchBucket as e:
        print(f"ERROR: S3 bucket access error: {e}")
        bucket_name_for_error = bucket_name if bucket_name else '<unknown>'
        return jsonify({"error": f"S3 bucket not found or access denied: {e.response.get('Error',{}).get('BucketName', bucket_name_for_error)}"}), 404
    except s3_client.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        print(f"ERROR: AWS ClientError occurred: {error_code} - {e}")
        if error_code == 'AccessDenied':
            return jsonify({"error": f"Access Denied for S3 operation: {e}"}), 403
        elif error_code == 'NoSuchKey' or error_code == '404':
            log_prefix_info = f" ({delta_log_prefix})" if delta_log_prefix else ""
            if 'download_s3_file' in traceback.format_exc() or 'read_parquet_sample' in traceback.format_exc():
                 return jsonify({"error": f"A required Delta log/data file was not found{log_prefix_info} (NoSuchKey/404 during download/sampling): {e}"}), 404
            else:
                 return jsonify({"error": f"Delta log prefix or required file not found{log_prefix_info} (NoSuchKey/404): {e}"}), 404
        else:
            traceback.print_exc()
            return jsonify({"error": f"AWS ClientError: {e}"}), 500
    except FileNotFoundError as e: # Can be raised by download_s3_file or local file ops
        print(f"ERROR: FileNotFoundError occurred: {e}")
        traceback.print_exc()
        # Distinguish between S3 not found and local issue if possible
        if "S3 object not found" in str(e):
             return jsonify({"error": f"A required S3 file was not found: {e}"}), 404
        else:
             return jsonify({"error": f"A required local file was not found during processing: {e}"}), 500
    except ValueError as e:
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