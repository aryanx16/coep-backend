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
    s3_client = None # Initialize

    try:
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)
        if not table_base_key.endswith('/'): table_base_key += '/'
        delta_log_prefix = table_base_key + "_delta_log/"
        print(f"INFO: Processing Delta Lake table at: s3://{bucket_name}/{table_base_key}")

        s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

        with tempfile.TemporaryDirectory(prefix="delta_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory: {temp_dir}")

            # --- 1. Find Delta Log Files ---
            # (Listing logic remains the same as before)
            log_files_raw = []
            continuation_token = None
            while True:
                list_kwargs = {'Bucket': bucket_name, 'Prefix': delta_log_prefix}
                if continuation_token: list_kwargs['ContinuationToken'] = continuation_token
                try: list_response = s3_client.list_objects_v2(**list_kwargs)
                except s3_client.exceptions.NoSuchKey: list_response = {}
                except s3_client.exceptions.ClientError as list_err:
                    print(f"ERROR: ClientError listing objects under {delta_log_prefix}: {list_err}")
                    return jsonify({"error": f"Error listing Delta log files: {list_err}"}), 500
                # Check for empty log on first fetch after ensuring base path exists (logic unchanged)
                if 'Contents' not in list_response and not log_files_raw:
                     try:
                         s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_base_key, Delimiter='/', MaxKeys=1)
                         return jsonify({"error": f"Delta log prefix '{delta_log_prefix}' is empty."}), 404
                     except s3_client.exceptions.ClientError as head_err: # Treat errors as path not found/accessible
                          return jsonify({"error": f"Base table path or Delta log not found/accessible: s3://{bucket_name}/{table_base_key}. Error: {head_err}"}), 404
                log_files_raw.extend(list_response.get('Contents', []))
                if list_response.get('IsTruncated'): continuation_token = list_response.get('NextContinuationToken')
                else: break
            print(f"DEBUG: Found {len(log_files_raw)} total objects under delta log prefix.")

            # --- Collect Metadata File Info ---
            # (File parsing logic remains the same)
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
                if filename == "_last_checkpoint" or json_pattern.match(filename) or checkpoint_pattern.match(filename):
                     delta_log_files_info.append({"file_path": f"s3://{bucket_name}/{key}", "relative_path": key.replace(table_base_key, "", 1), "size_bytes": size, "size_human": format_bytes(size)})
                # (Parsing _last_checkpoint, json commits, checkpoint files remains the same)
                if filename == "_last_checkpoint":
                    local_last_cp_path = os.path.join(temp_dir, "_last_checkpoint")
                    try:
                        download_s3_file(s3_client, bucket_name, key, local_last_cp_path)
                        with open(local_last_cp_path, 'r') as f: last_checkpoint_data = json.load(f)
                        last_checkpoint_info = {'version': last_checkpoint_data['version'], 'parts': last_checkpoint_data.get('parts'), 'key': key, 'size': size}
                    except Exception as cp_err: print(f"Warning: Failed to read/parse _last_checkpoint {key}: {cp_err}")
                elif (json_match := json_pattern.match(filename)): json_commits[int(json_match.group(1))] = {'key': key, 'last_modified': obj['LastModified'], 'size': size}
                elif (cp_match := checkpoint_pattern.match(filename)):
                    version, part_num, num_parts = int(cp_match.group(1)), cp_match.group(2), cp_match.group(3)
                    part_num = int(part_num) if part_num else 1
                    num_parts = int(num_parts) if num_parts else 1
                    if version not in checkpoint_files: checkpoint_files[version] = {'num_parts': num_parts, 'parts': {}}
                    checkpoint_files[version]['parts'][part_num] = {'key': key, 'last_modified': obj['LastModified'], 'size': size}
                    if checkpoint_files[version]['num_parts'] != num_parts and cp_match.group(2): # Handle multi-part naming inconsistency
                         checkpoint_files[version]['num_parts'] = max(checkpoint_files[version]['num_parts'], num_parts)

            delta_log_files_info.sort(key=lambda x: x.get('relative_path', ''))

            # Determine latest version ID (logic unchanged)
            current_snapshot_id = -1
            if json_commits: current_snapshot_id = max(json_commits.keys())
            elif last_checkpoint_info: current_snapshot_id = last_checkpoint_info['version']
            elif checkpoint_files:
                complete_cp_versions = [v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']]
                if complete_cp_versions: current_snapshot_id = max(complete_cp_versions)
            if current_snapshot_id == -1 and checkpoint_files: current_snapshot_id = max(checkpoint_files.keys()) # Fallback to incomplete
            if current_snapshot_id == -1: return jsonify({"error": "No Delta commit JSON files or checkpoint files found."}), 404
            print(f"INFO: Latest Delta version (snapshot ID) identified: {current_snapshot_id}")


            # --- 2/3. Process Checkpoint and JSON Commits Incrementally ---
            active_files = {}
            metadata_from_log = None
            protocol_from_log = None
            all_commit_info = {} # Store details per commit, INCLUDING cumulative metrics
            processed_versions = set() # Track processed versions to avoid duplicates

            # --- Determine starting point (Checkpoint or Version 0) ---
            start_process_version = 0
            checkpoint_version_used = None
            effective_checkpoint_version = -1
            # (Find effective checkpoint logic remains the same)
            cp_version_candidate = -1
            if last_checkpoint_info: cp_version_candidate = last_checkpoint_info['version']
            elif checkpoint_files:
                 available_complete_checkpoints = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']], reverse=True)
                 if available_complete_checkpoints: cp_version_candidate = available_complete_checkpoints[0]
            if cp_version_candidate > -1:
                 if cp_version_candidate in checkpoint_files and len(checkpoint_files[cp_version_candidate]['parts']) == checkpoint_files[cp_version_candidate]['num_parts']:
                      effective_checkpoint_version = cp_version_candidate
                 else: # Fallback logic for incomplete/missing candidate
                      available_complete_checkpoints = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts'] and v < cp_version_candidate], reverse=True)
                      if available_complete_checkpoints: effective_checkpoint_version = available_complete_checkpoints[0]
                      else: effective_checkpoint_version = -1

            # --- Load State from Checkpoint if applicable ---
            if effective_checkpoint_version > -1:
                print(f"INFO: Reading state from checkpoint version {effective_checkpoint_version}")
                checkpoint_version_used = effective_checkpoint_version
                cp_info = checkpoint_files[effective_checkpoint_version]
                try:
                    all_checkpoint_actions = []
                    for part_num in sorted(cp_info['parts'].keys()):
                        part_key = cp_info['parts'][part_num]['key']
                        all_checkpoint_actions.extend(read_delta_checkpoint(s3_client, bucket_name, part_key, temp_dir))

                    # Process actions from checkpoint to establish initial state
                    for action in all_checkpoint_actions:
                        if 'add' in action and action['add']:
                             add_info = action['add']
                             path = add_info['path']
                             stats_parsed = json.loads(add_info['stats']) if isinstance(add_info.get('stats'), str) else add_info.get('stats')
                             active_files[path] = { 'size': add_info.get('size'), 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': add_info.get('modificationTime', 0), 'stats': stats_parsed, 'tags': add_info.get('tags') }
                        elif 'metaData' in action and action['metaData']: metadata_from_log = action['metaData']
                        elif 'protocol' in action and action['protocol']: protocol_from_log = action['protocol']

                    start_process_version = effective_checkpoint_version + 1
                    processed_versions.add(effective_checkpoint_version) # Mark checkpoint version as processed base
                    print(f"INFO: Checkpoint processing complete. Starting JSON processing from version {start_process_version}")

                    # **Calculate and store state for the checkpoint version itself**
                    cp_total_files = len(active_files)
                    cp_total_bytes = sum(f['size'] for f in active_files.values() if f.get('size'))
                    cp_total_records = sum(int(f['stats']['numRecords']) for f in active_files.values() if f.get('stats') and 'numRecords' in f['stats'])
                    all_commit_info[effective_checkpoint_version] = {
                         'version': effective_checkpoint_version, 'timestamp': None, # Timestamp might be unknown
                         'operation': 'CHECKPOINT_LOAD', 'operationParameters': {},
                         'num_added_files': cp_total_files, 'num_removed_files': 0, # Treat CP load as adding all files
                         'added_bytes': cp_total_bytes, 'removed_bytes': 0,
                         'metrics': {'numOutputFiles': str(cp_total_files), 'numOutputBytes': str(cp_total_bytes)}, # Simulate metrics
                         # Store cumulative state at this version
                         'total_files_at_version': cp_total_files,
                         'total_bytes_at_version': cp_total_bytes,
                         'total_records_at_version': cp_total_records
                    }


                except Exception as cp_read_err:
                    print(f"ERROR: Failed to read/process checkpoint {effective_checkpoint_version}: {cp_read_err}. Falling back.")
                    active_files = {} # Reset state
                    metadata_from_log = None
                    protocol_from_log = None
                    start_process_version = 0
                    checkpoint_version_used = None
                    processed_versions = set() # Reset processed
                    all_commit_info = {} # Reset history
            else:
                print("INFO: No usable checkpoint found. Processing JSON logs from version 0.")
                start_process_version = 0

            # --- Process JSON Commits Incrementally ---
            versions_to_process = sorted([v for v in json_commits if v >= start_process_version])
            print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} up to {current_snapshot_id}...")

            removed_file_sizes_by_commit = {} # Track sizes for commits lacking metrics

            for version in versions_to_process:
                if version in processed_versions: continue # Should not happen with sorted list, but safety check
                commit_file_info = json_commits[version]
                commit_key = commit_file_info['key']
                print(f"DEBUG: Processing version {version} ({commit_key})...")
                removed_file_sizes_by_commit[version] = 0
                try:
                    actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
                    commit_summary = { # Store details specific to this commit's changes
                        'version': version, 'timestamp': None, 'operation': 'Unknown',
                        'num_actions': len(actions), 'operationParameters': {},
                        'num_added_files': 0, 'num_removed_files': 0,
                        'added_bytes': 0, 'removed_bytes': 0, 'metrics': {}
                    }
                    op_metrics = {}

                    for action in actions:
                        if 'commitInfo' in action and action['commitInfo']:
                            ci = action['commitInfo']
                            commit_summary['timestamp'] = ci.get('timestamp')
                            commit_summary['operation'] = ci.get('operation', 'Unknown')
                            commit_summary['operationParameters'] = ci.get('operationParameters', {})
                            op_metrics = ci.get('operationMetrics', {})
                            commit_summary['metrics'] = op_metrics
                            commit_summary['num_added_files'] = int(op_metrics.get('numOutputFiles', 0))
                            commit_summary['num_removed_files'] = int(op_metrics.get('numRemovedFiles', 0))
                            commit_summary['added_bytes'] = int(op_metrics.get('numOutputBytes', 0))
                            # Prefer Spark 3 metric, fallback, else calculate later
                            commit_summary['removed_bytes'] = int(op_metrics.get('numTargetFilesRemoved', op_metrics.get('numRemovedBytes', 0)))

                        elif 'add' in action and action['add']:
                            add_info = action['add']
                            path = add_info['path']
                            stats_parsed = json.loads(add_info['stats']) if isinstance(add_info.get('stats'), str) else add_info.get('stats')
                            active_files[path] = { 'size': add_info.get('size'), 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': add_info.get('modificationTime', 0), 'stats': stats_parsed, 'tags': add_info.get('tags') }
                            if 'numOutputFiles' not in op_metrics: commit_summary['num_added_files'] += 1
                            if 'numOutputBytes' not in op_metrics: commit_summary['added_bytes'] += add_info.get('size', 0)

                        elif 'remove' in action and action['remove']:
                            remove_info = action['remove']
                            path = remove_info['path']
                            if remove_info.get('dataChange', True):
                                removed_file_info = active_files.pop(path, None)
                                if 'numRemovedFiles' not in op_metrics and 'numTargetFilesRemoved' not in op_metrics:
                                    commit_summary['num_removed_files'] += 1
                                    if removed_file_info and removed_file_info.get('size'):
                                         removed_file_sizes_by_commit[version] += removed_file_info.get('size',0)

                        elif 'metaData' in action and action['metaData']: metadata_from_log = action['metaData']
                        elif 'protocol' in action and action['protocol']: protocol_from_log = action['protocol']

                    # Calculate removed bytes if metrics were missing
                    if commit_summary['removed_bytes'] == 0 and removed_file_sizes_by_commit[version] > 0:
                         commit_summary['removed_bytes'] = removed_file_sizes_by_commit[version]

                    # **Calculate and store cumulative state AFTER applying this version**
                    current_total_files = len(active_files)
                    current_total_bytes = sum(f['size'] for f in active_files.values() if f.get('size'))
                    current_total_records = sum(int(f['stats']['numRecords']) for f in active_files.values() if f.get('stats') and 'numRecords' in f['stats'])

                    commit_summary['total_files_at_version'] = current_total_files
                    commit_summary['total_bytes_at_version'] = current_total_bytes
                    commit_summary['total_records_at_version'] = current_total_records

                    all_commit_info[version] = commit_summary # Store all info
                    processed_versions.add(version)

                except Exception as json_proc_err:
                    print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
                    traceback.print_exc()
                    all_commit_info[version] = {'version': version, 'error': str(json_proc_err)}
                    processed_versions.add(version) # Mark as processed even if error occurred

            print(f"INFO: Finished incremental processing.")

            # --- 3.5 Find Definitive Metadata & Protocol ---
            # (Backward search logic remains the same, needed if latest version had error or logs missing)
            definitive_metadata = metadata_from_log
            definitive_protocol = protocol_from_log
            if not definitive_metadata or not definitive_protocol:
                 print("DEBUG: Searching backwards for definitive metadata/protocol...")
                 # Need to search from latest *successfully processed* or latest known
                 search_start = current_snapshot_id
                 while search_start > -1 and (search_start not in all_commit_info or all_commit_info[search_start].get('error')):
                     search_start -= 1
                 if search_start == -1 and checkpoint_version_used is not None: # Handle checkpoint-only case
                     search_start = checkpoint_version_used

                 processed_backward = set()
                 for v in range(search_start, -1, -1):
                     if definitive_metadata and definitive_protocol: break
                     if v in processed_versions and v >= start_process_version: continue # Skip if processed forward successfully
                     if v not in json_commits and v != checkpoint_version_used: continue

                     actions_to_check = []
                     if v in json_commits and v not in processed_backward:
                          try: actions_to_check = read_delta_json_lines(s3_client, bucket_name, json_commits[v]['key'], temp_dir); processed_backward.add(v)
                          except Exception: pass # Ignore errors during backward search
                     elif v == checkpoint_version_used and 'all_checkpoint_actions' in locals(): # Check if checkpoint actions were loaded
                          actions_to_check = all_checkpoint_actions

                     for action in reversed(actions_to_check):
                         if not definitive_metadata and 'metaData' in action and action['metaData']: definitive_metadata = action['metaData']; print(f"DEBUG: Found definitive metaData in version {v}")
                         if not definitive_protocol and 'protocol' in action and action['protocol']: definitive_protocol = action['protocol']; print(f"DEBUG: Found definitive protocol in version {v}")
                         if definitive_metadata and definitive_protocol: break

            # Handle missing metadata/protocol (logic unchanged)
            if not definitive_metadata: return jsonify({"error": "Could not determine table metadata."}), 500
            if not definitive_protocol: definitive_protocol = {"minReaderVersion": 1, "minWriterVersion": 2}

            # --- Assemble Format Configuration, Parse Schema, Partition Spec ---
            # (Logic remains the same)
            format_configuration = {**definitive_protocol, **(definitive_metadata.get("configuration", {}))}
            table_schema = _parse_delta_schema_string(definitive_metadata.get("schemaString", "{}"))
            if not table_schema: return jsonify({"error": "Failed to parse table schema."}), 500
            partition_cols = definitive_metadata.get("partitionColumns", [])
            partition_spec_fields = []
            schema_fields_map = {f['name']: f for f in table_schema.get('fields', [])}
            for i, col_name in enumerate(partition_cols):
                 source_field = schema_fields_map.get(col_name)
                 if source_field: partition_spec_fields.append({ "name": col_name, "transform": "identity", "source-id": source_field.get('id', i+1), "field-id": 1000 + i })
            partition_spec = {"spec-id": 0, "fields": partition_spec_fields}


            # --- 4. Calculate FINAL State Metrics (for latest version) ---
            # Get metrics from the stored info for the current snapshot id
            final_commit_info = all_commit_info.get(current_snapshot_id, {})
            total_data_files = final_commit_info.get('total_files_at_version', 0)
            total_data_storage_bytes = final_commit_info.get('total_bytes_at_version', 0)
            approx_live_records = final_commit_info.get('total_records_at_version', 0)
            gross_records_in_data_files = approx_live_records # Same estimate for Delta

            # Other metrics remain 0 for Delta
            total_delete_files = 0
            total_delete_storage_bytes = 0
            approx_deleted_records_in_manifests = 0

            # Averages based on final state
            avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
            avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024))

            # Metrics Note (Update slightly if needed)
            metrics_note = f"Live record count ({approx_live_records}) is estimated based on 'numRecords' in file stats. Delete file/record metrics common in Iceberg V2 are not tracked in Delta metadata."


            # --- 5. Calculate Partition Stats (based on FINAL active_files) ---
            # (Logic remains the same, uses the final state of active_files)
            partition_stats = {}
            for path, file_info in active_files.items():
                 part_values = file_info.get('partitionValues', {})
                 part_key_string = json.dumps(dict(sorted(part_values.items())), default=str) if part_values else "<unpartitioned>"
                 if part_key_string not in partition_stats: partition_stats[part_key_string] = { "partition_values": part_values, "partition_key_string": part_key_string, "num_data_files": 0, "size_bytes": 0, "gross_record_count": 0 }
                 partition_stats[part_key_string]["num_data_files"] += 1
                 partition_stats[part_key_string]["size_bytes"] += file_info.get('size', 0)
                 if file_info.get('stats') and 'numRecords' in file_info['stats']:
                      try: partition_stats[part_key_string]["gross_record_count"] += int(file_info['stats']['numRecords'])
                      except (ValueError, TypeError): pass
            partition_explorer_data = list(partition_stats.values())
            for p_data in partition_explorer_data: p_data["size_human"] = format_bytes(p_data["size_bytes"])
            partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))


            # --- 6. Get Sample Data (based on FINAL active_files) ---
            # (Logic remains the same)
            sample_data = []
            if active_files:
                 # Find sample file (prefer parquet)
                 sample_file_path = next((p for p in active_files if p.lower().endswith('.parquet')), list(active_files.keys())[0] if active_files else None)
                 if sample_file_path:
                      full_sample_s3_key = table_base_key.rstrip('/') + '/' + sample_file_path.lstrip('/')
                      print(f"INFO: Attempting sample from: s3://{bucket_name}/{full_sample_s3_key}")
                      try:
                          if sample_file_path.lower().endswith('.parquet'):
                               sample_data = read_parquet_sample(s3_client, bucket_name, full_sample_s3_key, temp_dir, num_rows=10)
                          else: sample_data = [{"error": "Sampling only implemented for Parquet"}]
                      except Exception as sample_err: sample_data = [{"error": "Failed to read sample data", "details": str(sample_err)}]
                 else: sample_data = [{"error": "No active files found for sampling"}]


            # --- 7. Assemble Final Result ---
            print("\nINFO: Assembling final Delta result...")

            # --- Assemble Current Snapshot Details (use final calculated metrics) ---
            current_commit_summary_changes = all_commit_info.get(current_snapshot_id, {}) # Changes made by this commit
            current_snapshot_details = {
                 "version": current_snapshot_id,
                 "timestamp_ms": current_commit_summary_changes.get('timestamp'),
                 "timestamp_iso": format_timestamp_ms(current_commit_summary_changes.get('timestamp')),
                 "operation": current_commit_summary_changes.get('operation', 'N/A'),
                 "operation_parameters": current_commit_summary_changes.get('operationParameters', {}),
                 # Total state metrics for THIS snapshot
                 "num_files_total_snapshot": total_data_files,
                 "total_data_files_snapshot": total_data_files,
                 "total_delete_files_snapshot": total_delete_files, # 0
                 "total_data_storage_bytes_snapshot": total_data_storage_bytes,
                 "total_records_snapshot": approx_live_records,
                 # Metrics specific to the COMMIT that CREATED this snapshot
                 "num_added_files_commit": current_commit_summary_changes.get('num_added_files'),
                 "num_removed_files_commit": current_commit_summary_changes.get('num_removed_files'),
                 "commit_added_bytes": current_commit_summary_changes.get('added_bytes'),
                 "commit_removed_bytes": current_commit_summary_changes.get('removed_bytes'),
                 "commit_metrics_raw": current_commit_summary_changes.get('metrics', {}),
                 "error": current_commit_summary_changes.get('error')
            }

            # --- Assemble Version History (Using stored cumulative metrics) ---
            snapshots_overview = []
            known_versions = sorted(list(processed_versions), reverse=True) # Use versions actually processed
            history_limit = 20 # Limit displayed history
            versions_in_history = known_versions[:min(len(known_versions), history_limit)]
            current_snapshot_summary_for_history = None

            for v in versions_in_history:
                 commit_details = all_commit_info.get(v)
                 if not commit_details: continue # Skip if details missing (shouldn't happen often)

                 summary = {}
                 if commit_details.get('error'):
                      summary = {"error": commit_details.get('error')}
                 else:
                      summary = {
                          "operation": commit_details.get('operation', 'Unknown'),
                          "added-data-files": str(commit_details.get('num_added_files', 'N/A')),
                          "removed-data-files": str(commit_details.get('num_removed_files', 'N/A')),
                          "added-files-size": str(commit_details.get('added_bytes', 'N/A')),
                          "removed-files-size": str(commit_details.get('removed_bytes', 'N/A')),
                          "operation-parameters": commit_details.get('operationParameters', {}),
                          # --- Add the stored cumulative totals for this version ---
                          "total-data-files": str(commit_details.get('total_files_at_version', 'N/A')),
                          "total-files-size": str(commit_details.get('total_bytes_at_version', 'N/A')),
                          "total-records": str(commit_details.get('total_records_at_version', 'N/A')),
                          # --- Add Delta-specific zero/NA fields for consistency ---
                          "total-delete-files": "0",
                          "total-equality-deletes": "0",
                          "total-position-deletes": "0",
                      }

                 snapshot_entry = {
                     "snapshot-id": v,
                     "timestamp-ms": commit_details.get('timestamp'),
                     "summary": summary
                 }
                 snapshots_overview.append(snapshot_entry)

                 # Store the summary for the current version separately
                 if v == current_snapshot_id:
                      current_snapshot_summary_for_history = snapshot_entry


            # --- Final Result Structure ---
            result = {
                "table_type": "Delta",
                "table_uuid": definitive_metadata.get("id"),
                "location": s3_url if s3_url else f"s3://{bucket_name}/{table_base_key}",
                "format_configuration": format_configuration,
                "format_version": definitive_protocol.get('minReaderVersion', 1),
                "delta_log_files": delta_log_files_info,
                "current_snapshot_id": current_snapshot_id,
                "current_snapshot_details": current_snapshot_details,
                "table_schema": table_schema,
                "table_properties": definitive_metadata.get("configuration", {}),
                "partition_spec": partition_spec,
                "sort_order": {"order-id": 0, "fields": []},
                "version_history": {
                    "total_snapshots": len(known_versions), # Count versions we have info for
                    "current_snapshot_summary": current_snapshot_summary_for_history,
                    "snapshots_overview": snapshots_overview
                },
                "key_metrics": {
                    "total_data_files": total_data_files,
                    "total_delete_files": total_delete_files,
                    "total_data_storage_bytes": total_data_storage_bytes,
                    "total_data_storage_human": format_bytes(total_data_storage_bytes),
                    "total_delete_storage_bytes": total_delete_storage_bytes,
                    "total_delete_storage_human": format_bytes(total_delete_storage_bytes),
                    "gross_records_in_data_files": gross_records_in_data_files,
                    "approx_deleted_records_in_manifests": approx_deleted_records_in_manifests,
                    "approx_live_records": approx_live_records,
                    "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                    "avg_data_file_size_mb": round(avg_data_file_size_mb, 4),
                    "metrics_note": metrics_note,
                },
                "partition_explorer": partition_explorer_data,
                "sample_data": sample_data,
            }

            # Convert bytes and handle non-serializable types before returning
            result_serializable = json.loads(json.dumps(convert_bytes(result), default=str)) # Use json dump/load with default=str as final safety

            end_time = time.time()
            print(f"--- Delta Request Completed in {end_time - start_time:.2f} seconds ---")
            return jsonify(result_serializable), 200

    # --- Exception Handling (Keep As Is) ---
    except boto3.exceptions.NoCredentialsError:
        return jsonify({"error": "AWS credentials not found."}), 401
    except (s3_client.exceptions.NoSuchBucket if s3_client else Exception) as e: # Check if s3_client exists
        # Simplified bucket not found handling
         return jsonify({"error": f"S3 bucket not found or access denied: {bucket_name}"}), 404
    except (s3_client.exceptions.ClientError if s3_client else Exception) as e:
        # Simplified client error handling
         error_code = e.response.get('Error', {}).get('Code', 'Unknown') if hasattr(e, 'response') else 'Unknown'
         print(f"ERROR: AWS ClientError: {error_code} - {e}")
         return jsonify({"error": f"AWS ClientError ({error_code}): Check logs for details."}), 500
    except FileNotFoundError as e:
         print(f"ERROR: FileNotFoundError: {e}")
         return jsonify({"error": f"Required file not found: {e}"}), 404
    except ValueError as e:
       print(f"ERROR: ValueError: {e}")
       return jsonify({"error": f"Input value error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
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