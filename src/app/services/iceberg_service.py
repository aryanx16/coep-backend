# src/app/services/iceberg_service.py
import os
import json
import re
import time
import traceback
from urllib.parse import urlparse, unquote

# Import utilities from the 'utils' package
from ..utils.aws_s3 import extract_bucket_and_key, download_s3_file
from ..utils.formatters import format_bytes
from ..utils.parsers import parse_avro_file, read_parquet_sample
from ..utils.serializers import convert_bytes # Import if needed for internal structures, though final serialization happens in route

def get_iceberg_details(s3_url, s3_client, temp_dir):
    """
    Processes an Iceberg table at the given S3 URL and returns its details.

    Args:
        s3_url (str): The S3 URL pointing to the base of the Iceberg table.
        s3_client: An initialized Boto3 S3 client instance.
        temp_dir (str): Path to a temporary directory for downloads.

    Returns:
        dict: A dictionary containing Iceberg table details, ready for serialization.
              Includes 'error' key if processing fails.
    """
    start_time = time.time()
    print(f"\n--- Service: Processing Iceberg Request for {s3_url} ---")

    try:
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)
        # Ensure table base key ends with a slash for consistent prefix matching
        if not table_base_key.endswith('/') and table_base_key != "": table_base_key += '/'
        metadata_prefix = table_base_key + "metadata/"

        iceberg_manifest_files_info = [] # Store details about manifest list and manifest files

        # 1. Find the latest metadata.json file
        print(f"DEBUG: Listing objects under metadata prefix: s3://{bucket_name}/{metadata_prefix}")
        list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)

        if 'Contents' not in list_response:
            # Check if the base path exists at all
            try:
                s3_client.head_object(Bucket=bucket_name, Key=table_base_key) # Check if base exists
                # Base exists, but metadata folder is empty or non-existent
                return {"error": f"No metadata objects found under prefix: s3://{bucket_name}/{metadata_prefix}. Is it a valid Iceberg table?"}
            except s3_client.exceptions.ClientError as head_err:
                 if head_err.response['Error']['Code'] in ['404', 'NoSuchKey']:
                     return {"error": f"Base table path or metadata prefix not found: s3://{bucket_name}/{metadata_prefix}"}
                 else:
                     raise # Re-raise other head_object errors

        # Extract details for all metadata.json files found
        metadata_files = []
        s3_object_details = {obj['Key']: {'size': obj['Size'], 'last_modified': obj['LastModified']} for obj in list_response.get('Contents', [])}

        version_pattern = re.compile(r'(v?)(\d+)-[a-f0-9-]+\.metadata\.json$')
        for obj_key, obj_info in s3_object_details.items():
            filename = os.path.basename(obj_key)
            if filename.endswith('.metadata.json'):
                match = version_pattern.search(filename)
                version_num = 0
                if match:
                    try:
                        # Version might be prefixed with 'v'
                        version_num = int(match.group(2))
                    except (ValueError, IndexError):
                        print(f"Warning: Could not parse version from metadata file: {filename}")
                metadata_files.append({
                    'key': obj_key,
                    'version': version_num,
                    'last_modified': obj_info['last_modified'],
                    'size': obj_info['size']
                })

        if not metadata_files:
            return {"error": f"No *.metadata.json files found under s3://{bucket_name}/{metadata_prefix}"}

        # Sort by version number (desc) and then last modified (desc) to find the latest
        metadata_files.sort(key=lambda x: (x['version'], x['last_modified']), reverse=True)
        latest_metadata_file_info = metadata_files[0]
        latest_metadata_key = latest_metadata_file_info['key']
        print(f"INFO: Using latest metadata file: {latest_metadata_key} (Version: {latest_metadata_file_info['version']})")

        # Define local path for download
        safe_latest_meta_filename = os.path.basename(latest_metadata_key).replace('%', '_').replace(':', '_')
        local_latest_metadata_path = os.path.join(temp_dir, safe_latest_meta_filename)

        # 2. Download and parse the latest metadata.json
        download_s3_file(s3_client, bucket_name, latest_metadata_key, local_latest_metadata_path)
        with open(local_latest_metadata_path, 'r', encoding='utf-8') as f:
            latest_meta = json.load(f)

        # Extract key information from metadata.json
        table_uuid = latest_meta.get("table-uuid")
        current_snapshot_id = latest_meta.get("current-snapshot-id")
        snapshots = latest_meta.get("snapshots", [])
        current_schema_id = latest_meta.get("current-schema-id", 0) # Default to 0 if missing
        # Find the current schema object
        schema = next((s for s in latest_meta.get("schemas", []) if s.get("schema-id") == current_schema_id), latest_meta.get("schema")) # Fallback for older formats
        current_spec_id = latest_meta.get("current-spec-id", 0)
        # Find the current partition spec object
        partition_spec = next((s for s in latest_meta.get("partition-specs", []) if s.get("spec-id") == current_spec_id), latest_meta.get("partition-spec"))
        current_sort_order_id = latest_meta.get("current-sort-order-id", 0)
        # Find the current sort order object
        sort_order = next((s for s in latest_meta.get("sort-orders", []) if s.get("order-id") == current_sort_order_id), latest_meta.get("sort-order"))
        properties = latest_meta.get("properties", {})
        format_version = latest_meta.get("format-version", 1) # Default to V1 if missing
        snapshot_log = latest_meta.get("snapshot-log", []) # History of snapshot changes

        print(f"DEBUG: Table format version: {format_version}")
        print(f"DEBUG: Current Snapshot ID: {current_snapshot_id}")

        # Assemble Format Configuration section
        format_configuration = {
            "format-version": format_version,
            "table-uuid": table_uuid,
            "location": s3_url, # The input URL
            "properties": properties,
            "snapshot-log-available": bool(snapshot_log), # Indicate if snapshot log exists
        }

        # Handle case where table exists but has no snapshots yet
        if current_snapshot_id is None:
             print("INFO: Table metadata found, but no current snapshot exists.")
             return {
                 "message": "Table metadata found, but no current snapshot exists (table might be empty or newly created).",
                 "table_type": "Iceberg",
                 "table_uuid": table_uuid,
                 "location": s3_url,
                 "format_configuration": format_configuration,
                 "table_schema": schema,
                 "partition_spec": partition_spec or {"spec-id": 0, "fields": []}, # Provide default empty spec
                 "version_history": {"total_snapshots": len(snapshots), "snapshots_overview": snapshots[-5:]}, # Show recent snapshot history if any
                 # Add empty sections for consistency maybe?
                 "iceberg_manifest_files": [],
                 "key_metrics": {},
                 "partition_explorer": [],
                 "sample_data": []
             }

        # 3. Find the current snapshot details and the path to its manifest list
        current_snapshot = next((s for s in snapshots if s.get("snapshot-id") == current_snapshot_id), None)
        if not current_snapshot:
            return {"error": f"Current Snapshot ID {current_snapshot_id} referenced in metadata not found in snapshots list."}
        print(f"DEBUG: Current snapshot summary: {current_snapshot.get('summary', {})}")

        manifest_list_path = current_snapshot.get("manifest-list")
        if not manifest_list_path:
            return {"error": f"Manifest list path missing in current snapshot {current_snapshot_id}"}

        # Determine the absolute S3 path for the manifest list
        manifest_list_key = ""
        manifest_list_bucket = bucket_name # Default to table bucket
        try:
            parsed_manifest_list_url = urlparse(manifest_list_path)
            if parsed_manifest_list_url.scheme == "s3":
                # Absolute S3 path provided
                manifest_list_bucket, manifest_list_key = extract_bucket_and_key(manifest_list_path)
                if manifest_list_bucket != bucket_name:
                    print(f"Warning: Manifest list bucket '{manifest_list_bucket}' differs from table bucket '{bucket_name}'. Using manifest list bucket.")
            elif not parsed_manifest_list_url.scheme and parsed_manifest_list_url.path:
                # Relative path - resolve relative to the metadata file's directory
                relative_path = unquote(parsed_manifest_list_url.path)
                # Use normpath for cleaner path resolution, ensure forward slashes
                manifest_list_key = os.path.normpath(os.path.join(os.path.dirname(latest_metadata_key), relative_path)).replace("\\", "/")
                manifest_list_bucket = bucket_name # Relative paths use the table's bucket
            else:
                raise ValueError(f"Cannot parse manifest list path format: {manifest_list_path}")

            manifest_list_s3_uri = f"s3://{manifest_list_bucket}/{manifest_list_key}"

            # Get Manifest List Size and add to info list
            try:
                manifest_list_head = s3_client.head_object(Bucket=manifest_list_bucket, Key=manifest_list_key)
                manifest_list_size = manifest_list_head.get('ContentLength')
            except s3_client.exceptions.ClientError as head_err:
                print(f"Warning: Could not get size for manifest list {manifest_list_s3_uri}: {head_err}")
                manifest_list_size = None

            iceberg_manifest_files_info.append({
                "file_path": manifest_list_s3_uri,
                "size_bytes": manifest_list_size,
                "size_human": format_bytes(manifest_list_size),
                "type": "Manifest List"
            })

        except ValueError as e:
            return {"error": f"Error processing manifest list path '{manifest_list_path}': {e}"}

        # Define local path for the manifest list download
        safe_manifest_list_filename = os.path.basename(manifest_list_key).replace('%', '_').replace(':', '_')
        local_manifest_list_path = os.path.join(temp_dir, safe_manifest_list_filename)

        # 4. Download and parse the manifest list (which is an Avro file)
        download_s3_file(s3_client, manifest_list_bucket, manifest_list_key, local_manifest_list_path)
        manifest_list_entries = parse_avro_file(local_manifest_list_path)
        print(f"DEBUG: Number of manifest files listed: {len(manifest_list_entries)}")

        # 5. Process each Manifest File listed in the manifest list
        total_data_files, gross_records_in_data_files, total_delete_files, approx_deleted_records = 0, 0, 0, 0
        total_data_storage_bytes, total_delete_storage_bytes = 0, 0
        partition_stats = {}
        data_file_paths_sample = [] # Store info for one data file to sample later

        print("\nINFO: Processing Manifest Files...")
        for i, entry in enumerate(manifest_list_entries):
            manifest_file_path = entry.get("manifest_path") # Path to the manifest file (Avro)
            print(f"\nDEBUG: Manifest List Entry {i+1}/{len(manifest_list_entries)}: Path='{manifest_file_path}'")
            if not manifest_file_path:
                print(f"Warning: Skipping manifest list entry {i+1} due to missing 'manifest_path'.")
                continue

            manifest_file_key = ""
            manifest_bucket = bucket_name # Default unless path specifies otherwise
            manifest_file_s3_uri = "" # Store full S3 URI for reporting

            try:
                # Resolve the manifest file path (can be absolute S3 or relative)
                parsed_manifest_url = urlparse(manifest_file_path)
                if parsed_manifest_url.scheme == "s3":
                    m_bucket, manifest_file_key = extract_bucket_and_key(manifest_file_path)
                    manifest_bucket = m_bucket
                    manifest_file_s3_uri = manifest_file_path
                elif not parsed_manifest_url.scheme and parsed_manifest_url.path:
                    relative_path = unquote(parsed_manifest_url.path)
                    # Manifest paths are usually relative to the manifest list file's location
                    manifest_file_key = os.path.normpath(os.path.join(os.path.dirname(manifest_list_key), relative_path)).replace("\\", "/")
                    manifest_bucket = manifest_list_bucket # Usually same bucket as the list
                    manifest_file_s3_uri = f"s3://{manifest_bucket}/{manifest_file_key}"
                else:
                    raise ValueError("Cannot parse manifest file path format")

                # Get Manifest File Size and add to info list
                manifest_file_size = entry.get('manifest_length') # Size often included in manifest list
                if manifest_file_size is None: # Fallback to head_object if not present
                     try:
                         manifest_head = s3_client.head_object(Bucket=manifest_bucket, Key=manifest_file_key)
                         manifest_file_size = manifest_head.get('ContentLength')
                     except s3_client.exceptions.ClientError as head_err:
                         print(f"Warning: Could not get size for manifest file {manifest_file_s3_uri}: {head_err}")
                         manifest_file_size = None

                iceberg_manifest_files_info.append({
                     "file_path": manifest_file_s3_uri,
                     "size_bytes": manifest_file_size,
                     "size_human": format_bytes(manifest_file_size),
                     "type": "Manifest File"
                })

                # Download and parse the individual manifest file (Avro)
                safe_manifest_filename = f"manifest_{i}_" + os.path.basename(manifest_file_key).replace('%', '_').replace(':', '_')
                local_manifest_path = os.path.join(temp_dir, safe_manifest_filename)
                download_s3_file(s3_client, manifest_bucket, manifest_file_key, local_manifest_path)
                manifest_records = parse_avro_file(local_manifest_path) # Each record describes a data/delete file
                print(f"DEBUG: Processing {len(manifest_records)} entries in manifest: {os.path.basename(manifest_file_key)}")

                # --- Process entries within the current manifest file ---
                for j, manifest_entry in enumerate(manifest_records):
                    # Status indicates if the file described is active (EXISTING/ADDED) or removed (DELETED)
                    # 0: EXISTING, 1: ADDED, 2: DELETED (relative to the snapshot this manifest belongs to)
                    status = manifest_entry.get('status', 0)
                    if status == 2: continue # Skip files marked as DELETED in this manifest

                    record_count, file_size, file_path_in_manifest, partition_data_struct = 0, 0, "", None
                    content = 0 # 0: data, 1: position deletes, 2: equality deletes

                    # Extract fields based on Iceberg Format Version (V1 or V2)
                    if format_version >= 2:
                        # V2 format nests file info under 'data_file' or 'delete_file'
                        if 'data_file' in manifest_entry and manifest_entry['data_file'] is not None:
                            content = 0 # Data file
                            nested_info = manifest_entry['data_file']
                            record_count = nested_info.get("record_count", 0) or 0
                            file_size = nested_info.get("file_size_in_bytes", 0) or 0
                            file_path_in_manifest = nested_info.get("file_path", "")
                            partition_data_struct = nested_info.get("partition") # Partition data (struct format)
                            # Extract other V2 fields if needed (column sizes, value counts, etc.)
                        elif 'delete_file' in manifest_entry and manifest_entry['delete_file'] is not None:
                            nested_info = manifest_entry['delete_file']
                            content = nested_info.get("content", 1) # 1=position, 2=equality
                            record_count = nested_info.get("record_count", 0) or 0 # Count of deleted records/positions
                            file_size = nested_info.get("file_size_in_bytes", 0) or 0
                            file_path_in_manifest = nested_info.get("file_path", "")
                            # Delete files don't typically repeat partition info here
                        else:
                            print(f"Warning: Skipping manifest entry {j} in {manifest_file_key} - invalid V2 structure.")
                            continue # Skip invalid V2 entry
                    elif format_version == 1:
                        # V1 format has fields directly in the manifest entry
                        content = 0 # V1 only supports data files
                        record_count = manifest_entry.get("record_count", 0) or 0
                        file_size = manifest_entry.get("file_size_in_bytes", 0) or 0
                        file_path_in_manifest = manifest_entry.get("file_path", "")
                        partition_data_struct = manifest_entry.get("partition") # Partition data (tuple/list format)
                    else:
                        print(f"Warning: Skipping manifest entry {j} - unsupported format version {format_version}.")
                        continue # Skip unsupported format

                    if not file_path_in_manifest:
                        print(f"Warning: Skipping manifest entry {j} in {manifest_file_key} due to missing file path.")
                        continue

                    # Construct the Full S3 Path for the data/delete file
                    full_file_s3_path = ""
                    file_bucket = bucket_name # Default to table bucket
                    try:
                        parsed_file_path = urlparse(file_path_in_manifest)
                        if parsed_file_path.scheme == "s3":
                            # Absolute path provided in manifest
                            f_bucket, f_key = extract_bucket_and_key(file_path_in_manifest)
                            file_bucket = f_bucket
                            full_file_s3_path = file_path_in_manifest
                        elif not parsed_file_path.scheme and parsed_file_path.path:
                            # Relative path - resolve relative to the table's base data location
                            relative_data_path = unquote(parsed_file_path.path).lstrip('/')
                            # Assume relative paths are relative to the table's root directory
                            full_file_s3_path = f"s3://{bucket_name}/{table_base_key}{relative_data_path}"
                        else:
                             print(f"Warning: Cannot determine absolute path for file '{file_path_in_manifest}'. Skipping accumulation.")
                             continue # Skip accumulation if path cannot be determined

                    except ValueError as path_err:
                        print(f"Warning: Error parsing file path '{file_path_in_manifest}': {path_err}. Skipping accumulation.")
                        continue

                    # Accumulate statistics based on file content type
                    if content == 0: # Data File
                        total_data_files += 1
                        gross_records_in_data_files += record_count
                        total_data_storage_bytes += file_size

                        # --- Process Partition Info ---
                        partition_key_string = "<unpartitioned>"
                        partition_values_dict = {} # Store as key-value pairs

                        if partition_spec and partition_spec.get('fields') and partition_data_struct is not None:
                            try:
                                spec_fields = partition_spec['fields']
                                if format_version >= 2 and isinstance(partition_data_struct, dict):
                                     # V2 partition data is often a struct/dict { "col_name": value, ... }
                                     # We need to map it based on the spec field names
                                     partition_values_dict = {
                                         field['name']: partition_data_struct.get(field['name'])
                                         for field in spec_fields
                                         # Ensure we only include fields defined in the spec
                                         if field.get('name') in partition_data_struct
                                     }
                                elif format_version == 1 and isinstance(partition_data_struct, (list, tuple)):
                                    # V1 partition data is typically a tuple/list [val1, val2, ...]
                                    if len(partition_data_struct) == len(spec_fields):
                                        partition_values_dict = {
                                            spec_fields[idx]['name']: partition_data_struct[idx]
                                            for idx in range(len(spec_fields))
                                        }
                                    else:
                                        print(f"Warning: Partition data length mismatch for V1. Spec: {len(spec_fields)}, Data: {len(partition_data_struct)}. Storing raw.")
                                        partition_values_dict = {'_raw_v1_mismatch': str(partition_data_struct)}
                                else:
                                     # Handle unexpected partition data format
                                     print(f"Warning: Unexpected partition data format. Version: {format_version}, Data: {partition_data_struct}. Storing raw.")
                                     partition_values_dict = {'_raw_unexpected': str(partition_data_struct)}

                                # Create a canonical string representation for grouping stats
                                if partition_values_dict and not any(k.startswith('_raw') for k in partition_values_dict):
                                     partition_key_string = json.dumps(dict(sorted(partition_values_dict.items())), default=str)
                                elif partition_values_dict: # If raw data was stored
                                    partition_key_string = json.dumps(partition_values_dict, default=str)

                            except Exception as part_err:
                                print(f"Warning: Error processing partition data {partition_data_struct}: {part_err}")
                                partition_key_string = f"<error: {part_err}>"
                                partition_values_dict = {'_error': str(part_err)}
                        # else: Table is unpartitioned or partition data is missing

                        # Store stats grouped by partition key string
                        if partition_key_string not in partition_stats:
                             partition_stats[partition_key_string] = {
                                 "gross_record_count": 0,
                                 "size_bytes": 0,
                                 "num_data_files": 0,
                                 "partition_values": partition_values_dict # Store the parsed dict
                             }
                        partition_stats[partition_key_string]["gross_record_count"] += record_count
                        partition_stats[partition_key_string]["size_bytes"] += file_size
                        partition_stats[partition_key_string]["num_data_files"] += 1

                        # Store the path of the *first* suitable (Parquet) data file found for sampling
                        if full_file_s3_path and not data_file_paths_sample and full_file_s3_path.lower().endswith(".parquet"):
                            file_key_for_sample = urlparse(full_file_s3_path).path.lstrip('/')
                            data_file_paths_sample.append({'bucket': file_bucket, 'key': file_key_for_sample})
                            print(f"DEBUG: Selected potential sample file: s3://{file_bucket}/{file_key_for_sample}")

                    elif content == 1 or content == 2: # Delete File (Position or Equality)
                        total_delete_files += 1
                        approx_deleted_records += record_count # Number of records deleted by this file
                        total_delete_storage_bytes += file_size
                        # Note: Partition stats usually focus on data files, but you could add delete file stats too if needed.

            except Exception as manifest_err:
                 # Catch errors during processing of a single manifest file
                 print(f"ERROR: Failed to process manifest file {manifest_file_s3_uri}: {manifest_err}")
                 traceback.print_exc()
                 # Add info about the failed manifest file to the list
                 iceberg_manifest_files_info.append({
                     "file_path": manifest_file_s3_uri or manifest_file_path, # Use URI if available
                     "size_bytes": None, "size_human": "N/A",
                     "type": "Manifest File (Error Processing)"
                 })
                 # Continue processing other manifest files if possible
        # --- END Manifest File Loop ---
        print("INFO: Finished processing manifest files.")

        # 6. Get Sample Data (if a suitable Parquet file was identified)
        sample_data = []
        if data_file_paths_sample:
            sample_file_info = data_file_paths_sample[0]
            print(f"INFO: Attempting to get sample data from: s3://{sample_file_info['bucket']}/{sample_file_info['key']}")
            try:
                # Call the utility function to download and read the sample
                sample_data = read_parquet_sample(
                    s3_client,
                    sample_file_info['bucket'],
                    sample_file_info['key'],
                    temp_dir,
                    num_rows=10 # Get up to 10 rows
                )
            except Exception as sample_err:
                 # Catch any unexpected errors during sampling
                 print(f"ERROR: Failed during sample data retrieval: {sample_err}")
                 sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
        else:
             print("INFO: No suitable Parquet data file found in the current snapshot for sampling.")


        # 7. Assemble the final result dictionary
        print("\nINFO: Assembling final Iceberg result...")
        # Calculate derived metrics
        approx_live_records = max(0, gross_records_in_data_files - approx_deleted_records)
        avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
        avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024))

        # Format partition explorer data
        partition_explorer_data = []
        for k, v in partition_stats.items():
             partition_explorer_data.append({
                 "partition_values": v["partition_values"], # The dictionary of values
                 "partition_key_string": k, # The JSON string representation used for grouping
                 "gross_record_count": v["gross_record_count"],
                 "size_bytes": v["size_bytes"],
                 "size_human": format_bytes(v["size_bytes"]), # Add human-readable size
                 "num_data_files": v["num_data_files"]
             })
        # Sort partitions, e.g., alphabetically by string key
        partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))

        # Structure the final result
        result = {
            "table_type": "Iceberg",
            "table_uuid": table_uuid,
            "location": s3_url,
            "format_configuration": format_configuration, # Includes format-version, uuid, props etc.
            "iceberg_manifest_files": iceberg_manifest_files_info, # List of manifest/list files processed
            # -- Schema & Structure --
            "table_schema": schema,
            "partition_spec": partition_spec or {"spec-id": 0, "fields": []}, # Ensure it exists
            "sort_order": sort_order or {"order-id": 0, "fields": []}, # Ensure it exists
             # -- Versioning / History --
            "current_snapshot_id": current_snapshot_id,
            "version_history": {
                "total_snapshots": len(snapshots),
                # Provide summary of current snapshot directly
                "current_snapshot_summary": current_snapshot,
                 # Provide limited history (e.g., last 10 snapshots) - full list can be large
                 # Convert timestamps for better readability
                "snapshots_overview": [
                     {**snap, 'timestamp-ms': snap.get('timestamp-ms'), 'timestamp-iso': format_timestamp_ms(snap.get('timestamp-ms'))}
                     for snap in snapshots[-min(len(snapshots), 10):] # Get last 10 or fewer
                ]
             },
             # -- Key Metrics (Current Snapshot) --
             "key_metrics": {
                 "total_data_files": total_data_files,
                 "total_delete_files": total_delete_files,
                 "gross_records_in_data_files": gross_records_in_data_files,
                 "approx_deleted_records_in_manifests": approx_deleted_records,
                 "approx_live_records": approx_live_records,
                 "total_data_storage_bytes": total_data_storage_bytes,
                 "total_data_storage_human": format_bytes(total_data_storage_bytes), # Human readable total
                 "total_delete_storage_bytes": total_delete_storage_bytes,
                 "total_delete_storage_human": format_bytes(total_delete_storage_bytes), # Human readable delete total
                 "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                 "avg_data_file_size_mb": round(avg_data_file_size_mb, 4),
                 "metrics_note": ("Live record count is an approximation based on manifest metadata counts for data and delete files (if V2+). "
                                  "It may differ from query engine results. Partition record counts are gross counts from data files."),
            },
            # -- Partition Details --
            "partition_explorer": partition_explorer_data,
            # -- Sample Data --
            "sample_data": sample_data, # Contains list of dicts or error message
        }

        end_time = time.time()
        print(f"--- Service: Iceberg Request Completed in {end_time - start_time:.2f} seconds ---")
        # The final serialization (convert_bytes) should happen in the route just before jsonify
        return result

    except FileNotFoundError as e:
        # Catch file not found errors specifically (likely from S3 download)
        print(f"ERROR: Required file not found during Iceberg processing: {e}")
        return {"error": f"A required file was not found: {e}"}
    except ValueError as e:
        # Catch invalid input or format errors
        print(f"ERROR: Value error during Iceberg processing: {e}")
        return {"error": f"Input or value error: {str(e)}"}
    except Exception as e:
        # Catch all other unexpected errors
        print(f"ERROR: An unexpected error occurred during Iceberg processing: {e}")
        traceback.print_exc()
        return {"error": f"An unexpected server error occurred: {str(e)}"}