# src/app/services/delta_service.py
import os
import json
import re
import time
import traceback
from urllib.parse import urlparse, unquote

# Import utilities
from ..utils.aws_s3 import extract_bucket_and_key, download_s3_file
from ..utils.formatters import format_bytes, format_timestamp_ms
from ..utils.parsers import read_delta_checkpoint, read_delta_json_lines, read_parquet_sample
from ..utils.serializers import convert_bytes # Import if needed

# Import internal delta utils
from ._delta_utils import _parse_delta_schema_string


def get_delta_details(s3_url, s3_client, temp_dir):
    """
    Processes a Delta Lake table at the given S3 URL and returns its details.

    Args:
        s3_url (str): The S3 URL pointing to the base of the Delta table.
        s3_client: An initialized Boto3 S3 client instance.
        temp_dir (str): Path to a temporary directory for downloads.

    Returns:
        dict: A dictionary containing Delta table details, ready for serialization.
              Includes 'error' key if processing fails.
    """
    start_time = time.time()
    print(f"\n--- Service: Processing Delta Request for {s3_url} ---")

    bucket_name = None
    table_base_key = ""
    delta_log_prefix = ""

    try:
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)
        # Ensure table base key ends with a slash for consistency
        if not table_base_key.endswith('/') and table_base_key != "": table_base_key += '/'
        # Delta log directory standard location
        delta_log_prefix = table_base_key + "_delta_log/"

        print(f"INFO: Processing Delta Lake table at: s3://{bucket_name}/{table_base_key}")
        print(f"INFO: Delta log prefix: {delta_log_prefix}")

        # --- 1. Find and Categorize Delta Log Files ---
        log_files_raw = []
        continuation_token = None
        print(f"DEBUG: Listing objects under {delta_log_prefix}")
        while True:
            list_kwargs = {'Bucket': bucket_name, 'Prefix': delta_log_prefix}
            if continuation_token: list_kwargs['ContinuationToken'] = continuation_token
            try:
                list_response = s3_client.list_objects_v2(**list_kwargs)
            except s3_client.exceptions.NoSuchKey:
                # If the prefix itself doesn't exist
                 list_response = {}
            # Let other ClientErrors propagate (like AccessDenied)

            log_files_raw.extend(list_response.get('Contents', []))
            if list_response.get('IsTruncated'):
                continuation_token = list_response.get('NextContinuationToken')
            else:
                break # No more pages

        print(f"DEBUG: Found {len(log_files_raw)} total objects under delta log prefix.")

        # Check if _delta_log exists but is empty, or if base path is missing
        if not log_files_raw:
            try:
                # Check if the base prefix exists by listing with MaxKeys=1
                s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_base_key, Delimiter='/', MaxKeys=1)
                # If the above succeeds, the base exists but the log is empty.
                return {"error": f"Delta log prefix '{delta_log_prefix}' found but is empty. May not be a valid Delta table or is empty."}
            except s3_client.exceptions.ClientError as head_err:
                 error_code = head_err.response.get('Error', {}).get('Code')
                 if error_code == '404' or error_code == 'NoSuchKey':
                     return {"error": f"Base table path s3://{bucket_name}/{table_base_key} or Delta log prefix '{delta_log_prefix}' not found."}
                 elif error_code == 'AccessDenied':
                     return {"error": f"Access Denied listing S3 path s3://{bucket_name}/{table_base_key} or log. Check permissions."}
                 else: # Other errors during check
                     raise head_err # Re-raise unexpected listing errors


        # --- Categorize log files: JSON commits, Checkpoints, _last_checkpoint ---
        delta_log_files_info = [] # For reporting found log files
        json_commits = {} # version -> {key, size, last_modified}
        checkpoint_files = {} # version -> {num_parts, parts: {part_num -> {key, size, last_modified}}}
        last_checkpoint_info = None # Info from _last_checkpoint file

        # Regex patterns for matching log file names
        json_pattern = re.compile(r"(\d+)\.json$")
        # Matches single part (00000.checkpoint.parquet) and multi-part (00001.checkpoint.01.10.parquet)
        checkpoint_pattern = re.compile(r"(\d+)\.checkpoint(?:\.(\d{1,5})\.(\d{1,5}))?\.parquet$")

        for obj in log_files_raw:
            key = obj['Key']
            filename = os.path.basename(key)
            size = obj.get('Size')
            last_modified = obj['LastModified']
            file_s3_uri = f"s3://{bucket_name}/{key}"
            relative_path = key.replace(table_base_key, "", 1) # Path relative to table root

            # Store info about relevant files for the report
            is_log_file = False
            if filename == "_last_checkpoint": is_log_file = True
            if json_pattern.match(filename): is_log_file = True
            if checkpoint_pattern.match(filename): is_log_file = True

            if is_log_file:
                delta_log_files_info.append({
                    "file_path": file_s3_uri,
                    "relative_path": relative_path,
                    "size_bytes": size,
                    "size_human": format_bytes(size),
                    "last_modified_iso": last_modified.isoformat() if last_modified else None
                })

            # Process _last_checkpoint file
            if filename == "_last_checkpoint":
                local_last_cp_path = os.path.join(temp_dir, "_last_checkpoint")
                try:
                    download_s3_file(s3_client, bucket_name, key, local_last_cp_path)
                    with open(local_last_cp_path, 'r') as f:
                        last_checkpoint_data = json.load(f)
                    # Basic validation
                    if isinstance(last_checkpoint_data, dict) and 'version' in last_checkpoint_data:
                        last_checkpoint_info = {
                            'version': last_checkpoint_data['version'],
                            'size': last_checkpoint_data.get('size'), # Optional
                            'parts': last_checkpoint_data.get('parts'), # Optional, for multi-part checkpoints
                            'key': key, # Keep track of the file itself
                            'file_size': size # Size of the _last_checkpoint file
                        }
                        print(f"DEBUG: Found _last_checkpoint pointing to version {last_checkpoint_info['version']} (parts: {last_checkpoint_info['parts']})")
                    else:
                        print(f"Warning: Invalid content in _last_checkpoint file: {key}")
                except FileNotFoundError:
                     print(f"Warning: _last_checkpoint file listed but not found on download: {key}") # Should be rare
                except json.JSONDecodeError:
                     print(f"Warning: Failed to parse JSON in _last_checkpoint file: {key}")
                except Exception as cp_err:
                     print(f"Warning: Error processing _last_checkpoint file {key}: {cp_err}")
                continue # Move to next file

            # Process JSON commit files (N.json)
            json_match = json_pattern.match(filename)
            if json_match:
                version = int(json_match.group(1))
                json_commits[version] = {'key': key, 'size': size, 'last_modified': last_modified}
                continue # Move to next file

            # Process checkpoint files (N.checkpoint.parquet or N.checkpoint.X.Y.parquet)
            cp_match = checkpoint_pattern.match(filename)
            if cp_match:
                version = int(cp_match.group(1))
                part_num = int(cp_match.group(2)) if cp_match.group(2) else 1 # Default to part 1 if single file
                num_parts = int(cp_match.group(3)) if cp_match.group(3) else 1 # Default to 1 part if single file

                if version not in checkpoint_files:
                    checkpoint_files[version] = {'num_parts': num_parts, 'parts': {}}
                # Store info for this part
                checkpoint_files[version]['parts'][part_num] = {'key': key, 'size': size, 'last_modified': last_modified}
                # Check for consistency in the number of parts specified in filenames
                if checkpoint_files[version]['num_parts'] != num_parts and cp_match.group(2):
                     print(f"Warning: Inconsistent number of parts detected for checkpoint version {version}. Expected {checkpoint_files[version]['num_parts']}, found file suggesting {num_parts}.")
                     # Potentially update num_parts to the maximum seen? Or just note the inconsistency.
                     checkpoint_files[version]['num_parts'] = max(checkpoint_files[version]['num_parts'], num_parts)

        # Sort the reported log files for clarity
        delta_log_files_info.sort(key=lambda x: x.get('relative_path', ''))

        # --- Determine the latest available version (Snapshot ID) ---
        # This is the highest version number seen in either JSON commits or completed checkpoints
        current_snapshot_id = -1
        if json_commits:
            current_snapshot_id = max(json_commits.keys())

        # Consider checkpoint versions only if they are higher than the latest JSON commit found
        latest_complete_cp_version = -1
        if checkpoint_files:
            complete_cp_versions = [
                v for v, info in checkpoint_files.items()
                if isinstance(info.get('parts'), dict) and len(info['parts']) == info.get('num_parts', 1)
            ]
            if complete_cp_versions:
                latest_complete_cp_version = max(complete_cp_versions)

        # The true latest snapshot ID is the max of latest JSON or latest complete checkpoint
        current_snapshot_id = max(current_snapshot_id, latest_complete_cp_version)

        # If _last_checkpoint points higher, trust it (even if files are missing), but log warning
        if last_checkpoint_info and last_checkpoint_info['version'] > current_snapshot_id:
            print(f"Warning: _last_checkpoint version ({last_checkpoint_info['version']}) is higher than latest found commit/checkpoint ({current_snapshot_id}). Files might be missing.")
            # Decide whether to use last_checkpoint_info['version'] or the highest found. Using highest found is safer.
            # current_snapshot_id = last_checkpoint_info['version'] # Option to trust _last_checkpoint

        if current_snapshot_id == -1:
            # If still -1 after checking JSON and checkpoints, something is wrong
            return {"error": "No Delta commit JSON files or valid checkpoint files found. Cannot determine table version."}

        print(f"INFO: Latest Delta version (snapshot ID) identified: {current_snapshot_id}")

        # --- 2. Determine State Reconstruction Strategy ---
        # Goal: Find the most recent valid checkpoint <= current_snapshot_id
        # and process JSON commits > checkpoint version.

        effective_checkpoint_version = -1 # The version number of the checkpoint we will load state from
        checkpoint_to_load_info = None # Info about the checkpoint files to load

        # Find the latest checkpoint version available that is less than or equal to the target snapshot_id
        # and is complete (all parts found).
        candidate_cp_versions = sorted([
            v for v, info in checkpoint_files.items()
            if v <= current_snapshot_id and isinstance(info.get('parts'), dict) and len(info['parts']) == info.get('num_parts', 1)
        ], reverse=True) # Sort descending

        if candidate_cp_versions:
            effective_checkpoint_version = candidate_cp_versions[0]
            checkpoint_to_load_info = checkpoint_files[effective_checkpoint_version]
            print(f"INFO: Planning to load state from checkpoint version {effective_checkpoint_version}")
        else:
            print("INFO: No suitable checkpoint found <= current version. Will process JSON logs from version 0.")
            effective_checkpoint_version = -1 # Explicitly mark no checkpoint used

        # --- Load state from Checkpoint (if applicable) ---
        active_files = {} # path -> { size, partitionValues, modificationTime, stats, tags }
        metadata_from_log = None # Stores the most recent 'metaData' action
        protocol_from_log = None # Stores the most recent 'protocol' action
        all_checkpoint_actions = [] # Store raw actions from checkpoint if loaded

        if effective_checkpoint_version > -1 and checkpoint_to_load_info:
            try:
                print(f"INFO: Reading state from checkpoint version {effective_checkpoint_version}...")
                num_parts_to_load = checkpoint_to_load_info.get('num_parts', 1)
                for part_num in range(1, num_parts_to_load + 1):
                    part_info = checkpoint_to_load_info['parts'].get(part_num)
                    if not part_info:
                        # This should not happen if we checked for completeness earlier, but double-check
                        raise RuntimeError(f"Checkpoint part {part_num} for version {effective_checkpoint_version} is missing!")

                    part_key = part_info['key']
                    print(f"DEBUG: Reading checkpoint part {part_num}/{num_parts_to_load}: {part_key}")
                    # Use the parser utility to read the Parquet checkpoint part
                    part_actions = read_delta_checkpoint(s3_client, bucket_name, part_key, temp_dir)
                    all_checkpoint_actions.extend(part_actions) # Collect all actions

                # Process actions loaded from the checkpoint
                for action in all_checkpoint_actions:
                    if 'add' in action and action['add']:
                        add_info = action['add']
                        path = add_info.get('path')
                        if path:
                            # Parse stats string into dict if present
                            stats_parsed = None
                            if add_info.get('stats'):
                                try: stats_parsed = json.loads(add_info['stats'])
                                except json.JSONDecodeError: pass # Ignore if stats are not valid JSON
                            active_files[path] = {
                                'size': add_info.get('size'),
                                'partitionValues': add_info.get('partitionValues', {}), # Ensure it's a dict
                                'modificationTime': add_info.get('modificationTime'),
                                'dataChange': add_info.get('dataChange', True), # Assume true if missing
                                'stats': stats_parsed,
                                'tags': add_info.get('tags')
                            }
                    elif 'remove' in action and action['remove']:
                         # Checkpoints might contain 'remove' actions if compaction happened,
                         # but they usually represent files already removed *before* the checkpoint state.
                         # We generally don't need to process 'remove' actions FROM a checkpoint itself
                         # unless reconstructing history. For current state, 'add' actions define the active set.
                         pass
                    elif 'metaData' in action and action['metaData']:
                         # Capture the metadata state from the checkpoint
                         metadata_from_log = action['metaData']
                         print(f"DEBUG: Found metaData in checkpoint {effective_checkpoint_version}")
                    elif 'protocol' in action and action['protocol']:
                         # Capture the protocol state from the checkpoint
                         protocol_from_log = action['protocol']
                         print(f"DEBUG: Found protocol in checkpoint {effective_checkpoint_version}")

                start_process_version = effective_checkpoint_version + 1 # Start processing JSON files after the checkpoint
                print(f"INFO: Checkpoint processing complete. Active files from CP: {len(active_files)}. Starting JSON processing from version {start_process_version}")

            except Exception as cp_read_err:
                print(f"ERROR: Failed to read or process checkpoint version {effective_checkpoint_version}: {cp_read_err}. Falling back to processing all JSON logs.")
                traceback.print_exc()
                # Reset state if checkpoint loading failed
                active_files = {}
                metadata_from_log = None
                protocol_from_log = None
                start_process_version = 0
                effective_checkpoint_version = -1 # Mark checkpoint as not used
                all_checkpoint_actions = []
        else:
            # No checkpoint used, start from the beginning
            start_process_version = 0

        # --- 3. Process JSON Commits ---
        # Process commit files from start_process_version up to the current_snapshot_id
        versions_to_process = sorted([v for v in json_commits if v >= start_process_version and v <= current_snapshot_id])

        if not versions_to_process and effective_checkpoint_version == -1:
             # No checkpoint loaded AND no JSON files to process - indicates an issue or empty table state after version 0
             if current_snapshot_id == 0 and 0 not in json_commits: # Check if version 0 json is missing
                 return {"error": "No JSON commits found to process, including version 0, and no checkpoint loaded."}
             elif not active_files: # If checkpoint load failed and still no JSONs
                 print("Warning: No JSON commits found to process after failed checkpoint load.")
                 # Allow proceeding if metadata was possibly found in failed CP attempt
             # If a checkpoint was loaded successfully, it's okay if there are no newer JSON files.
             elif effective_checkpoint_version > -1:
                 print("INFO: No newer JSON files found after the loaded checkpoint. State is as per checkpoint.")


        print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} up to {current_snapshot_id}...")
        all_commit_info = {} # Store summary details per processed commit version
        # Track sizes of removed files per commit if not available in commitInfo metrics
        removed_file_sizes_by_commit = {}

        for version in versions_to_process:
            commit_file_info = json_commits[version]
            commit_key = commit_file_info['key']
            print(f"DEBUG: Processing version {version} ({os.path.basename(commit_key)})...")
            removed_file_sizes_by_commit[version] = 0 # Initialize size tracking for this commit
            commit_actions = [] # Store actions for this commit

            try:
                # Read actions from the JSON commit file
                commit_actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)

                # Initialize summary details for this commit version
                commit_summary_details = {
                    'version': version, 'timestamp': None, 'operation': 'Unknown',
                    'num_actions': len(commit_actions), 'operationParameters': {},
                    'num_added_files': 0, 'num_removed_files': 0,
                    'added_bytes': 0, 'removed_bytes': 0,
                    'metrics': {}, # Store raw operation metrics from commitInfo
                    'error': None
                }
                op_metrics = {} # Parsed metrics from commitInfo

                # Process actions within the commit file
                for action in commit_actions:
                    if 'commitInfo' in action and action['commitInfo']:
                        # Found commitInfo - extract metadata about the operation
                        ci = action['commitInfo']
                        commit_summary_details['timestamp'] = ci.get('timestamp')
                        commit_summary_details['operation'] = ci.get('operation', 'Unknown')
                        commit_summary_details['operationParameters'] = ci.get('operationParameters', {})
                        op_metrics = ci.get('operationMetrics', {})
                        commit_summary_details['metrics'] = op_metrics # Store raw metrics

                        # Prefer metrics from commitInfo if available
                        # Note: Key names can vary slightly between Spark versions (e.g., numTargetFilesRemoved vs numRemovedFiles)
                        commit_summary_details['num_added_files'] = int(op_metrics.get('numOutputFiles', 0) or op_metrics.get('numTargetFilesAdded', 0))
                        commit_summary_details['num_removed_files'] = int(op_metrics.get('numRemovedFiles', 0) or op_metrics.get('numTargetFilesRemoved', 0))
                        commit_summary_details['added_bytes'] = int(op_metrics.get('numOutputBytes', 0) or op_metrics.get('numTargetBytesAdded', 0))
                        # Prefer newer metric name for removed bytes
                        commit_summary_details['removed_bytes'] = int(op_metrics.get('numTargetBytesRemoved', 0) or op_metrics.get('numRemovedBytes', 0))


                    elif 'add' in action and action['add']:
                        # Apply 'add' action to the current state
                        add_info = action['add']
                        path = add_info.get('path')
                        if path:
                            stats_parsed = None
                            if add_info.get('stats'):
                                try: stats_parsed = json.loads(add_info['stats'])
                                except json.JSONDecodeError: pass
                            # Add or update the file in our active state
                            active_files[path] = {
                                'size': add_info.get('size'),
                                'partitionValues': add_info.get('partitionValues', {}),
                                'modificationTime': add_info.get('modificationTime'),
                                'dataChange': add_info.get('dataChange', True),
                                'stats': stats_parsed,
                                'tags': add_info.get('tags')
                            }
                            # Increment summary counts only if commitInfo metrics were NOT available
                            if not op_metrics.get('numOutputFiles') and not op_metrics.get('numTargetFilesAdded'):
                                commit_summary_details['num_added_files'] += 1
                            if not op_metrics.get('numOutputBytes') and not op_metrics.get('numTargetBytesAdded'):
                                commit_summary_details['added_bytes'] += add_info.get('size', 0)

                    elif 'remove' in action and action['remove']:
                        # Apply 'remove' action to the current state
                        remove_info = action['remove']
                        path = remove_info.get('path')
                        # Only remove if dataChange is true (default)
                        if path and remove_info.get('dataChange', True):
                            # Remove the file from active state and get its info (if it existed)
                            removed_file_info = active_files.pop(path, None)
                            # Increment summary counts only if commitInfo metrics were NOT available
                            if not op_metrics.get('numRemovedFiles') and not op_metrics.get('numTargetFilesRemoved'):
                                commit_summary_details['num_removed_files'] += 1
                            # Track removed size if metrics missing and file info was available
                            if not op_metrics.get('numTargetBytesRemoved') and not op_metrics.get('numRemovedBytes'):
                                if removed_file_info and removed_file_info.get('size') is not None:
                                    removed_file_sizes_by_commit[version] += removed_file_info['size']


                    elif 'metaData' in action and action['metaData']:
                        # Update metadata if changed in this commit
                        metadata_from_log = action['metaData']
                        print(f"DEBUG: Updated metaData from version {version}")
                    elif 'protocol' in action and action['protocol']:
                        # Update protocol if changed in this commit
                        protocol_from_log = action['protocol']
                        print(f"DEBUG: Updated protocol from version {version}")

                # --- Post-action processing for the commit ---
                # If removed_bytes metric was missing, use the sum calculated from 'remove' actions
                if commit_summary_details['removed_bytes'] == 0 and removed_file_sizes_by_commit[version] > 0 :
                    commit_summary_details['removed_bytes'] = removed_file_sizes_by_commit[version]

                # Store the finalized summary for this version
                all_commit_info[version] = commit_summary_details

            except FileNotFoundError:
                print(f"ERROR: Commit file {commit_key} for version {version} not found during processing.")
                all_commit_info[version] = {'version': version, 'error': f"Commit file not found: {commit_key}"}
                # Decide whether to stop or continue processing subsequent commits
                # For robustness, let's try to continue. The state might be incomplete.
            except Exception as json_proc_err:
                print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
                traceback.print_exc()
                all_commit_info[version] = {'version': version, 'error': str(json_proc_err)}
                # Continue processing if possible

        print(f"INFO: Finished processing JSON logs.")


        # --- 3.5 Find Definitive Metadata & Protocol ---
        # Ensure we have the metadata and protocol from the *latest* relevant action
        # If they weren't updated in the last processed JSON/CP, search backwards if necessary.
        definitive_metadata = metadata_from_log
        definitive_protocol = protocol_from_log

        # If metadata or protocol is missing after forward pass, search backwards through
        # processed JSONs or the loaded checkpoint.
        if not definitive_metadata or not definitive_protocol:
             print("DEBUG: Metadata or protocol missing after forward pass. Searching backwards...")
             # Search order: Processed JSONs (desc), then loaded Checkpoint actions
             versions_checked_backward = set()
             # Check processed JSONs first
             for v in sorted(versions_to_process, reverse=True):
                 if definitive_metadata and definitive_protocol: break
                 if v in all_commit_info and all_commit_info[v].get('error') is None:
                     try:
                        # Re-read the file (or retrieve cached actions if implemented)
                        commit_key = json_commits[v]['key']
                        backward_actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
                        versions_checked_backward.add(v)
                        for action in reversed(backward_actions): # Check actions within file in reverse
                             if not definitive_metadata and 'metaData' in action and action['metaData']:
                                 definitive_metadata = action['metaData']
                                 print(f"DEBUG: Found definitive metaData searching backwards in version {v}")
                             if not definitive_protocol and 'protocol' in action and action['protocol']:
                                 definitive_protocol = action['protocol']
                                 print(f"DEBUG: Found definitive protocol searching backwards in version {v}")
                             if definitive_metadata and definitive_protocol: break
                     except Exception as bk_err:
                        print(f"Warning: Error re-reading version {v} JSON for backward search: {bk_err}")

             # If still missing, check the loaded checkpoint actions (if any)
             if (not definitive_metadata or not definitive_protocol) and all_checkpoint_actions:
                  print(f"DEBUG: Checking loaded checkpoint (v{effective_checkpoint_version}) actions for metadata/protocol...")
                  for action in reversed(all_checkpoint_actions):
                      if not definitive_metadata and 'metaData' in action and action['metaData']:
                          definitive_metadata = action['metaData']
                          print(f"DEBUG: Found definitive metaData in loaded checkpoint actions.")
                      if not definitive_protocol and 'protocol' in action and action['protocol']:
                          definitive_protocol = action['protocol']
                          print(f"DEBUG: Found definitive protocol in loaded checkpoint actions.")
                      if definitive_metadata and definitive_protocol: break

        # Final check and default for protocol
        if not definitive_metadata:
            # This is critical, table cannot be understood without metadata
            return {"error": "Could not determine table metadata (schema, partitioning) after searching logs. Invalid Delta table state."}
        if not definitive_protocol:
            # Protocol is less critical, can assume defaults, but log a warning
            print("Warning: Could not find protocol information in logs. Assuming default reader/writer versions.")
            definitive_protocol = {"minReaderVersion": 1, "minWriterVersion": 2} # Common defaults


        # --- 3.6 Parse Schema and Format Partition Spec (using definitive metadata) ---
        table_schema_string = definitive_metadata.get("schemaString", "{}")
        table_schema = _parse_delta_schema_string(table_schema_string) # Use the helper
        if not table_schema:
             return {"error": "Failed to parse table schema from the definitive metadata."}

        partition_cols = definitive_metadata.get("partitionColumns", [])
        partition_spec_fields = []
        schema_fields_map = {f['name']: f for f in table_schema.get('fields', [])}
        for i, col_name in enumerate(partition_cols):
            source_field = schema_fields_map.get(col_name)
            if source_field:
                 # Create an Iceberg-like partition field definition
                 partition_spec_fields.append({
                     "name": col_name,
                     "transform": "identity", # Delta uses identity transform
                     "source-id": source_field['id'], # Link to the schema field ID
                     "field-id": 1000 + i # Assign a unique field ID for the partition spec (Iceberg convention)
                 })
            else:
                 print(f"Warning: Partition column '{col_name}' defined in metadata not found in the parsed table schema.")
        # Create the final partition spec structure (similar to Iceberg)
        partition_spec = {"spec-id": 0, "fields": partition_spec_fields}


        # --- 4. Calculate Final State Metrics (Based on `active_files`) ---
        total_data_files = len(active_files)
        total_delete_files = 0 # Delta doesn't use separate delete files tracked this way
        total_data_storage_bytes = sum(f['size'] for f in active_files.values() if f.get('size') is not None)
        total_delete_storage_bytes = 0 # N/A for Delta

        approx_live_records = 0
        gross_records_in_data_files = 0
        files_missing_stats = 0
        files_with_num_records = 0

        for path, f_info in active_files.items():
             num_recs = 0
             has_num_records_stat = False
             if f_info.get('stats') and 'numRecords' in f_info['stats']:
                 try:
                     num_recs = int(f_info['stats']['numRecords'])
                     if num_recs >= 0: # Basic sanity check
                         has_num_records_stat = True
                         files_with_num_records += 1
                     else:
                         print(f"Warning: File {path} has negative numRecords stat: {num_recs}")
                         files_missing_stats += 1
                 except (ValueError, TypeError):
                     print(f"Warning: Could not parse numRecords stat for file {path}: {f_info['stats']['numRecords']}")
                     files_missing_stats += 1
             else:
                 files_missing_stats += 1

             # For Delta, gross and live are often the same based on available stats
             # unless implementing complex logic for DV etc.
             if has_num_records_stat:
                 gross_records_in_data_files += num_recs
                 approx_live_records += num_recs # Best estimate available

        # Note: approx_deleted_records_in_manifests is an Iceberg concept
        approx_deleted_records_in_manifests = 0

        avg_live_records_per_data_file = (approx_live_records / files_with_num_records) if files_with_num_records > 0 else 0
        avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1)) / (1024*1024)

        metrics_note = f"Live record count ({approx_live_records}) is an estimate based on 'numRecords' in Delta file stats."
        if files_missing_stats > 0:
             metrics_note += f" Stats were missing or unparseable for {files_missing_stats}/{total_data_files} active files."
        metrics_note += " Delete counts/bytes are 0 as Delta Lake doesn't track delete files like Iceberg V2."

        # --- 5. Calculate Partition Stats ---
        partition_stats = {} # partition_key_string -> { stats }
        for path, file_info in active_files.items():
            part_values = file_info.get('partitionValues', {}) # Should be a dict
            # Create canonical string key from sorted partition values
            part_key_string = json.dumps(dict(sorted(part_values.items())), default=str) if part_values else "<unpartitioned>"

            if part_key_string not in partition_stats:
                 partition_stats[part_key_string] = {
                     "partition_values": part_values,
                     "partition_key_string": part_key_string,
                     "num_data_files": 0,
                     "size_bytes": 0,
                     "gross_record_count": 0, # Based on numRecords stat
                     "files_missing_stats": 0
                 }

            stats_entry = partition_stats[part_key_string]
            stats_entry["num_data_files"] += 1
            stats_entry["size_bytes"] += file_info.get('size', 0)

            # Add record count if available
            if file_info.get('stats') and 'numRecords' in file_info['stats']:
                try:
                    num_recs = int(file_info['stats']['numRecords'])
                    if num_recs >= 0:
                        stats_entry["gross_record_count"] += num_recs
                    else: stats_entry["files_missing_stats"] += 1
                except (ValueError, TypeError):
                    stats_entry["files_missing_stats"] += 1
            else:
                stats_entry["files_missing_stats"] += 1

        # Format for partition explorer output
        partition_explorer_data = []
        for p_data in partition_stats.values():
             p_data["size_human"] = format_bytes(p_data["size_bytes"])
             if p_data["files_missing_stats"] > 0:
                 p_data["record_count_note"] = f"Record count may be inaccurate ({p_data['files_missing_stats']} files missing stats)."
             partition_explorer_data.append(p_data)

        partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))


        # --- 6. Get Sample Data ---
        sample_data = []
        if active_files:
            # Try to find a Parquet file for sampling, prefer smaller files? Or just first?
            sample_file_relative_path = None
            # Iterate through active files to find a parquet file
            for p in active_files.keys():
                if p.lower().endswith('.parquet'):
                    sample_file_relative_path = p
                    break # Take the first one found

            if sample_file_relative_path:
                # Construct full S3 key for the sample file
                # Ensure no double slashes if table_base_key is root "/"
                sample_file_s3_key = (table_base_key + sample_file_relative_path).lstrip('/')
                print(f"INFO: Attempting to get sample data from: s3://{bucket_name}/{sample_file_s3_key}")
                try:
                    sample_data = read_parquet_sample(
                        s3_client, bucket_name, sample_file_s3_key, temp_dir, num_rows=10
                    )
                except FileNotFoundError:
                    # Should be caught by read_parquet_sample now, but handle just in case
                    sample_data = [{"error": f"Sample file listed in log but not found on S3", "details": f"s3://{bucket_name}/{sample_file_s3_key}"}]
                except Exception as sample_err:
                    # Catch errors from read_parquet_sample
                    print(f"ERROR: Sampling failed - {sample_err}")
                    # Ensure sample_data contains the error if read_parquet_sample didn't return it
                    if not (isinstance(sample_data, list) and sample_data and 'error' in sample_data[0]):
                         sample_data = [{"error": f"Failed to read sample data", "details": str(sample_err)}]
            else:
                sample_data = [{"error": "No Parquet files found in the active file list to sample."}]
        else:
             print("INFO: No active data files found in the final state to sample.")


        # --- 7. Assemble Final Result ---
        print("\nINFO: Assembling final Delta result...")

        # Assemble details about the current snapshot (version)
        current_commit_summary = all_commit_info.get(current_snapshot_id, {}) # Get summary if processed
        current_snapshot_details = {
            "version": current_snapshot_id,
            "timestamp_ms": current_commit_summary.get('timestamp'),
            "timestamp_iso": format_timestamp_ms(current_commit_summary.get('timestamp')),
            "operation": current_commit_summary.get('operation', 'N/A (Checkpoint?)'), # Indicate if loaded from CP
            "operation_parameters": current_commit_summary.get('operationParameters', {}),
            # Total state metrics for THIS snapshot version
            "num_files_total_snapshot": total_data_files, # Total active files
            "total_data_storage_bytes_snapshot": total_data_storage_bytes,
            "total_records_snapshot": approx_live_records, # Estimated total records
             # Metrics specific to the COMMIT that CREATED this snapshot (if available)
            "commit_num_added_files": current_commit_summary.get('num_added_files'),
            "commit_num_removed_files": current_commit_summary.get('num_removed_files'),
            "commit_added_bytes": current_commit_summary.get('added_bytes'),
            "commit_removed_bytes": current_commit_summary.get('removed_bytes'),
            "commit_metrics_raw": current_commit_summary.get('metrics', {}),
            "commit_error": current_commit_summary.get('error') # Include error if commit processing failed
        }

        # Assemble Version History (limited for performance)
        snapshots_overview = []
        # Combine known versions from JSON and the loaded Checkpoint
        processed_versions = set(all_commit_info.keys())
        if effective_checkpoint_version > -1: processed_versions.add(effective_checkpoint_version)
        # Sort descending, limit history
        history_limit = 20
        versions_in_history = sorted(list(processed_versions), reverse=True)[:history_limit]

        current_snapshot_summary_for_history = None # To store enhanced summary for current snapshot

        for v in versions_in_history:
             summary_dict = {"snapshot-id": v, "timestamp-ms": None, "summary": {}}
             commit_details = all_commit_info.get(v)

             if commit_details and not commit_details.get('error'):
                 summary_dict["timestamp-ms"] = commit_details.get('timestamp')
                 summary_dict["summary"] = {
                     "operation": commit_details.get('operation', 'Unknown'),
                     "added-data-files": str(commit_details.get('num_added_files', 'N/A')),
                     "removed-data-files": str(commit_details.get('num_removed_files', 'N/A')),
                     "added-files-size": str(commit_details.get('added_bytes', 'N/A')),
                     "removed-files-size": str(commit_details.get('removed_bytes', 'N/A')),
                     "operation-parameters": commit_details.get('operationParameters', {})
                     # Add raw metrics if desired: "metrics": commit_details.get('metrics', {})
                 }
                 # If this is the current snapshot, add total state info to its summary
                 if v == current_snapshot_id:
                      summary_dict["summary"]["total-data-files"] = str(total_data_files)
                      summary_dict["summary"]["total-files-size"] = str(total_data_storage_bytes)
                      summary_dict["summary"]["total-records"] = str(approx_live_records) # Estimate
                      # Add placeholders for Iceberg-specific fields for consistency?
                      summary_dict["summary"]["total-delete-files"] = "0"
                      summary_dict["summary"]["total-equality-deletes"] = "0"
                      summary_dict["summary"]["total-position-deletes"] = "0"
                      # Store this enhanced summary separately for the version_history.current_snapshot_summary field
                      current_snapshot_summary_for_history = summary_dict.copy()

             elif commit_details and commit_details.get('error'):
                 summary_dict["summary"] = {"error": commit_details['error']}
             elif v == effective_checkpoint_version and not commit_details: # State loaded from CP, no JSON for this version processed
                 summary_dict["summary"] = {"operation": "CHECKPOINT_LOAD", "info": f"State loaded from checkpoint {v}"}
                 if v == current_snapshot_id: # Checkpoint IS the latest state
                     summary_dict["summary"]["total-data-files"] = str(total_data_files)
                     summary_dict["summary"]["total-files-size"] = str(total_data_storage_bytes)
                     summary_dict["summary"]["total-records"] = str(approx_live_records)
                     summary_dict["summary"]["total-delete-files"] = "0"
                     summary_dict["summary"]["total-equality-deletes"] = "0"
                     summary_dict["summary"]["total-position-deletes"] = "0"
                     current_snapshot_summary_for_history = summary_dict.copy()
             else:
                 summary_dict["summary"] = {"operation": "Unknown", "info": "Commit details not processed or unavailable"}

             # Add placeholder totals for historical snapshots for consistency in the list
             if v != current_snapshot_id:
                 summary_dict["summary"]["total-data-files"] = "N/A (Historical)"
                 summary_dict["summary"]["total-files-size"] = "N/A (Historical)"
                 summary_dict["summary"]["total-records"] = "N/A (Historical)"
                 summary_dict["summary"]["total-delete-files"] = "0"
                 summary_dict["summary"]["total-equality-deletes"] = "0"
                 summary_dict["summary"]["total-position-deletes"] = "0"

             snapshots_overview.append(summary_dict)


        # Final Result Structure
        result = {
            "table_type": "Delta",
            "table_uuid": definitive_metadata.get("id"), # Delta table ID
            "location": s3_url, # Original requested URL
            # "canonical_location": f"s3://{bucket_name}/{table_base_key}", # More precise location

            "format_configuration": { # Combine protocol and config properties
                 **definitive_protocol, # minReaderVersion, minWriterVersion
                 **(definitive_metadata.get("configuration", {})) # Table properties
            },
            "format_version": definitive_protocol.get('minReaderVersion', 1), # Legacy field?

            "delta_log_files": delta_log_files_info, # List of found log/checkpoint files

            "current_snapshot_id": current_snapshot_id,
            "current_snapshot_details": current_snapshot_details, # Detailed info about latest

            "table_schema": table_schema, # Iceberg-like schema format
            "table_properties": definitive_metadata.get("configuration", {}), # Direct access to properties

            "partition_spec": partition_spec, # Iceberg-like partition spec format
            "sort_order": {"order-id": 0, "fields": []}, # Placeholder, Delta doesn't have explicit sort order like Iceberg

            "version_history": {
                "total_snapshots": len(processed_versions), # Total versions found/processed
                "current_snapshot_summary": current_snapshot_summary_for_history, # Enhanced summary for current
                "snapshots_overview": snapshots_overview # List of recent snapshots (limited)
            },
            "key_metrics": {
                 # Totals for the CURRENT state
                "total_data_files": total_data_files,
                "total_delete_files": total_delete_files, # 0
                "total_data_storage_bytes": total_data_storage_bytes,
                "total_data_storage_human": format_bytes(total_data_storage_bytes),
                "total_delete_storage_bytes": total_delete_storage_bytes, # 0
                "total_delete_storage_human": format_bytes(total_delete_storage_bytes),
                 # Record Counts (Estimates for Delta)
                "gross_records_in_data_files": gross_records_in_data_files,
                "approx_deleted_records_in_manifests": approx_deleted_records_in_manifests, # 0
                "approx_live_records": approx_live_records,
                 # Averages
                "avg_live_records_per_data_file": round(avg_live_records_per_data_file, 2),
                "avg_data_file_size_mb": round(avg_data_file_size_mb, 4),
                # Note
                "metrics_note": metrics_note,
            },
            "partition_explorer": partition_explorer_data, # Stats per partition
            "sample_data": sample_data, # Sample rows or error
        }

        end_time = time.time()
        print(f"--- Service: Delta Request Completed in {end_time - start_time:.2f} seconds ---")
        # Final serialization (convert_bytes) happens in the route
        return result

    except FileNotFoundError as e:
         # Catch file not found errors specifically (likely from S3 download)
         print(f"ERROR: Required file not found during Delta processing: {e}")
         return {"error": f"A required file was not found: {e}"}
    except ValueError as e:
         # Catch invalid input or format errors
         print(f"ERROR: Value error during Delta processing: {e}")
         return {"error": f"Input or value error: {str(e)}"}
    except Exception as e:
         # Catch all other unexpected errors during Delta processing
         print(f"ERROR: An unexpected error occurred during Delta processing: {e}")
         traceback.print_exc()
         return {"error": f"An unexpected server error occurred: {str(e)}"}