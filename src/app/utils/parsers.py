# src/app/utils/parsers.py
import os
import json
import traceback
from fastavro import reader as avro_reader
import pyarrow.parquet as pq

# Import the S3 download function
from .aws_s3 import download_s3_file

def parse_avro_file(file_path):
    """Parses an Avro file and returns a list of records."""
    records = []
    print(f"DEBUG: Attempting to parse Avro file: {file_path}")
    try:
        with open(file_path, 'rb') as fo:
            # Use fastavro reader to read all records into a list
            records = list(avro_reader(fo))
        print(f"DEBUG: Successfully parsed Avro file: {file_path}, Records found: {len(records)}")
        return records
    except Exception as e:
        print(f"Error parsing Avro file {file_path}: {e}")
        traceback.print_exc()
        raise # Re-raise the error for upstream handling

def read_parquet_sample(s3_client, bucket, key, local_dir, num_rows=10):
    """Downloads a Parquet file from S3 and reads a sample."""
    # Create a safe local filename from the S3 key
    safe_basename = os.path.basename(key).replace('%', '_').replace(':', '_').replace('/', '_')
    local_filename = "sample_" + safe_basename
    local_path = os.path.join(local_dir, local_filename)
    try:
        download_s3_file(s3_client, bucket, key, local_path)
        print(f"DEBUG: Reading Parquet sample from: {local_path}")
        # Read the entire table (consider memory for very large files if not sampling)
        table = pq.read_table(local_path)
        # Slice the table to get the desired number of rows (or fewer if table is smaller)
        sample_table = table.slice(length=min(num_rows, len(table)))
        # Convert the Arrow Table slice to a list of Python dictionaries
        sample_data = sample_table.to_pylist()
        print(f"DEBUG: Successfully read {len(sample_data)} sample rows from Parquet: {key}")
        return sample_data
    except FileNotFoundError:
        # This error is now raised by download_s3_file if the S3 object doesn't exist
        print(f"ERROR: Sample Parquet file not found on S3: s3://{bucket}/{key}")
        # Return an error structure consistent with other potential errors
        return [{"error": f"Sample Parquet file not found on S3", "details": f"s3://{bucket}/{key}"}]
    except Exception as e:
        print(f"Error reading Parquet file s3://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        # Return an error structure
        return [{"error": f"Failed to read sample Parquet data", "details": str(e)}]
    finally:
        # Clean up the downloaded sample file
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                print(f"DEBUG: Cleaned up temp sample file: {local_path}")
            except Exception as rm_err:
                print(f"Warning: Could not remove temp sample file {local_path}: {rm_err}")

def read_delta_checkpoint(s3_client, bucket, key, local_dir):
    """Downloads and reads a Delta checkpoint Parquet file."""
    # Create a safe local filename
    safe_basename = os.path.basename(key).replace('%', '_').replace(':', '_').replace('/', '_')
    local_filename = "checkpoint_" + safe_basename
    local_path = os.path.join(local_dir, local_filename)
    actions = []
    try:
        download_s3_file(s3_client, bucket, key, local_path)
        print(f"DEBUG: Reading Delta checkpoint file: {local_path}")
        # Read the Parquet file into an Arrow Table
        table = pq.read_table(local_path)

        # Checkpoint files have columns like 'txn', 'add', 'remove', 'metaData', 'protocol'.
        # Each row represents one action, usually only one action column per row is non-null.
        for batch in table.to_batches():
            batch_dict = batch.to_pydict()
            # Get the number of rows in the current batch
            if not batch_dict: continue # Skip empty batches
            num_rows = len(batch_dict[list(batch_dict.keys())[0]])

            for i in range(num_rows):
                action = {}
                # Check each possible action type column for non-null value
                if batch_dict.get('add') and batch_dict['add'][i] is not None:
                    action['add'] = batch_dict['add'][i]
                elif batch_dict.get('remove') and batch_dict['remove'][i] is not None:
                    action['remove'] = batch_dict['remove'][i]
                elif batch_dict.get('metaData') and batch_dict['metaData'][i] is not None:
                    action['metaData'] = batch_dict['metaData'][i]
                elif batch_dict.get('protocol') and batch_dict['protocol'][i] is not None:
                    action['protocol'] = batch_dict['protocol'][i]
                elif batch_dict.get('txn') and batch_dict['txn'][i] is not None:
                    # 'txn' actions are less common directly in checkpoints but possible
                    action['txn'] = batch_dict['txn'][i]
                # Add elif conditions for other potential action types if needed (e.g., 'commitInfo')

                if action: # Append if any action was found in the row
                    actions.append(action)

        print(f"DEBUG: Successfully read {len(actions)} actions from checkpoint: {key}")
        return actions
    except FileNotFoundError:
        # download_s3_file now raises this if the S3 object isn't found
        print(f"ERROR: Checkpoint file not found on S3: s3://{bucket}/{key}")
        raise # Re-raise to be caught by the service layer
    except Exception as e:
        print(f"Error reading Delta checkpoint file s3://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        raise # Re-raise for handling by the service layer
    finally:
        # Clean up the downloaded checkpoint file
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                print(f"DEBUG: Cleaned up temp checkpoint file: {local_path}")
            except Exception as rm_err:
                print(f"Warning: Could not remove temp checkpoint file {local_path}: {rm_err}")


def read_delta_json_lines(s3_client, bucket, key, local_dir):
    """Downloads and reads a Delta JSON commit log file line by line."""
    # Create a safe local filename
    safe_basename = os.path.basename(key).replace('%', '_').replace(':', '_').replace('/', '_')
    local_filename = "commit_" + safe_basename
    local_path = os.path.join(local_dir, local_filename)
    actions = []
    try:
        download_s3_file(s3_client, bucket, key, local_path)
        print(f"DEBUG: Reading Delta JSON file: {local_path}")
        with open(local_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                line = line.strip()
                if not line: continue # Skip empty lines
                try:
                    # Each line in a Delta commit log is a separate JSON object representing an action
                    action = json.loads(line)
                    actions.append(action)
                except json.JSONDecodeError as json_err:
                    # Log problematic lines but continue processing the rest of the file
                    print(f"Warning: Skipping invalid JSON line {line_num+1} in {key}: {json_err} - Line: '{line}'")
        print(f"DEBUG: Successfully read {len(actions)} actions from JSON file: {key}")
        return actions
    except FileNotFoundError:
        # download_s3_file raises this if S3 object not found
        print(f"ERROR: JSON commit file not found on S3: s3://{bucket}/{key}")
        raise # Re-raise to be caught by the service layer
    except Exception as e:
        print(f"Error reading Delta JSON file s3://{bucket}/{key} (local: {local_path}): {e}")
        traceback.print_exc()
        raise # Re-raise for handling by the service layer
    finally:
        # Clean up the downloaded JSON file
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
                print(f"DEBUG: Cleaned up temp JSON file: {local_path}")
            except Exception as rm_err:
                print(f"Warning: Could not remove temp JSON file {local_path}: {rm_err}")