from flask import Flask, request, jsonify
import boto3
import os
import json
import tempfile
from dotenv import load_dotenv
from fastavro import reader as avro_reader

app = Flask(__name__)

# AWS credentials (for production, use environment variables or IAM roles)

def extract_bucket_and_key(s3_url):
    """Extract bucket name and key/prefix from an S3 URL."""
    if not s3_url.startswith("s3://"):
        raise ValueError("Invalid S3 URL format")
    parts = s3_url.replace("s3://", "").split("/", 1)
    if len(parts) != 2:
        raise ValueError("S3 URL must include both bucket and key/prefix")
    return parts[0], parts[1]

def download_s3_file(s3_client, bucket, key, local_path):
    """Download a file from S3 to a local path."""
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3_client.download_file(bucket, key, local_path)

def parse_avro_file(file_path):
    """Read an Avro file and return its records as a list of dictionaries."""
    records = []
    with open(file_path, 'rb') as fo:
        avro_file = avro_reader(fo)
        for record in avro_file:
            records.append(record)
    return records

def convert_bytes(obj):
    """
    Recursively convert bytes in the given object to strings.
    This ensures the entire structure is JSON serializable.
    """
    if isinstance(obj, bytes):
        try:
            return obj.decode('utf-8')
        except Exception:
            return str(obj)
    elif isinstance(obj, dict):
        return {k: convert_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_bytes(item) for item in obj]
    else:
        return obj

@app.route('/iceberg-details', methods=['GET'])
def iceberg_details():
    """
    Endpoint to extract all necessary details from an Iceberg table.
    It lists all metadata and Avro files, parses them, and aggregates details 
    like table schema, properties, partition details, version history, key metrics, 
    and sample data for the frontend UI.
    """
    s3_url = request.args.get('s3_url')
    if not s3_url:
        return jsonify({"error": "s3_url parameter is missing"}), 400

    try:
        bucket_name, base_key = extract_bucket_and_key(s3_url)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        # Define the metadata folder path (e.g. {table_prefix}/metadata/)
        metadata_prefix = os.path.join(base_key, "metadata/")
        list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
        if 'Contents' not in list_response:
            return jsonify({"error": "No metadata files found for the given Iceberg table."}), 404

        # Prepare temporary directory for downloads
        temp_dir = tempfile.mkdtemp(prefix="iceberg_")
        metadata_jsons = []
        avro_files_info = []  # List of dicts holding parsed avro records

        # Loop through all files in the metadata folder
        for obj in list_response['Contents']:
            key = obj['Key']
            file_name = key.split('/')[-1]
            local_file_path = os.path.join(temp_dir, file_name)
            
            # Download each file
            download_s3_file(s3_client, bucket_name, key, local_file_path)
            
            if file_name.endswith('.metadata.json'):
                # Parse JSON metadata
                with open(local_file_path, 'r') as f:
                    try:
                        metadata_jsons.append(json.load(f))
                    except Exception as json_err:
                        print(f"Error parsing {file_name}: {json_err}")
            elif file_name.endswith('.avro'):
                try:
                    records = parse_avro_file(local_file_path)
                    avro_files_info.append({
                        "file_name": file_name,
                        "records": records
                    })
                except Exception as avro_err:
                    print(f"Error parsing Avro file {file_name}: {avro_err}")
        
        # Now aggregate data from the metadata JSON files.
        aggregated = {
            "table_schema": {},
            "table_properties": {},
            "partition_details": {},
            "version_history": [],
            "key_metrics": {
                "total_files": 0,
                "number_of_data_files": 0,
                "total_records": 0,
                "total_storage_bytes": 0,
                "file_size_distribution": [],
                "record_count_distribution": []
            },
            "sample_data": [],
            "avro_files_details": []
        }

        # Use the latest metadata file for primary info (in a full implementation, you may merge multiple)
        if metadata_jsons:
            latest_meta = sorted(metadata_jsons, key=lambda m: m.get("last-updated", 0), reverse=True)[0]
            aggregated["table_schema"] = latest_meta.get("schema", {})
            aggregated["table_properties"] = latest_meta.get("properties", {})
            aggregated["partition_details"] = latest_meta.get("partition-spec", {})
            aggregated["version_history"] = latest_meta.get("snapshots", [])
            
            # Process key metrics from manifests if available.
            total_files = 0
            data_files = 0
            total_records = 0
            total_storage = 0
            file_sizes = []
            record_counts = []
            for manifest in latest_meta.get("manifests", []):
                total_files += manifest.get("total_files", 0)
                data_files += manifest.get("data_files", 0)
                recs = manifest.get("total_records", 0)
                total_records += recs
                total_storage += manifest.get("total_size", 0)
                file_sizes.append(manifest.get("total_size", 0))
                record_counts.append(recs)
            aggregated["key_metrics"]["total_files"] = total_files
            aggregated["key_metrics"]["number_of_data_files"] = data_files
            aggregated["key_metrics"]["total_records"] = total_records
            aggregated["key_metrics"]["total_storage_bytes"] = total_storage
            aggregated["key_metrics"]["file_size_distribution"] = file_sizes
            aggregated["key_metrics"]["record_count_distribution"] = record_counts

        # Additionally, use Avro files to extract further details.
        for avro_info in avro_files_info:
            sample = avro_info["records"][:5] if avro_info["records"] else []
            aggregated["avro_files_details"].append({
                "file_name": avro_info["file_name"],
                "record_count": len(avro_info["records"]),
                "sample_data": sample
            })

        # Calculate averages for UI display.
        if aggregated["key_metrics"]["number_of_data_files"] > 0:
            aggregated["key_metrics"]["avg_records_per_file"] = (
                aggregated["key_metrics"]["total_records"] / aggregated["key_metrics"]["number_of_data_files"]
            )
            aggregated["key_metrics"]["avg_file_size_mb"] = (
                (aggregated["key_metrics"]["total_storage_bytes"] / aggregated["key_metrics"]["number_of_data_files"]) / (1024*1024)
            )
        else:
            aggregated["key_metrics"]["avg_records_per_file"] = 0
            aggregated["key_metrics"]["avg_file_size_mb"] = 0

        # Example Partition Explorer Data (replace with real partition stats if available)
        aggregated["partition_explorer"] = [
            {"partition": "signup_date=2023-08", "size_bytes": int(1.3 * 1024**3), "num_files": 10},
            {"partition": "signup_date=2023-07", "size_bytes": int(3.0 * 1024**3), "num_files": 4},
            {"partition": "region=NA", "size_bytes": int(1.68 * 1024**3), "num_files": 6},
            {"partition": "region=EU", "size_bytes": int(1.02 * 1024**3), "num_files": 3},
            {"partition": "region=APAC", "size_bytes": int(686.65 * 1024**2), "num_files": 2},
            {"partition": "region=LATAM", "size_bytes": int(295.64 * 1024**2), "num_files": 1},
            {"partition": "signup_date=2023-06", "size_bytes": int(3.35 * 1024**3), "num_files": 5},
            {"partition": "signup_date=2023-05", "size_bytes": int(3.17 * 1024**3), "num_files": 5},
            {"partition": "signup_date=2023-04", "size_bytes": int(2.89 * 1024**3), "num_files": 4},
            {"partition": "signup_date=2023-03", "size_bytes": int(2.50 * 1024**3), "num_files": 4}
        ]

        # Use sample data from Avro files if available
        aggregated["sample_data"] = aggregated["avro_files_details"][0]["sample_data"] if aggregated["avro_files_details"] else []

        # Convert any bytes in the aggregated structure to strings.
        aggregated = convert_bytes(aggregated)

        # Return the aggregated details to be used by the frontend for UI visualizations.
        return jsonify(aggregated), 200

    except Exception as e:
        # Ensure the error message is a string
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def hello():
    return "Hello! Use /iceberg-details?s3_url=<your_iceberg_table_url> to fetch detailed table metadata."

if __name__ == '__main__':
    app.run(debug=True)
