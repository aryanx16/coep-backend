from flask import Flask, request, jsonify
import boto3
import os
import json

app = Flask(__name__)

@app.route('/', methods=['GET'])
def hello():
    return 'hello'

BUCKET_NAME = "coep-inspiron-iceberg-demo"
PREFIX = "nyc_taxi_iceberg/metadata/"


# woking 
@app.route('/download-latest', methods=['GET'])
def download_latest_file():
    try:
        # Create an S3 client with provided credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        # List all objects under the specified prefix in the bucket
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
        if 'Contents' not in response:
            return jsonify({"error": "No files found in the bucket"}), 404
        
        print(response)

        # Filter objects ending with '.metadata.json'
        metadata_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.metadata.json')]
        if not metadata_files:
            return jsonify({"error": "No metadata files found"}), 404

        # Select the latest file based on the LastModified attribute
        latest_file = max(metadata_files, key=lambda x: x['LastModified'])
        print(latest_file)
        object_key = latest_file['Key']

        # Ensure the 'downloads' folder exists
        os.makedirs("downloads", exist_ok=True)

        # Define local file path using the base name of the object key
        local_file_path = os.path.join("downloads", os.path.basename(object_key))

        # Download the latest metadata file from S3
        s3_client.download_file(BUCKET_NAME, object_key, local_file_path)

        return jsonify({"message": f"Latest file '{object_key}' downloaded to {local_file_path}"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500 

@app.route('/parse-s3', methods=['GET'])
def parse_s3():
    s3_url = "s3://coep-inspiron-iceberg-demo/nyc_taxi_iceberg"
    
    if not s3_url:
        return jsonify({"error": "s3_url parameter is missing"}), 400

    try:
        # Extract bucket and prefix (which represents the folder path)
        bucket_name, prefix = extract_bucket_and_key(s3_url)

        # Create an S3 client using the provided credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )

        # List all objects under the given prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        objects = response.get("Contents", [])
        
        # Filter objects to get only those ending with "metadata.json"
        metadata_files = [obj for obj in objects if obj["Key"].endswith("metadata.json")]
        
        if not metadata_files:
            return jsonify({"error": "No metadata.json file found in the provided path"}), 404

        # Sort the metadata files by LastModified date (newest first)
        metadata_files.sort(key=lambda x: x["LastModified"], reverse=True)
        latest_metadata = metadata_files[0]
        latest_key = latest_metadata["Key"]

        # Retrieve the latest metadata.json file from S3
        obj_response = s3_client.get_object(Bucket=bucket_name, Key=latest_key)
        content = obj_response["Body"].read().decode('utf-8')

        # Parse the JSON content
        json_data = json.loads(content)
        
        return jsonify(json_data), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500 
    
    
def extract_bucket_and_key(s3_url):
    """Extract bucket name and object key from an S3 URL."""
    if not s3_url.startswith("s3://"):
        raise ValueError("Invalid S3 URL format")
    parts = s3_url.replace("s3://", "").split("/", 1)
    if len(parts) != 2:
        raise ValueError("S3 URL must include bucket and key")
    return parts[0], parts[1]




if __name__ == '__main__':
    app.run(debug=True)
