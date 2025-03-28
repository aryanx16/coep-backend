# src/app/routes/iceberg.py
import time
import tempfile
import traceback

from flask import Blueprint, request, jsonify, current_app
import boto3

# Import the service function that contains the core logic
from ..services.iceberg_service import get_iceberg_details
# Import the serializer utility
from ..utils.serializers import convert_bytes

# Create a Blueprint object for Iceberg routes
iceberg_bp = Blueprint('iceberg', __name__, url_prefix='/Iceberg')

@iceberg_bp.route('', methods=['GET']) # Route is now relative to the blueprint prefix '/Iceberg'
def iceberg_details_route():
    """
    API Endpoint to get details for an Iceberg table.
    Expects 's3_url' query parameter.
    """
    s3_url = request.args.get('s3_url')
    if not s3_url:
        return jsonify({"error": "s3_url query parameter is missing"}), 400

    print(f"\n--- Received Iceberg request for: {s3_url} ---")
    request_start_time = time.time()

    # Get AWS credentials from Flask app config
    aws_access_key_id = current_app.config.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = current_app.config.get('AWS_SECRET_ACCESS_KEY')
    aws_region = current_app.config.get('AWS_REGION')

    # Basic credential check
    if not aws_access_key_id or not aws_secret_access_key:
         print("ERROR: AWS credentials missing in configuration.")
         return jsonify({"error": "Server configuration error: AWS credentials missing."}), 500

    s3_client = None
    try:
        # Create Boto3 S3 client for this request
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

        # Use a temporary directory that cleans itself up
        with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory for route: {temp_dir}")

            # Call the service function to do the actual work
            result_data = get_iceberg_details(s3_url, s3_client, temp_dir)

            # Check if the service returned an error dictionary
            if isinstance(result_data, dict) and 'error' in result_data:
                 # Determine appropriate HTTP status code based on error type if possible
                 status_code = 500 # Default internal server error
                 if "not found" in result_data['error'].lower() or "no metadata objects" in result_data['error'].lower():
                     status_code = 404
                 elif "invalid s3 url" in result_data['error'].lower() or "input or value error" in result_data['error'].lower():
                     status_code = 400
                 print(f"ERROR from service: {result_data['error']} (Status: {status_code})")
                 return jsonify(result_data), status_code

            # --- Success Case ---
            # Ensure the result is JSON serializable before returning
            serializable_result = convert_bytes(result_data)

            request_end_time = time.time()
            print(f"--- Iceberg request successful ({s3_url}) - Total Time: {request_end_time - request_start_time:.2f} seconds ---")
            return jsonify(serializable_result), 200

    # --- Route-Level Exception Handling ---
    except boto3.exceptions.NoCredentialsError:
         print("ERROR: Boto3 could not find AWS credentials.")
         return jsonify({"error": "AWS credentials not found or invalid."}), 401 # Unauthorized
    except s3_client.exceptions.NoSuchBucket as e:
         print(f"ERROR: S3 bucket access error: {e}")
         # Extract bucket name if possible from the error response
         bucket_name_from_error = e.response.get('Error',{}).get('BucketName', '<unknown>')
         return jsonify({"error": f"S3 bucket not found or access denied: {bucket_name_from_error}"}), 404
    except s3_client.exceptions.ClientError as e:
         error_code = e.response.get('Error', {}).get('Code')
         error_message = e.response.get('Error', {}).get('Message', str(e))
         print(f"ERROR: AWS ClientError occurred: {error_code} - {error_message}")
         if error_code == 'AccessDenied':
             return jsonify({"error": f"Access Denied for S3 operation: {error_message}"}), 403
         # Add handling for other specific S3 errors if needed (e.g., throttling)
         else:
             return jsonify({"error": f"AWS ClientError: {error_code} - {error_message}"}), 500
    except Exception as e:
         # Catch any other unexpected errors at the route level
         print(f"ERROR: An unexpected error occurred in the Iceberg route: {e}")
         traceback.print_exc()
         return jsonify({"error": f"An unexpected server error occurred: {str(e)}"}), 500