# src/app/routes/delta.py
import time
import tempfile
import traceback

from flask import Blueprint, request, jsonify, current_app
import boto3

# Import the service function
from ..services.delta_service import get_delta_details
# Import the serializer utility
from ..utils.serializers import convert_bytes

# Create a Blueprint object for Delta routes
delta_bp = Blueprint('delta', __name__, url_prefix='/Delta')

@delta_bp.route('', methods=['GET']) # Route is now relative to '/Delta'
def delta_details_route():
    """
    API Endpoint to get details for a Delta Lake table.
    Expects 's3_url' query parameter.
    """
    s3_url = request.args.get('s3_url')
    if not s3_url:
        return jsonify({"error": "s3_url query parameter is missing"}), 400

    print(f"\n--- Received Delta request for: {s3_url} ---")
    request_start_time = time.time()

    # Get AWS credentials from Flask app config
    aws_access_key_id = current_app.config.get('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = current_app.config.get('AWS_SECRET_ACCESS_KEY')
    aws_region = current_app.config.get('AWS_REGION')

    if not aws_access_key_id or not aws_secret_access_key:
         print("ERROR: AWS credentials missing in configuration.")
         return jsonify({"error": "Server configuration error: AWS credentials missing."}), 500

    s3_client = None
    try:
        # Create Boto3 S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

        # Use a temporary directory
        with tempfile.TemporaryDirectory(prefix="delta_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory for route: {temp_dir}")

            # Call the service function
            result_data = get_delta_details(s3_url, s3_client, temp_dir)

            # Check if the service returned an error
            if isinstance(result_data, dict) and 'error' in result_data:
                 status_code = 500 # Default internal error
                 error_msg_lower = result_data['error'].lower()
                 if "not found" in error_msg_lower or "no delta commit json" in error_msg_lower or "empty" in error_msg_lower:
                     status_code = 404
                 elif "invalid s3 url" in error_msg_lower or "input or value error" in error_msg_lower:
                     status_code = 400
                 elif "access denied" in error_msg_lower:
                    status_code = 403
                 print(f"ERROR from service: {result_data['error']} (Status: {status_code})")
                 return jsonify(result_data), status_code

            # --- Success Case ---
            # Ensure JSON serializability
            serializable_result = convert_bytes(result_data)

            request_end_time = time.time()
            print(f"--- Delta request successful ({s3_url}) - Total Time: {request_end_time - request_start_time:.2f} seconds ---")
            return jsonify(serializable_result), 200

    # --- Route-Level Exception Handling (Similar to Iceberg route) ---
    except boto3.exceptions.NoCredentialsError:
         print("ERROR: Boto3 could not find AWS credentials.")
         return jsonify({"error": "AWS credentials not found or invalid."}), 401
    # Handle cases where s3_client might not be initialized if boto3 itself fails
    except (boto3.exceptions.Boto3Error, AttributeError) as boto_init_err:
         # Catch potential errors during boto3.client() creation if credentials/region are bad early on
         print(f"ERROR: Failed to initialize Boto3 client: {boto_init_err}")
         return jsonify({"error": f"Failed to initialize AWS connection: {boto_init_err}"}), 500
    except s3_client.exceptions.NoSuchBucket as e:
         print(f"ERROR: S3 bucket access error: {e}")
         bucket_name_from_error = e.response.get('Error',{}).get('BucketName', '<unknown>')
         return jsonify({"error": f"S3 bucket not found or access denied: {bucket_name_from_error}"}), 404
    except s3_client.exceptions.ClientError as e:
         error_code = e.response.get('Error', {}).get('Code')
         error_message = e.response.get('Error', {}).get('Message', str(e))
         print(f"ERROR: AWS ClientError occurred: {error_code} - {error_message}")
         if error_code == 'AccessDenied':
             return jsonify({"error": f"Access Denied for S3 operation: {error_message}"}), 403
         elif error_code == 'NoSuchKey': # Often indicates log prefix not found
              return jsonify({"error": f"Delta log prefix or a required file not found (NoSuchKey): {error_message}"}), 404
         else:
             return jsonify({"error": f"AWS ClientError: {error_code} - {error_message}"}), 500
    except Exception as e:
         print(f"ERROR: An unexpected error occurred in the Delta route: {e}")
         traceback.print_exc()
         return jsonify({"error": f"An unexpected server error occurred: {str(e)}"}), 500