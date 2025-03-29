# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify, session, g
import boto3
from botocore.exceptions import ClientError, NoCredentialsError # Added ClientError
import os
import json
import tempfile
import traceback
import datetime
import uuid # Added for unique session names
from dotenv import load_dotenv
from fastavro import reader as avro_reader
import pyarrow.parquet as pq
from urllib.parse import urlparse, unquote
from flask_cors import CORS
import re
import time
# from botocore.exceptions import NoCredentialsError # Already imported above
from flask_sqlalchemy import SQLAlchemy
import bcrypt
import functools
from cryptography.fernet import Fernet, InvalidToken

load_dotenv()

app = Flask(__name__)

# <<< BOLD INDICATOR START >>>
# Relax CORS for development if needed, tighten for production
# Example: Allow requests from a specific frontend origin
# Adjust based on your frontend URL
FRONTEND_URL = os.getenv("FRONTEND_URL", "*") # Default to allow all for simplicity, restrict in prod
CORS(app, supports_credentials=True, origins=[FRONTEND_URL] if FRONTEND_URL != "*" else "*")
# <<< BOLD INDICATOR END >>>

# --- Flask Configuration ---
app.config['SECRET_KEY'] = os.getenv("FLASK_SECRET_KEY")
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# Set Secure=True only when running HTTPS (like on Render)
app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV', 'development') == 'production'
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

if not app.config['SECRET_KEY']:
    raise ValueError("No FLASK_SECRET_KEY set for Flask application")
if not app.config['SQLALCHEMY_DATABASE_URI']:
    raise ValueError("No DATABASE_URL set for Flask application")

# --- Encryption Setup ---
ENCRYPTION_KEY_STR = os.getenv("ENCRYPTION_KEY")
if not ENCRYPTION_KEY_STR:
    raise ValueError("No ENCRYPTION_KEY set for URL encryption")
try:
    fernet = Fernet(ENCRYPTION_KEY_STR.encode())
except Exception as e:
    raise ValueError(f"Invalid ENCRYPTION_KEY format: {e}")

# --- Initialize SQLAlchemy ---
db = SQLAlchemy(app)

# --- AWS Configuration ---
# Boto3 will automatically look for credentials in environment variables
# (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) on Render
# for the *initial* STS client. We only need to set the default region.
# <<< BOLD INDICATOR START >>>
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1") # Set default region (e.g., Mumbai)
# Removed global AWS_ACCESS_KEY_ID = os.getenv(...)
# Removed global AWS_SECRET_ACCESS_KEY = os.getenv(...)
# <<< BOLD INDICATOR END >>>


# --- Database Models ---
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    # Field to store the AWS Role ARN provided by the user
    aws_role_arn = db.Column(db.String(255), nullable=True)
    usages = db.relationship('S3Usage', backref='user', lazy=True)

    def set_password(self, password):
        self.password_hash = bcrypt.hashpw(
            password.encode('utf-8'), bcrypt.gensalt()
        ).decode('utf-8')

    def check_password(self, password):
        if not self.password_hash: return False
        try:
            return bcrypt.checkpw(
                password.encode('utf-8'), self.password_hash.encode('utf-8')
            )
        except ValueError:
            return False

class S3Usage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    s3_bucket_link_encrypted = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.datetime.utcnow)

# --- Encryption Helper Functions ---
def encrypt_data(data_string):
    if not isinstance(data_string, str):
        raise TypeError("Input must be a string")
    return fernet.encrypt(data_string.encode()).decode()

def decrypt_data(encrypted_string):
    if not isinstance(encrypted_string, str):
        print(f"Warning: Attempting to decrypt non-string data: {type(encrypted_string)}")
        return None
    try:
        return fernet.decrypt(encrypted_string.encode()).decode()
    except InvalidToken:
        print("ERROR: Failed to decrypt data - Invalid Token")
        return None
    except Exception as e:
        print(f"ERROR: Decryption failed: {e}")
        return None


# --- Authentication Middleware/Hooks ---
@app.before_request
def load_logged_in_user():
    """If user_id is stored in the session, load the user object from DB into g.user."""
    user_id = session.get('user_id')
    # Ensure the loaded user includes the aws_role_arn
    g.user = User.query.options(db.defer(User.password_hash)).get(user_id) if user_id else None


def login_required(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if g.user is None:
            return jsonify(error="Authentication required. Please log in."), 401
        return func(*args, **kwargs)
    return wrapper

# --- Authentication Routes ---
@app.route('/register', methods=['POST'])
def register():
    # ... (Implementation including optional aws_role_arn from previous step) ...
    if g.user: return jsonify(error="Already logged in"), 400
    data = request.get_json()
    if not data: return jsonify(error="Invalid JSON payload"), 400

    username = data.get('username')
    email = data.get('email')
    password = data.get('password')
    aws_role_arn = data.get('aws_role_arn', None)

    if not username or not email or not password:
        return jsonify(error="Username, email, and password are required"), 400

    validated_arn = None
    if aws_role_arn and aws_role_arn.strip():
        validated_arn = aws_role_arn.strip()
        arn_pattern = r"^arn:aws:iam::\d{12}:role\/[\w+=,.@\/-]+$"
        if not re.match(arn_pattern, validated_arn):
            return jsonify(error="Invalid AWS Role ARN format provided."), 400

    if User.query.filter((User.username == username) | (User.email == email)).first():
        return jsonify(error=f'User with username "{username}" or email "{email}" already exists.'), 409

    try:
        new_user = User(username=username, email=email, aws_role_arn=validated_arn)
        new_user.set_password(password)
        db.session.add(new_user)
        db.session.commit()
        print(f"DEBUG: User registered successfully: {username}, ARN provided: {'Yes' if validated_arn else 'No'}")
        user_info = {'id': new_user.id, 'username': new_user.username, 'email': new_user.email}
        if new_user.aws_role_arn: user_info['aws_role_arn'] = new_user.aws_role_arn
        return jsonify(message="Registration successful", user=user_info), 201
    except Exception as e:
        db.session.rollback()
        print(f"ERROR: Database error during registration: {e}")
        traceback.print_exc()
        return jsonify(error="Registration failed due to a server error."), 500

@app.route('/login', methods=['POST'])
def login():
    # ... (Implementation unchanged) ...
    if g.user: return jsonify(error="Already logged in"), 400
    data = request.get_json()
    if not data: return jsonify(error="Invalid JSON payload"), 400
    username_or_email = data.get('username_or_email')
    password = data.get('password')
    if not username_or_email or not password: return jsonify(error="Username/email and password are required"), 400
    user = User.query.filter((User.username == username_or_email) | (User.email == username_or_email)).first()
    if user and user.check_password(password):
        session.clear()
        session['user_id'] = user.id
        g.user = user
        print(f"DEBUG: User logged in successfully: {user.username}")
        return jsonify(message="Login successful", user={'id': user.id, 'username': user.username, 'email': user.email})
    else:
        print(f"DEBUG: Failed login attempt for: {username_or_email}")
        return jsonify(error="Invalid username/email or password"), 401

@app.route('/logout', methods=['POST'])
@login_required
def logout():
    # ... (Implementation unchanged) ...
    user_id = g.user.id
    session.clear()
    g.user = None
    print(f"DEBUG: User {user_id} logged out.")
    return jsonify(message="Logout successful")

@app.route('/me', methods=['GET'])
@login_required
def get_current_user():
    # ... (Implementation unchanged, maybe add ARN if needed by frontend) ...
    user_data = {'id': g.user.id, 'username': g.user.username, 'email': g.user.email}
    if g.user.aws_role_arn: user_data['aws_role_arn'] = g.user.aws_role_arn
    return jsonify(user=user_data)


@app.route('/settings/aws_role', methods=['POST'])
@login_required
def update_aws_role():
    # ... (Implementation unchanged from previous example) ...
    data = request.get_json()
    if not data: return jsonify(error="Invalid JSON payload"), 400
    new_role_arn = data.get('aws_role_arn')
    validated_arn = None
    if new_role_arn and new_role_arn.strip():
        validated_arn = new_role_arn.strip()
        arn_pattern = r"^arn:aws:iam::\d{12}:role\/[\w+=,.@\/-]+$"
        if not re.match(arn_pattern, validated_arn):
            return jsonify(error="Invalid AWS Role ARN format."), 400
    elif isinstance(new_role_arn, str) and not new_role_arn.strip(): # Allow empty string to clear ARN
        validated_arn = None

    try:
        g.user.aws_role_arn = validated_arn
        db.session.commit()
        print(f"DEBUG User {g.user.id}: Updated AWS Role ARN.")
        return jsonify(message="AWS Role ARN updated successfully.", aws_role_arn=validated_arn)
    except Exception as e:
        db.session.rollback(); print(f"ERROR User {g.user.id}: Failed to update AWS Role ARN: {e}"); traceback.print_exc()
        return jsonify(error="Failed to update AWS Role ARN."), 500

# --- AssumeRole Helper Function ---
def get_s3_client_for_user(user_role_arn):
    """Assumes the user's role, returns temporary S3 client or None."""
    if not user_role_arn:
        print(f"ERROR User {g.user.id}: Cannot assume role, Role ARN is missing.")
        return None
    try:
        sts_client = boto3.client('sts', region_name=AWS_REGION)
        session_name = f"LakeInsightSession-{g.user.id}-{uuid.uuid4()}"
        print(f"DEBUG User {g.user.id}: Attempting to assume role: {user_role_arn} with session: {session_name}")
        assumed_role_object = sts_client.assume_role(
            RoleArn=user_role_arn, RoleSessionName=session_name)
        temp_credentials = assumed_role_object['Credentials']
        print(f"DEBUG User {g.user.id}: Successfully assumed role {user_role_arn}. Expires: {temp_credentials['Expiration']}")
        s3_temp_client = boto3.client(
            's3',
            aws_access_key_id=temp_credentials['AccessKeyId'],
            aws_secret_access_key=temp_credentials['SecretAccessKey'],
            aws_session_token=temp_credentials['SessionToken'],
            # region_name=AWS_REGION # Usually not needed for standard S3 ops
        )
        return s3_temp_client
    except ClientError as error:
        error_code = error.response.get('Error', {}).get('Code')
        print(f"ERROR User {g.user.id}: Could not assume role {user_role_arn}. Code: {error_code}. Message: {error}")
        if error_code == 'AccessDenied': print("-> Access Denied Check: Role ARN, Trust Policy (Principal = App User ARN), External ID?")
        return None
    except Exception as e:
         print(f"ERROR User {g.user.id}: Unexpected error assuming role {user_role_arn}: {e}"); traceback.print_exc()
         return None

# --- S3 Usage Logging ---
def add_s3_usage_log(user_id, s3_link):
    # ... (Implementation unchanged) ...
    pass

# --- Helper Functions ---
def format_bytes(size_bytes):
    # ... (Implementation unchanged) ...
    pass

def format_timestamp_ms(timestamp_ms):
    # ... (Implementation unchanged) ...
    pass

def extract_bucket_and_key(s3_url):
    # ... (Implementation unchanged) ...
    pass

# <<< BOLD INDICATOR START >>>
# Ensure all these helpers use the passed s3_client
def download_s3_file(s3_client, bucket, key, local_path):
    # ... (Implementation unchanged, verify it uses the passed client) ...
    pass

def parse_avro_file(file_path):
     # ... (Implementation unchanged, doesn't need s3_client) ...
    pass

def read_parquet_sample(s3_client, bucket, key, local_dir, num_rows=10):
     # ... (Implementation unchanged, verify it uses the passed client for download_s3_file) ...
    pass

def read_delta_checkpoint(s3_client, bucket, key, local_dir):
     # ... (Implementation unchanged, verify it uses the passed client for download_s3_file) ...
    pass

def read_delta_json_lines(s3_client, bucket, key, local_dir):
     # ... (Implementation unchanged, verify it uses the passed client for download_s3_file) ...
    pass
# <<< BOLD INDICATOR END >>>

def convert_bytes(obj):
    # ... (Implementation unchanged) ...
    pass

def _parse_delta_schema_string(schema_string):
    # ... (Implementation unchanged) ...
    pass

# --- Schema Comparison Helpers ---
# <<< BOLD INDICATOR START >>>
# Ensure these helpers use the passed s3_client
def get_iceberg_schema_for_version(s3_client, bucket_name, table_base_key, sequence_number, temp_dir):
     # ... (Implementation unchanged, verify it uses the passed client) ...
    pass

def get_delta_schema_for_version(s3_client, bucket_name, table_base_key, version_id, temp_dir):
     # ... (Implementation unchanged, verify it uses the passed client) ...
    pass
# <<< BOLD INDICATOR END >>>

def compare_schemas(schema1, schema2, version1_label="version1", version2_label="version2"):
     # ... (Implementation unchanged) ...
    pass


# --- Refactored S3 Endpoints ---

@app.route('/list_tables', methods=['GET'])
@login_required
def list_tables():
    s3_root_path = request.args.get('s3_root_path')
    if not s3_root_path: return jsonify({"error": "s3_root_path parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing List Tables Request for User {g.user.id}, Path {s3_root_path} ---")

    bucket_name, root_prefix, s3_client = None, None, None

    # <<< BOLD INDICATOR START >>>
    # AssumeRole Integration
    user_role_arn = g.user.aws_role_arn
    if not user_role_arn:
         return jsonify({"error": "AWS Role ARN not configured for this user."}), 400
    s3_client = get_s3_client_for_user(user_role_arn) # Get temporary client
    if not s3_client:
        return jsonify({"error": "Failed to access AWS Role. Please verify configuration in your AWS account and ensure it's correctly saved here."}), 403
    # <<< BOLD INDICATOR END >>>

    try:
        # <<< BOLD INDICATOR START >>>
        # Use the temporary s3_client for all operations below
        if not s3_root_path.endswith('/'): s3_root_path += '/'
        parsed_url = urlparse(s3_root_path)
        bucket_name = parsed_url.netloc
        root_prefix = unquote(parsed_url.path).lstrip('/')
        if not s3_root_path.startswith("s3://") or not bucket_name: raise ValueError(f"Invalid S3 root path: {s3_root_path}")

        discovered_tables = []
        paginator = s3_client.get_paginator('list_objects_v2') # Use temporary client
        print(f"DEBUG User {g.user.id}: Listing common prefixes under s3://{bucket_name}/{root_prefix} using assumed role")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=root_prefix, Delimiter='/')
        processed_prefixes = set()

        for page in page_iterator:
            if 'CommonPrefixes' in page:
                for common_prefix_obj in page['CommonPrefixes']:
                    prefix = common_prefix_obj.get('Prefix')
                    if not prefix or prefix in processed_prefixes: continue
                    processed_prefixes.add(prefix)
                    table_path = f"s3://{bucket_name}/{prefix}"
                    detected_type = "Unknown"
                    print(f"DEBUG User {g.user.id}: Checking prefix {prefix} for table type...")

                    # Check types using the temporary s3_client
                    try: # Delta Check
                        delta_check = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}_delta_log/", MaxKeys=1)
                        if 'Contents' in delta_check or delta_check.get('KeyCount', 0) > 0:
                            detected_type = "Delta"; discovered_tables.append({"path": table_path, "type": detected_type}); continue
                    except ClientError as e: # Handle potential access denied etc.
                         if e.response['Error']['Code'] != 'AccessDenied': print(f"Warning: S3 error checking Delta log for {prefix}: {e}")
                    except Exception as e: print(f"Warning: Unexpected error checking Delta log for {prefix}: {e}")

                    try: # Iceberg Check
                        iceberg_check = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}metadata/", MaxKeys=10)
                        if any(item.get('Key', '').endswith('.metadata.json') for item in iceberg_check.get('Contents', [])):
                             detected_type = "Iceberg"; discovered_tables.append({"path": table_path, "type": detected_type}); continue
                    except ClientError as e:
                         if e.response['Error']['Code'] != 'AccessDenied': print(f"Warning: S3 error checking Iceberg metadata for {prefix}: {e}")
                    except Exception as e: print(f"Warning: Unexpected error checking Iceberg metadata for {prefix}: {e}")

                    try: # Hudi Check
                        hudi_check = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}.hoodie/", MaxKeys=1)
                        if 'Contents' in hudi_check or hudi_check.get('KeyCount', 0) > 0:
                            detected_type = "Hudi"; discovered_tables.append({"path": table_path, "type": detected_type}); continue
                    except ClientError as e:
                         if e.response['Error']['Code'] != 'AccessDenied': print(f"Warning: S3 error checking Hudi metadata for {prefix}: {e}")
                    except Exception as e: print(f"Warning: Unexpected error checking Hudi metadata for {prefix}: {e}")

        end_time = time.time()
        print(f"INFO User {g.user.id}: Found {len(discovered_tables)} potential tables in {end_time - start_time:.2f} seconds.")
        return jsonify(discovered_tables)
        # <<< BOLD INDICATOR END >>>

    # --- Exception Handling (uses temporary s3_client context) ---
    except NoCredentialsError:
        print(f"ERROR User {g.user.id}: Boto3 NoCredentialsError after successful AssumeRole? Investigate.")
        return jsonify({"error": "AWS credentials issue occurred after assuming role."}), 500
    except (s3_client.exceptions.NoSuchBucket if s3_client else Exception) as e:
         print(f"ERROR User {g.user.id}: S3 Bucket not found or access denied via assumed role: {bucket_name}. Error: {e}")
         return jsonify({"error": f"S3 bucket '{bucket_name}' not found or access denied via assumed role."}), 404
    except (s3_client.exceptions.ClientError if s3_client else Exception) as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        print(f"ERROR User {g.user.id}: AWS ClientError listing tables via assumed role: {error_code} - {e}")
        if error_code == 'AccessDenied':
            return jsonify({"error": f"Access Denied via assumed role when listing path: {s3_root_path}. Check Role permissions."}), 403
        return jsonify({"error": f"AWS ClientError ({error_code}) listing tables via assumed role. Check Role permissions or path."}), 500
    except ValueError as e:
         print(f"ERROR User {g.user.id}: ValueError listing tables: {e}")
         return jsonify({"error": f"Invalid input path: {str(e)}"}), 400
    except Exception as e:
         print(f"ERROR User {g.user.id}: An unexpected error occurred while listing tables: {e}")
         traceback.print_exc()
         return jsonify({"error": f"An unexpected server error occurred: {str(e)}"}), 500


@app.route('/Iceberg', methods=['GET'])
@login_required
def iceberg_details():
    s3_url = request.args.get('s3_url')
    if not s3_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Iceberg Request for User {g.user.id}, URL {s3_url} ---")

    bucket_name, table_base_key, s3_client = None, None, None

    # <<< BOLD INDICATOR START >>>
    # AssumeRole Integration
    user_role_arn = g.user.aws_role_arn
    if not user_role_arn: return jsonify({"error": "AWS Role ARN not configured for this user."}), 400
    s3_client = get_s3_client_for_user(user_role_arn) # Get temporary client
    if not s3_client: return jsonify({"error": "Failed to access AWS Role. Please verify configuration."}), 403
    # <<< BOLD INDICATOR END >>>

    try:
        # <<< BOLD INDICATOR START >>>
        # Use the temporary s3_client for all operations below
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)
        if not table_base_key.endswith('/'): table_base_key += '/'
        metadata_prefix = table_base_key + "metadata/"

        with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
            print(f"DEBUG User {g.user.id}: Using temporary directory: {temp_dir}")
            # ... [Existing Iceberg logic, ensuring all helpers and direct Boto3 calls
            #      use the temporary s3_client] ...
            # Example:
            # list_response = s3_client.list_objects_v2(...)
            # download_s3_file(s3_client, ...)
            # manifest_list_head = s3_client.head_object(...)
            # sample_data = read_parquet_sample(s3_client, ...)
            result = {} # Placeholder for your result assembly

        add_s3_usage_log(g.user.id, s3_url)
        result_serializable = convert_bytes(result) # Your result assembly needed here
        end_time = time.time()
        print(f"--- Iceberg Request Completed for User {g.user.id} in {end_time - start_time:.2f} seconds ---")
        return jsonify(result_serializable), 200
        # <<< BOLD INDICATOR END >>>

    # Exception Handling (uses temporary s3_client context)
    except (NoCredentialsError, s3_client.exceptions.NoSuchBucket if s3_client else Exception, s3_client.exceptions.ClientError if s3_client else Exception, FileNotFoundError, ValueError, Exception) as e:
        # ... (Consolidated Exception Handling as shown in list_tables) ...
        print(f"ERROR User {g.user.id}: Error during Iceberg details fetch: {e}")
        status_code=500; error_message=f"Error processing Iceberg table: {str(e)}" # Determine code based on e
        return jsonify({"error": error_message}), status_code

@app.route('/Delta', methods=['GET'])
@login_required
def delta_details():
    s3_url = request.args.get('s3_url')
    if not s3_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Delta Request for User {g.user.id}, URL {s3_url} ---")

    bucket_name, table_base_key, delta_log_prefix, s3_client = None, None, None, None

    # <<< BOLD INDICATOR START >>>
    # AssumeRole Integration
    user_role_arn = g.user.aws_role_arn
    if not user_role_arn: return jsonify({"error": "AWS Role ARN not configured for this user."}), 400
    s3_client = get_s3_client_for_user(user_role_arn) # Get temporary client
    if not s3_client: return jsonify({"error": "Failed to access AWS Role. Please verify configuration."}), 403
    # <<< BOLD INDICATOR END >>>

    try:
        # <<< BOLD INDICATOR START >>>
        # Use the temporary s3_client for all operations below
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)
        if not table_base_key.endswith('/'): table_base_key += '/'
        delta_log_prefix = table_base_key + "_delta_log/"
        print(f"INFO User {g.user.id}: Processing Delta Lake table at: s3://{bucket_name}/{table_base_key}")

        with tempfile.TemporaryDirectory(prefix="delta_meta_") as temp_dir:
            print(f"DEBUG User {g.user.id}: Using temporary directory: {temp_dir}")
            # ... [Existing Delta logic, ensuring all helpers and direct Boto3 calls
            #      use the temporary s3_client] ...
            # Example:
            # list_response = s3_client.list_objects_v2(...)
            # download_s3_file(s3_client, ...)
            # all_checkpoint_actions.extend(read_delta_checkpoint(s3_client, ...))
            # actions = read_delta_json_lines(s3_client, ...)
            # sample_data = read_parquet_sample(s3_client, ...)
            result = {} # Placeholder for your result assembly

        add_s3_usage_log(g.user.id, s3_url)
        result_serializable = json.loads(json.dumps(convert_bytes(result), default=str)) # Your result assembly needed here
        end_time = time.time()
        print(f"--- Delta Request Completed for User {g.user.id} in {end_time - start_time:.2f} seconds ---")
        return jsonify(result_serializable), 200
        # <<< BOLD INDICATOR END >>>

    # Exception Handling (uses temporary s3_client context)
    except (NoCredentialsError, s3_client.exceptions.NoSuchBucket if s3_client else Exception, s3_client.exceptions.ClientError if s3_client else Exception, FileNotFoundError, ValueError, Exception) as e:
        # ... (Consolidated Exception Handling as shown in list_tables) ...
        print(f"ERROR User {g.user.id}: Error during Delta details fetch: {e}")
        status_code=500; error_message=f"Error processing Delta table: {str(e)}" # Determine code based on e
        return jsonify({"error": error_message}), status_code


# --- Schema Comparison Endpoints ---
@app.route('/compare_schema/Iceberg', methods=['GET'])
@login_required
def compare_iceberg_schema_endpoint():
    # ... (Parameter validation: s3_url, seq1, seq2) ...
    start_time = time.time()
    print(f"\n--- Processing Iceberg Schema Comparison for User {g.user.id} ---")
    bucket_name, table_base_key, s3_client, temp_dir_obj = None, None, None, None

    # <<< BOLD INDICATOR START >>>
    # AssumeRole Integration
    user_role_arn = g.user.aws_role_arn
    if not user_role_arn: return jsonify({"error": "AWS Role ARN not configured."}), 400
    s3_client = get_s3_client_for_user(user_role_arn)
    if not s3_client: return jsonify({"error": "Failed to assume AWS Role."}), 403
    # <<< BOLD INDICATOR END >>>

    temp_dir_obj = None # Initialize
    try:
        # <<< BOLD INDICATOR START >>>
        # ... (Parse seq1, seq2, extract bucket/key) ...
        seq1 = int(request.args.get('seq1')); seq2 = int(request.args.get('seq2')) # Example
        bucket_name, table_base_key = extract_bucket_and_key(request.args.get('s3_url')) # Example
        if not table_base_key.endswith('/'): table_base_key += '/'

        temp_dir_obj = tempfile.TemporaryDirectory(prefix="iceberg_schema_comp_")
        temp_dir = temp_dir_obj.name

        # --- Use temporary s3_client for helpers ---
        schema1 = get_iceberg_schema_for_version(s3_client, bucket_name, table_base_key, seq1, temp_dir)
        schema2 = get_iceberg_schema_for_version(s3_client, bucket_name, table_base_key, seq2, temp_dir)
        schema_diff = compare_schemas(schema1, schema2, f"seq {seq1}", f"seq {seq2}")

        # ... (Assemble result as before) ...
        result = {
            "table_type": "Iceberg", "location": request.args.get('s3_url'),
            "version1": {"identifier_type": "sequence_number", "identifier": seq1, "schema": schema1},
            "version2": {"identifier_type": "sequence_number", "identifier": seq2, "schema": schema2},
            "schema_comparison": schema_diff }

        result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))
        end_time = time.time()
        print(f"--- Iceberg Schema Comparison Completed for User {g.user.id} in {end_time - start_time:.2f} sec ---")
        return jsonify(result_serializable), 200
        # <<< BOLD INDICATOR END >>>
    except Exception as e:
        # ... (Consolidated Exception Handling) ...
        print(f"ERROR User {g.user.id}: Error comparing Iceberg schemas: {e}")
        status_code=500 # Determine code based on e
        return jsonify({"error": f"Error comparing Iceberg schemas: {str(e)}"}), status_code
    finally:
        if temp_dir_obj:
             try: temp_dir_obj.cleanup(); print(f"DEBUG: Cleaned up {temp_dir_obj.name}")
             except Exception as cl_err: print(f"ERROR: Cleanup failed for {temp_dir_obj.name}: {cl_err}")


@app.route('/compare_schema/Delta', methods=['GET'])
@login_required
def compare_delta_schema_endpoint():
    # ... (Parameter validation: s3_url, v1, v2) ...
    start_time = time.time()
    print(f"\n--- Processing Delta Schema Comparison for User {g.user.id} ---")
    bucket_name, table_base_key, s3_client, temp_dir_obj = None, None, None, None

    # <<< BOLD INDICATOR START >>>
    # AssumeRole Integration
    user_role_arn = g.user.aws_role_arn
    if not user_role_arn: return jsonify({"error": "AWS Role ARN not configured."}), 400
    s3_client = get_s3_client_for_user(user_role_arn)
    if not s3_client: return jsonify({"error": "Failed to assume AWS Role."}), 403
    # <<< BOLD INDICATOR END >>>

    temp_dir_obj = None # Initialize
    try:
        # <<< BOLD INDICATOR START >>>
        # ... (Parse v1, v2, extract bucket/key) ...
        v1 = int(request.args.get('v1')); v2 = int(request.args.get('v2')) # Example
        bucket_name, table_base_key = extract_bucket_and_key(request.args.get('s3_url')) # Example
        if not table_base_key.endswith('/'): table_base_key += '/'

        temp_dir_obj = tempfile.TemporaryDirectory(prefix="delta_schema_comp_")
        temp_dir = temp_dir_obj.name

        # --- Use temporary s3_client for helpers ---
        schema1 = get_delta_schema_for_version(s3_client, bucket_name, table_base_key, v1, temp_dir)
        schema2 = get_delta_schema_for_version(s3_client, bucket_name, table_base_key, v2, temp_dir)
        schema_diff = compare_schemas(schema1, schema2, f"version {v1}", f"version {v2}")

        # ... (Assemble result as before) ...
        result = {
            "table_type": "Delta", "location": request.args.get('s3_url'),
            "version1": {"identifier_type": "version_id", "identifier": v1, "schema": schema1},
            "version2": {"identifier_type": "version_id", "identifier": v2, "schema": schema2},
            "schema_comparison": schema_diff }

        result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))
        end_time = time.time()
        print(f"--- Delta Schema Comparison Completed for User {g.user.id} in {end_time - start_time:.2f} sec ---")
        return jsonify(result_serializable), 200
        # <<< BOLD INDICATOR END >>>
    except Exception as e:
        # ... (Consolidated Exception Handling) ...
        print(f"ERROR User {g.user.id}: Error comparing Delta schemas: {e}")
        status_code=500 # Determine code based on e
        return jsonify({"error": f"Error comparing Delta schemas: {str(e)}"}), status_code
    finally:
        if temp_dir_obj:
             try: temp_dir_obj.cleanup(); print(f"DEBUG: Cleaned up {temp_dir_obj.name}")
             except Exception as cl_err: print(f"ERROR: Cleanup failed for {temp_dir_obj.name}: {cl_err}")


# --- Recent Usage Endpoint (Unchanged) ---
@app.route('/recent_usage', methods=['GET'])
@login_required
def recent_usage():
    # ... (Implementation unchanged) ...
    pass

# --- Ping Endpoint (Unchanged) ---
@app.route('/ping', methods=['GET'])
def root():
    # ... (Implementation unchanged) ...
    pass

# --- Main Execution Block (Updated Checks) ---
if __name__ == '__main__':
    with app.app_context():
        print("Initializing database...")
        try:
            db.create_all()
            print("Database tables checked/created.")
        except Exception as e:
            print(f"ERROR: Could not connect or initialize DB '{app.config['SQLALCHEMY_DATABASE_URI']}': {e}")
            print("Ensure DB exists and connection details are correct.")

    # <<< BOLD INDICATOR START >>>
    # Configuration Warnings for Render environment
    print("\n--- Configuration Checks ---")
    if not app.config['SECRET_KEY'] or len(app.config['SECRET_KEY']) < 16 :
       print("WARNING: Flask SECRET_KEY is missing, short, or insecure. Set FLASK_SECRET_KEY env var.")
    if not os.getenv("ENCRYPTION_KEY"):
        print("WARNING: ENCRYPTION_KEY is not set. S3 URL encryption will fail.")
    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
         print("CRITICAL WARNING: AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY env vars not found.")
         print("                 STS AssumeRole calls WILL likely fail. Configure these in Render Env.")
    else:
         print("INFO: AWS Credentials found in environment (used for STS).")
    if not os.getenv("AWS_REGION"):
         print(f"WARNING: AWS_REGION env var not found. Defaulting to '{AWS_REGION}'.")
    else:
         print(f"INFO: AWS Region set to '{AWS_REGION}'.")
    print("--------------------------\n")
    # <<< BOLD INDICATOR END >>>

    print("Starting Flask server...")
    port = int(os.environ.get("PORT", 5000))
    # Set debug based on FLASK_ENV or default to False for safety
    debug_mode = os.getenv('FLASK_ENV', 'production') == 'development'
    print(f" * Running on http://0.0.0.0:{port}/ (Press CTRL+C to quit)")
    print(f" * Debug mode: {'on' if debug_mode else 'off'}")
    # Render uses gunicorn/uvicorn via Procfile/Start Command in production.
    # This app.run() is mainly for local development.
    # Use debug=False if deploying via `python app.py` without gunicorn (not recommended)
    app.run(debug=debug_mode, host='0.0.0.0', port=port)