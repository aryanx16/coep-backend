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
from botocore.exceptions import NoCredentialsError
import google.generativeai as genai
from flask_sqlalchemy import SQLAlchemy
import bcrypt
import functools
from cryptography.fernet import Fernet, InvalidToken
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
# Configure Gemini API
genai.configure(api_key=API_KEY)
app = Flask(__name__)

CORS(app)

# --- Flask Configuration ---
app.config['SECRET_KEY'] = os.getenv("FLASK_SECRET_KEY")
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DATABASE_URL")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# Optional: Configure session cookie parameters for security
app.config['SESSION_COOKIE_SECURE'] = True  # Send cookie only over HTTPS (Requires HTTPS setup)
app.config['SESSION_COOKIE_HTTPONLY'] = True # Prevent JS access to cookie
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax' # Basic CSRF protection ('Strict' is better but can break cross-origin links)

if not app.config['SECRET_KEY']:
    raise ValueError("No FLASK_SECRET_KEY set for Flask application")
if not app.config['SQLALCHEMY_DATABASE_URI']:
    raise ValueError("No DATABASE_URL set for Flask application")

# --- Encryption Setup ---
ENCRYPTION_KEY_STR = os.getenv("ENCRYPTION_KEY")
if not ENCRYPTION_KEY_STR:
    raise ValueError("No ENCRYPTION_KEY set for URL encryption")
try:
    fernet = Fernet(ENCRYPTION_KEY_STR.encode()) # Create Fernet instance
except Exception as e:
    raise ValueError(f"Invalid ENCRYPTION_KEY format: {e}")

# --- Initialize SQLAlchemy ---
db = SQLAlchemy(app)

# --- Configuration ---

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1") # Set default region (e.g., Mumbai)

# --- Database Models ---
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    aws_role_arn = db.Column(db.String(255), nullable=True)
    usages = db.relationship('S3Usage', backref='user', lazy=True)

    def set_password(self, password):
        self.password_hash = bcrypt.hashpw(
            password.encode('utf-8'), bcrypt.gensalt()
        ).decode('utf-8')

    def check_password(self, password):
        if not self.password_hash: return False # Handle case where hash might be null
        try:
            return bcrypt.checkpw(
                password.encode('utf-8'), self.password_hash.encode('utf-8')
            )
        except ValueError: # Handle potential errors if hash is malformed
             return False

class S3Usage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    # Store encrypted URL - might need more space than default String
    s3_bucket_link_encrypted = db.Column(db.Text, nullable=False)
    timestamp = db.Column(db.DateTime, nullable=False, default=datetime.datetime.utcnow)

# --- Encryption Helper Functions ---
def encrypt_data(data_string):
    """Encrypts a string using the global Fernet key."""
    if not isinstance(data_string, str):
        raise TypeError("Input must be a string")
    return fernet.encrypt(data_string.encode()).decode() # Encrypt and return as string

def decrypt_data(encrypted_string):
    """Decrypts a string using the global Fernet key. Returns None on failure."""
    if not isinstance(encrypted_string, str):
        # If data in DB isn't string, handle appropriately
        print(f"Warning: Attempting to decrypt non-string data: {type(encrypted_string)}")
        return None
    try:
        return fernet.decrypt(encrypted_string.encode()).decode()
    except InvalidToken:
        print("ERROR: Failed to decrypt data - Invalid Token (key mismatch or corrupted data?)")
        return None # Or raise an error, or return a specific marker
    except Exception as e:
        print(f"ERROR: Decryption failed: {e}")
        return None

# --- Authentication Middleware/Hooks ---
@app.before_request
def load_logged_in_user():
    """If user_id is stored in the session, load the user object from DB into g.user."""
    user_id = session.get('user_id')
    g.user = User.query.get(user_id) if user_id else None

def login_required(func):
    """Decorator that returns 401 JSON error if user is not logged in."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if g.user is None:
            return jsonify(error="Authentication required. Please log in."), 401
        return func(*args, **kwargs)
    return wrapper

# --- Authentication Routes (JSON API) ---

@app.route('/register', methods=['POST'])
def register():
    if g.user: # User is already logged in
        return jsonify(error="Already logged in"), 400

    data = request.get_json()
    if not data:
        return jsonify(error="Invalid JSON payload"), 400

    username = data.get('username')
    email = data.get('email')
    password = data.get('password')
    # --- Get optional aws_role_arn ---
    aws_role_arn = data.get('aws_role_arn', None) # Default to None if not provided

    if not username or not email or not password:
        return jsonify(error="Username, email, and password are required"), 400

    # --- Validate optional aws_role_arn format ---
    validated_arn = None
    if aws_role_arn and aws_role_arn.strip(): # Check if provided and not just whitespace
        validated_arn = aws_role_arn.strip()
        # Basic ARN format validation (adjust regex as needed for stricter validation)
        # Checks for: arn:aws:iam::<12 digits>:role/<role name path>
        arn_pattern = r"^arn:aws:iam::\d{12}:role\/[\w+=,.@\/-]+$"
        if not re.match(arn_pattern, validated_arn):
            print(f"DEBUG: Invalid ARN format provided during registration: {validated_arn}")
            return jsonify(error="Invalid AWS Role ARN format provided."), 400
    # --- End ARN Validation ---

    # Check if user already exists (by username or email)
    if User.query.filter((User.username == username) | (User.email == email)).first():
        return jsonify(error=f'User with username "{username}" or email "{email}" already exists.'), 409 # 409 Conflict

    try:
        # --- Create user, including the validated ARN (which might be None) ---
        new_user = User(
            username=username,
            email=email,
            aws_role_arn=validated_arn # Pass the validated ARN or None
        )
        new_user.set_password(password) # Set password hash
        # ----------------------------------------------------------------------

        db.session.add(new_user)
        db.session.commit()
        print(f"DEBUG: User registered successfully: {username}, ARN provided: {'Yes' if validated_arn else 'No'}")

        # Return limited user info upon successful registration
        user_info = {
            'id': new_user.id,
            'username': new_user.username,
            'email': new_user.email
        }
        # Include ARN in response only if it was set
        if new_user.aws_role_arn:
            user_info['aws_role_arn'] = new_user.aws_role_arn

        return jsonify(
            message="Registration successful",
            user=user_info
        ), 201 # 201 Created

    except Exception as e:
        db.session.rollback()
        print(f"ERROR: Database error during registration: {e}")
        traceback.print_exc()
        return jsonify(error="Registration failed due to a server error."), 500

@app.route('/login', methods=['POST'])
def login():
    if g.user:
        return jsonify(error="Already logged in"), 400

    data = request.get_json()
    if not data:
        return jsonify(error="Invalid JSON payload"), 400

    username_or_email = data.get('username_or_email')
    password = data.get('password')

    if not username_or_email or not password:
         return jsonify(error="Username/email and password are required"), 400

    # Find user
    user = User.query.filter((User.username == username_or_email) | (User.email == username_or_email)).first()

    if user and user.check_password(password):
        # Password matches - Log user in
        session.clear()
        session['user_id'] = user.id
        g.user = user # Update g for this request
        print(f"DEBUG: User logged in successfully: {user.username}")
        # Return user info (excluding password hash)
        return jsonify(
            message="Login successful",
            user={'id': user.id, 'username': user.username, 'email': user.email}
        )
    else:
        # Invalid credentials
        print(f"DEBUG: Failed login attempt for: {username_or_email}")
        return jsonify(error="Invalid username/email or password"), 401 # 401 Unauthorized

@app.route('/logout', methods=['POST'])
@login_required # Must be logged in to log out
def logout():
    user_id = g.user.id # Log who is logging out before clearing
    session.clear()
    g.user = None
    print(f"DEBUG: User {user_id} logged out.")
    return jsonify(message="Logout successful")

@app.route('/me', methods=['GET'])
@login_required # Get current user info
def get_current_user():
    """Returns information about the currently logged-in user."""
    return jsonify(
        user={'id': g.user.id, 'username': g.user.username, 'email': g.user.email}
    )

def get_s3_client_for_user(user_role_arn):
    """
    Assumes the user's specified role and returns a temporary S3 client.
    Uses the application's default credentials (from Render env vars) to call STS.
    Returns None on failure.
    """
    if not user_role_arn:
        print(f"ERROR User {g.user.id}: Cannot assume role, Role ARN is missing.")
        return None

    try:
        # 1. Create STS client using the application's credentials (from Render env vars)
        # Boto3 automatically finds credentials from env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        sts_client = boto3.client('sts', region_name=AWS_REGION) # Use default region from config

        session_name = f"LakeInsightSession-{g.user.id}-{uuid.uuid4()}"
        print(f"DEBUG User {g.user.id}: Attempting to assume role: {user_role_arn} with session: {session_name}")

        # 2. Assume the role in the user's account
        assumed_role_object = sts_client.assume_role(
            RoleArn=user_role_arn,
            RoleSessionName=session_name
            # Add ExternalId=user_external_id here if you implement external IDs
        )

        # 3. Extract temporary credentials
        temp_credentials = assumed_role_object['Credentials']
        print(f"DEBUG User {g.user.id}: Successfully assumed role {user_role_arn}. Credentials expire at {temp_credentials['Expiration']}")

        # 4. Create S3 client using the temporary credentials
        s3_temp_client = boto3.client(
            's3',
            aws_access_key_id=temp_credentials['AccessKeyId'],
            aws_secret_access_key=temp_credentials['SecretAccessKey'],
            aws_session_token=temp_credentials['SessionToken'],
            # Use the default region, or specify user bucket region if needed/known
            # region_name=AWS_REGION
            # For S3, region often doesn't matter for basic operations unless using regional features like acceleration
        )
        return s3_temp_client

    except ClientError as error:
        error_code = error.response.get('Error', {}).get('Code')
        print(f"ERROR User {g.user.id}: Could not assume role {user_role_arn}. Code: {error_code}. Message: {error}")
        if error_code == 'AccessDenied':
             print("-> Access Denied: Check Role ARN validity, Trust Policy in customer's account (Principal must be app's IAM User ARN), and External ID (if used).")
        # Add more specific error handling if needed
        return None
    except Exception as e:
         print(f"ERROR User {g.user.id}: Unexpected error assuming role {user_role_arn}. Error: {e}")
         traceback.print_exc()
         return None

# --- Helper to Add Encrypted S3 Usage Record ---
def add_s3_usage_log(user_id, s3_link):
    """
    Encrypts the S3 link and adds a record to the S3Usage table.
    If a record for the same user and S3 link already exists,
    it updates the timestamp to the current time instead of inserting a duplicate.
    """
    if not user_id or not s3_link:
        print("Warning: Cannot log S3 usage - missing user_id or s3_link.")
        return
    try:
        # Encrypt the link first, as this is what's stored and queried
        encrypted_link = encrypt_data(s3_link)

        # Check if an entry already exists for this user and encrypted link
        existing_usage = S3Usage.query.filter_by(
            user_id=user_id,
            s3_bucket_link_encrypted=encrypted_link
        ).first() # Use first() to get one or None

        if existing_usage:
            # --- UPDATE Existing Record ---
            print(f"DEBUG: Updating timestamp for existing S3 usage record for user {user_id}, link starts with: {s3_link[:50]}...")
            existing_usage.timestamp = datetime.datetime.utcnow() # Update timestamp
            db.session.commit() # Commit the change
            print(f"DEBUG: Timestamp updated successfully.")
        else:
            # --- INSERT New Record ---
            print(f"DEBUG: Logging new encrypted S3 usage for user {user_id}, link starts with: {s3_link[:50]}...")
            usage = S3Usage(user_id=user_id, s3_bucket_link_encrypted=encrypted_link)
            # timestamp defaults to datetime.datetime.utcnow on creation
            db.session.add(usage)
            db.session.commit() # Commit the new record
            print(f"DEBUG: New S3 usage logged successfully.")

    except Exception as e:
        db.session.rollback() # Rollback in case of any error during query or commit
        print(f"ERROR: Failed to log or update S3 usage for user {user_id}, link {s3_link}: {e}")
        traceback.print_exc()

# --- Helper Functions ---
def get_gemini_response(prompt):
    model = genai.GenerativeModel("gemini-2.0-flash-lite")  # Use "gemini-pro" for text-based tasks
    response = model.generate_content(prompt)
    return response.text

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

# --- Schema Comparison Helper Functions ---

def get_iceberg_schema_for_version(s3_client, bucket_name, table_base_key, sequence_number, temp_dir):
    """Fetches the Iceberg schema associated with a specific sequence number."""
    print(f"DEBUG: Getting Iceberg schema for sequence_number: {sequence_number}")
    metadata_prefix = table_base_key + "metadata/"

    # 1. Find latest metadata.json (reuse logic)
    list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
    if 'Contents' not in list_response:
        raise FileNotFoundError(f"No objects found under metadata prefix: {metadata_prefix}")

    metadata_files = []
    s3_object_details = {obj['Key']: {'size': obj['Size'], 'last_modified': obj['LastModified']} for obj in list_response.get('Contents', [])}
    for obj_key, obj_info in s3_object_details.items():
        if obj_key.endswith('.metadata.json'):
             match = re.search(r'(v?\d+)-[a-f0-9-]+\.metadata\.json$', os.path.basename(obj_key))
             version_num = 0
             if match:
                 try: version_num = int(match.group(1).lstrip('v'))
                 except ValueError: pass
             metadata_files.append({'key': obj_key, 'version': version_num, 'last_modified': obj_info['last_modified']})

    if not metadata_files:
        raise FileNotFoundError(f"No *.metadata.json files found under {metadata_prefix}")

    metadata_files.sort(key=lambda x: (x['version'], x['last_modified']), reverse=True)
    latest_metadata_key = metadata_files[0]['key']
    print(f"DEBUG: Reading latest metadata file: {latest_metadata_key} to find snapshot info")
    local_latest_metadata_path = os.path.join(temp_dir, os.path.basename(latest_metadata_key).replace('%', '_'))

    download_s3_file(s3_client, bucket_name, latest_metadata_key, local_latest_metadata_path)
    with open(local_latest_metadata_path, 'r') as f: latest_meta = json.load(f)

    # 2. Find the snapshot for the target sequence number
    target_snapshot = None
    snapshots = latest_meta.get("snapshots", [])
    for snapshot in snapshots:
        if snapshot.get("sequence-number") == sequence_number:
            target_snapshot = snapshot
            break

    if not target_snapshot:
        # TODO: Potentially search older metadata files if needed, but complex.
        raise ValueError(f"Snapshot with sequence number {sequence_number} not found in latest metadata file {latest_metadata_key}.")

    schema_id = target_snapshot.get("schema-id")
    if schema_id is None:
        # Fallback for older metadata formats that might only have 'schema' at top level
        # This assumes the schema didn't change if schema-id is missing in the snapshot
        current_schema_id = latest_meta.get("current-schema-id")
        if current_schema_id is not None:
             schema_id = current_schema_id
        else: # Very old format? Use top-level schema if present.
             print(f"Warning: Snapshot {target_snapshot.get('snapshot-id')} has no schema-id, trying top-level schema.")
             schema = latest_meta.get("schema")
             if schema: return schema
             raise ValueError(f"Cannot determine schema for snapshot {target_snapshot.get('snapshot-id')} (sequence number {sequence_number}). Missing schema-id and no top-level schema.")


    print(f"DEBUG: Snapshot found (ID: {target_snapshot.get('snapshot-id')}), looking for schema-id: {schema_id}")

    # 3. Find the schema definition in the metadata
    schemas = latest_meta.get("schemas", [])
    target_schema = None
    for schema in schemas:
        if schema.get("schema-id") == schema_id:
            target_schema = schema
            break

    if not target_schema:
         # Handle case where only top-level schema exists (older format?)
         if schema_id == latest_meta.get("current-schema-id") and latest_meta.get("schema"):
             print(f"DEBUG: Schema ID {schema_id} not in 'schemas' list, using top-level 'schema'.")
             target_schema = latest_meta.get("schema")
             # Add schema-id if missing for consistency
             if target_schema and "schema-id" not in target_schema:
                 target_schema["schema-id"] = schema_id

    if not target_schema:
        raise ValueError(f"Schema definition with schema-id {schema_id} not found in metadata file {latest_metadata_key}.")

    print(f"DEBUG: Found schema for sequence_number {sequence_number}")
    return target_schema


def get_delta_schema_for_version(s3_client, bucket_name, table_base_key, version_id, temp_dir):
    """Fetches the Delta schema active at a specific version ID."""
    print(f"DEBUG: Getting Delta schema for version_id: {version_id}")
    delta_log_prefix = table_base_key + "_delta_log/"

    # --- 1. Find Delta Log Files --- (Simplified listing, only need up to version_id)
    log_files_raw = []
    continuation_token = None
    while True:
        list_kwargs = {'Bucket': bucket_name, 'Prefix': delta_log_prefix}
        if continuation_token: list_kwargs['ContinuationToken'] = continuation_token
        try: list_response = s3_client.list_objects_v2(**list_kwargs)
        except s3_client.exceptions.NoSuchKey: list_response = {}
        except s3_client.exceptions.ClientError as list_err:
            print(f"ERROR: ClientError listing objects under {delta_log_prefix}: {list_err}")
            raise  # Re-raise to be caught by the endpoint
        if 'Contents' not in list_response and not log_files_raw:
            raise FileNotFoundError(f"Delta log prefix '{delta_log_prefix}' is empty or inaccessible.")

        log_files_raw.extend(list_response.get('Contents', []))
        if list_response.get('IsTruncated'): continuation_token = list_response.get('NextContinuationToken')
        else: break
    print(f"DEBUG: Found {len(log_files_raw)} total objects under delta log prefix (pre-filter).")

    # --- Filter and Identify Relevant Files ---
    json_commits = {}
    checkpoint_files = {}
    json_pattern = re.compile(r"(\d+)\.json$")
    checkpoint_pattern = re.compile(r"(\d+)\.checkpoint(?:\.(\d+)\.(\d+))?\.parquet$")

    for obj in log_files_raw:
        key = obj['Key']
        filename = os.path.basename(key)
        if (json_match := json_pattern.match(filename)):
             v = int(json_match.group(1))
             if v <= version_id: json_commits[v] = {'key': key}
        elif (cp_match := checkpoint_pattern.match(filename)):
            v, part_num, num_parts = int(cp_match.group(1)), cp_match.group(2), cp_match.group(3)
            if v <= version_id: # Only consider checkpoints at or before the target version
                part_num = int(part_num) if part_num else 1
                num_parts = int(num_parts) if num_parts else 1
                if v not in checkpoint_files: checkpoint_files[v] = {'num_parts': num_parts, 'parts': {}}
                checkpoint_files[v]['parts'][part_num] = {'key': key}
                if checkpoint_files[v]['num_parts'] != num_parts and cp_match.group(2):
                    checkpoint_files[v]['num_parts'] = max(checkpoint_files[v]['num_parts'], num_parts)

    # --- Determine starting point (Closest Checkpoint <= version_id) ---
    start_process_version = 0
    checkpoint_version_used = -1
    relevant_checkpoint_versions = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']], reverse=True)

    if relevant_checkpoint_versions:
        checkpoint_version_used = relevant_checkpoint_versions[0]
        print(f"INFO: Found usable checkpoint version {checkpoint_version_used} <= {version_id}")
        start_process_version = checkpoint_version_used + 1
    else:
        print("INFO: No usable checkpoint found <= target version. Processing JSON logs from version 0.")

    # --- Process Checkpoint and JSON Commits Incrementally ---
    active_metadata = None # Keep track of the last seen metadata

    # --- Load State from Checkpoint if applicable ---
    if checkpoint_version_used > -1:
        print(f"INFO: Reading state from checkpoint version {checkpoint_version_used}")
        cp_info = checkpoint_files[checkpoint_version_used]
        try:
            all_checkpoint_actions = []
            for part_num in sorted(cp_info['parts'].keys()):
                part_key = cp_info['parts'][part_num]['key']
                all_checkpoint_actions.extend(read_delta_checkpoint(s3_client, bucket_name, part_key, temp_dir))

            # Find metadata within the checkpoint
            for action in all_checkpoint_actions:
                if 'metaData' in action and action['metaData']:
                    active_metadata = action['metaData']
                    print(f"DEBUG: Found metaData in checkpoint {checkpoint_version_used}")
                    break # Assume only one metaData per checkpoint/commit
            # Protocol isn't needed for schema comparison, skipping protocol search

        except Exception as cp_read_err:
            print(f"ERROR: Failed to read/process checkpoint {checkpoint_version_used}: {cp_read_err}. Trying from version 0.")
            start_process_version = 0 # Reset start version
            active_metadata = None    # Reset metadata

    # --- Process JSON Commits Incrementally ---
    versions_to_process = sorted([v for v in json_commits if v >= start_process_version and v <= version_id])
    print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} up to {version_id}...")

    for version in versions_to_process:
        commit_file_info = json_commits[version]
        commit_key = commit_file_info['key']
        print(f"DEBUG: Processing version {version} ({commit_key})...")
        try:
            actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
            found_meta_in_commit = False
            for action in actions:
                if 'metaData' in action and action['metaData']:
                    active_metadata = action['metaData'] # Update with the latest metadata
                    found_meta_in_commit = True
                    print(f"DEBUG: Updated active metaData from version {version}")
                    break # Assume only one metaData per commit
            # No need to process add/remove for schema comparison
        except Exception as json_proc_err:
            print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
            # Decide if we should stop or continue? For schema, maybe stop.
            raise ValueError(f"Failed to process required commit file for version {version}. Cannot determine schema.") from json_proc_err

    # --- Parse the final active metadata ---
    if not active_metadata:
         raise ValueError(f"Could not find table metadata (metaData action) at or before version {version_id}.")

    schema_string = active_metadata.get("schemaString")
    if not schema_string:
         raise ValueError(f"Metadata found for version {version_id}, but it does not contain a 'schemaString'.")

    parsed_schema = _parse_delta_schema_string(schema_string)
    if not parsed_schema:
         raise ValueError(f"Failed to parse schema string found for version {version_id}.")

    # Add schema-id from metadata if it helps comparison consistency
    parsed_schema["schema-id"] = active_metadata.get("id", f"delta-v{version_id}") # Use version as pseudo-id if needed

    print(f"DEBUG: Successfully obtained and parsed schema for version_id {version_id}")
    return parsed_schema


def compare_schemas(schema1, schema2, version1_label="version1", version2_label="version2"):
    """Compares two schema dictionaries (Iceberg-like format) and returns differences."""
    if not schema1 or not schema1.get("fields"):
        return {"error": f"Schema for {version1_label} is missing or invalid."}
    if not schema2 or not schema2.get("fields"):
        return {"error": f"Schema for {version2_label} is missing or invalid."}

    fields1 = schema1.get("fields", [])
    fields2 = schema2.get("fields", [])

    fields1_map = {f['name']: f for f in fields1}
    fields2_map = {f['name']: f for f in fields2}

    added = []
    removed = []
    modified = []

    # Check for added fields (in schema2 but not in schema1)
    for name, field2 in fields2_map.items():
        if name not in fields1_map:
            added.append(field2)

    # Check for removed and modified fields (in schema1)
    for name, field1 in fields1_map.items():
        if name not in fields2_map:
            removed.append(field1)
        else:
            # Field exists in both, check for modifications
            field2 = fields2_map[name]
            diff_details = {}
            # Compare required (nullable) status
            if field1.get('required') != field2.get('required'):
                diff_details['required'] = {'from': field1.get('required'), 'to': field2.get('required')}
            # Compare type (simple string comparison for now)
            # TODO: Add recursive comparison for complex types (struct, list, map) if needed
            if str(field1.get('type')).strip() != str(field2.get('type')).strip():
                 diff_details['type'] = {'from': field1.get('type'), 'to': field2.get('type')}
            # Compare documentation (optional)
            # if field1.get('doc') != field2.get('doc'):
            #     diff_details['doc'] = {'from': field1.get('doc'), 'to': field2.get('doc')}

            if diff_details:
                modified.append({
                    "name": name,
                    "changes": diff_details,
                    # Optionally include full field definitions for context
                    # "from_definition": field1,
                    # "to_definition": field2
                })

    return {
        "added": added,
        "removed": removed,
        "modified": modified
    }


# --- Iceberg Endpoint ---

@app.route('/Iceberg', methods=['GET'])
@login_required
def iceberg_details():
    s3_url = request.args.get('s3_url')
    if not s3_url: return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Iceberg Request for User {g.user.id}, URL {s3_url} ---")

    bucket_name, table_base_key, s3_client = None, None, None

    user_role_arn = g.user.aws_role_arn
    if not user_role_arn: 
        return jsonify({"error": "AWS Role ARN not configured for this user."}), 400
    s3_client = get_s3_client_for_user(user_role_arn) # Get temporary client
    if not s3_client: 
        return jsonify({"error": "Failed to access AWS Role. Please verify configuration."}), 403

    try:
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)
        if not table_base_key.endswith('/'): table_base_key += '/'
        metadata_prefix = table_base_key + "metadata/"

        with tempfile.TemporaryDirectory(prefix="iceberg_meta_") as temp_dir:
            print(f"DEBUG User {g.user.id}: Using temporary directory: {temp_dir}")
            iceberg_manifest_files_info = [] # Stores info about manifest list/files

            # 1. Find latest metadata.json
            list_response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=metadata_prefix)
            if 'Contents' not in list_response:
                # Check if base path exists before declaring metadata empty
                try:
                    s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_base_key, Delimiter='/', MaxKeys=1)
                    # Base path exists, but no metadata files
                    return jsonify({"error": f"No objects found under metadata prefix: {metadata_prefix}"}), 404
                except s3_client.exceptions.ClientError as head_err:
                    # Can't even access base path
                    return jsonify({"error": f"Base table path or metadata prefix not found/accessible: s3://{bucket_name}/{table_base_key}. Error: {head_err}"}), 404

            metadata_files = []
            s3_object_details = {obj['Key']: {'size': obj['Size'], 'last_modified': obj['LastModified']} for obj in list_response.get('Contents', [])}

            for obj_key, obj_info in s3_object_details.items():
                if obj_key.endswith('.metadata.json'):
                    match = re.search(r'(v?\d+)-[a-f0-9-]+\.metadata\.json$', os.path.basename(obj_key))
                    version_num = 0
                    if match:
                        try: version_num = int(match.group(1).lstrip('v'))
                        except ValueError: pass
                    metadata_files.append({'key': obj_key, 'version': version_num, 'last_modified': obj_info['last_modified'], 'size': obj_info['size']})

            if not metadata_files:
                return jsonify({"error": f"No *.metadata.json files found under {metadata_prefix}"}), 404

            metadata_files.sort(key=lambda x: (x['version'], x['last_modified']), reverse=True)
            latest_metadata_key = metadata_files[0]['key']
            latest_metadata_size = metadata_files[0]['size']
            print(f"INFO: Using latest metadata file: {latest_metadata_key}")
            local_latest_metadata_path = os.path.join(temp_dir, os.path.basename(latest_metadata_key).replace('%', '_'))

            # 2. Parse latest metadata.json
            download_s3_file(s3_client, bucket_name, latest_metadata_key, local_latest_metadata_path)
            with open(local_latest_metadata_path, 'r') as f: latest_meta = json.load(f)

            table_uuid = latest_meta.get("table-uuid")
            current_snapshot_id = latest_meta.get("current-snapshot-id")
            all_snapshots_in_meta = latest_meta.get("snapshots", []) # Get all snapshots listed
            current_schema_id = latest_meta.get("current-schema-id", 0)
            # Default schema is the one marked as current, or the top-level one if list is missing
            default_schema = next((s for s in latest_meta.get("schemas", []) if s.get("schema-id") == current_schema_id), latest_meta.get("schema"))
            current_spec_id = latest_meta.get("current-spec-id", 0)
            partition_spec = next((s for s in latest_meta.get("partition-specs", []) if s.get("spec-id") == current_spec_id), latest_meta.get("partition-spec"))
            current_sort_order_id = latest_meta.get("current-sort-order-id", 0)
            sort_order = next((s for s in latest_meta.get("sort-orders", []) if s.get("order-id") == current_sort_order_id), latest_meta.get("sort-order"))
            properties = latest_meta.get("properties", {})
            format_version = latest_meta.get("format-version", 1)
            snapshot_log = latest_meta.get("snapshot-log", [])

            # --- Create a map of schema definitions by ID ---
            schemas_by_id = {s['schema-id']: s for s in latest_meta.get("schemas", []) if 'schema-id' in s}
            top_level_schema = latest_meta.get("schema")
            if top_level_schema:
                top_level_schema_id = top_level_schema.get("schema-id")
                if top_level_schema_id is not None and top_level_schema_id not in schemas_by_id:
                    schemas_by_id[top_level_schema_id] = top_level_schema
                elif top_level_schema_id is None and 0 not in schemas_by_id:
                    # Assign ID 0 if missing and 0 not used (common case for initial schema)
                    top_level_schema["schema-id"] = 0 # Add ID for consistency
                    schemas_by_id[0] = top_level_schema
                    print("DEBUG: Assigned schema-id 0 to top-level schema definition.")

            format_configuration = {
                "format-version": format_version,
                "table-uuid": table_uuid,
                "location": s3_url,
                "properties": properties,
                "snapshot-log": snapshot_log,
            }

            if current_snapshot_id is None:
                return jsonify({
                    "message": "Table metadata found, but no current snapshot exists (table might be empty or in intermediate state).",
                    "table_uuid": table_uuid, "location": s3_url,
                    "format_configuration": format_configuration,
                    "table_schema": default_schema, # Use default schema here
                    "partition_spec": partition_spec,
                    "version_history": {"total_snapshots": len(all_snapshots_in_meta), "snapshots_overview": []}
                }), 200

            # 3. Find current snapshot & manifest list
            current_snapshot = next((s for s in all_snapshots_in_meta if s.get("snapshot-id") == current_snapshot_id), None)
            if not current_snapshot:
                return jsonify({"error": f"Current Snapshot ID {current_snapshot_id} referenced in metadata not found in snapshots list."}), 404
            print(f"DEBUG: Current snapshot summary: {current_snapshot.get('summary', {})}")

            manifest_list_path = current_snapshot.get("manifest-list")
            if not manifest_list_path:
                return jsonify({"error": f"Manifest list path missing in current snapshot {current_snapshot_id}"}), 404

            manifest_list_key = ""
            manifest_list_bucket = bucket_name
            try:
                parsed_manifest_list_url = urlparse(manifest_list_path)
                if parsed_manifest_list_url.scheme == "s3":
                    manifest_list_bucket, manifest_list_key = extract_bucket_and_key(manifest_list_path)
                    if manifest_list_bucket != bucket_name:
                         print(f"Warning: Manifest list bucket '{manifest_list_bucket}' differs from table bucket '{bucket_name}'. Using manifest list bucket.")
                elif not parsed_manifest_list_url.scheme and parsed_manifest_list_url.path:
                    relative_path = unquote(parsed_manifest_list_url.path)
                    manifest_list_key = os.path.normpath(os.path.join(os.path.dirname(latest_metadata_key), relative_path)).replace("\\", "/")
                    manifest_list_bucket = bucket_name
                else: raise ValueError(f"Cannot parse manifest list path format: {manifest_list_path}")

                # Get Manifest List Size
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
                         "file_path": f"s3://{manifest_list_bucket}/{manifest_list_key}", "size_bytes": None,
                         "size_human": "N/A", "type": "Manifest List (Error getting size)"})

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
                manifest_bucket = bucket_name
                manifest_file_s3_uri = ""

                try:
                    parsed_manifest_url = urlparse(manifest_file_path)
                    if parsed_manifest_url.scheme == "s3":
                        m_bucket, manifest_file_key = extract_bucket_and_key(manifest_file_path)
                        manifest_bucket = m_bucket
                        manifest_file_s3_uri = manifest_file_path
                    elif not parsed_manifest_url.scheme and parsed_manifest_url.path:
                        relative_path = unquote(parsed_manifest_url.path)
                        manifest_file_key = os.path.normpath(os.path.join(os.path.dirname(manifest_list_key), relative_path)).replace("\\", "/")
                        manifest_bucket = manifest_list_bucket
                        manifest_file_s3_uri = f"s3://{manifest_bucket}/{manifest_file_key}"
                    else: raise ValueError("Cannot parse manifest file path format")

                    # Get Manifest File Size
                    manifest_file_size = entry.get('manifest_length')
                    if manifest_file_size is None:
                        try:
                            manifest_head = s3_client.head_object(Bucket=manifest_bucket, Key=manifest_file_key)
                            manifest_file_size = manifest_head.get('ContentLength')
                        except s3_client.exceptions.ClientError as head_err:
                            print(f"Warning: Could not get size for manifest file {manifest_file_key}: {head_err}")
                            manifest_file_size = None

                    iceberg_manifest_files_info.append({
                        "file_path": manifest_file_s3_uri, "size_bytes": manifest_file_size,
                        "size_human": format_bytes(manifest_file_size), "type": "Manifest File"})

                    local_manifest_path = os.path.join(temp_dir, f"manifest_{i}_" + os.path.basename(manifest_file_key).replace('%', '_'))
                    download_s3_file(s3_client, manifest_bucket, manifest_file_key, local_manifest_path)
                    manifest_records = parse_avro_file(local_manifest_path)
                    print(f"DEBUG: Processing {len(manifest_records)} entries in manifest: {os.path.basename(manifest_file_key)}")

                    # Process entries within the manifest file
                    for j, manifest_entry in enumerate(manifest_records):
                        status = manifest_entry.get('status', 0)
                        if status == 2: continue # Skip DELETED entries

                        record_count, file_size, file_path_in_manifest, partition_data = 0, 0, "", None
                        content = 0 # 0: data, 1: position deletes, 2: equality deletes

                        # Extract fields based on Format Version
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
                                 content = nested_info.get("content", 1) # Default to position deletes
                                 record_count = nested_info.get("record_count", 0) or 0
                                 file_size = nested_info.get("file_size_in_bytes", 0) or 0
                                 file_path_in_manifest = nested_info.get("file_path", "")
                                 # Delete files might also have partition info, handle if needed
                                 # partition_data = nested_info.get("partition")
                             else: continue # Skip invalid entry
                        elif format_version == 1:
                             content = 0
                             record_count = manifest_entry.get("record_count", 0) or 0
                             file_size = manifest_entry.get("file_size_in_bytes", 0) or 0
                             file_path_in_manifest = manifest_entry.get("file_path", "")
                             partition_data = manifest_entry.get("partition")
                        else: continue # Skip unsupported format

                        # Construct Full S3 Path for the data/delete file
                        full_file_s3_path = ""
                        file_bucket = bucket_name # Default
                        try:
                            parsed_file_path = urlparse(file_path_in_manifest)
                            if parsed_file_path.scheme == "s3":
                                f_bucket, f_key = extract_bucket_and_key(file_path_in_manifest)
                                file_bucket = f_bucket
                                full_file_s3_path = file_path_in_manifest
                            elif not parsed_file_path.scheme and parsed_file_path.path:
                                relative_data_path = unquote(parsed_file_path.path).lstrip('/')
                                full_file_s3_path = f"s3://{bucket_name}/{table_base_key.rstrip('/')}/{relative_data_path}"
                            else: continue # Skip accumulation if path cannot be determined
                        except ValueError as path_err:
                            print(f"Warning: Error parsing file path '{file_path_in_manifest}': {path_err}. Skipping accumulation.")
                            continue

                        # Accumulate stats
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
                                        # V2 stores partition data as struct/dict matching spec names
                                         partition_values_repr = {name: partition_data.get(name) for name in field_names if name in partition_data}
                                    elif format_version == 1 and isinstance(partition_data, (list, tuple)) and len(partition_data) == len(field_names):
                                        # V1 stores partition data as list in spec order
                                        partition_values_repr = dict(zip(field_names, partition_data))
                                    else: # Fallback or unexpected format
                                        partition_values_repr = {'_raw': str(partition_data)}
                                    partition_key_string = json.dumps(dict(sorted(partition_values_repr.items())), default=str)
                                except Exception as part_err:
                                    print(f"Warning: Error processing partition data {partition_data}: {part_err}")
                                    partition_key_string = f"<error: {part_err}>"
                                    partition_values_repr = {'_error': str(part_err)}
                            elif partition_data is None and partition_spec and not partition_spec.get('fields'):
                                partition_key_string = "<unpartitioned>"
                            elif partition_data is not None: # Unspecified partition spec but data present
                                partition_key_string = str(partition_data)
                                partition_values_repr = {'_raw': partition_data}

                            if partition_key_string not in partition_stats: partition_stats[partition_key_string] = {"gross_record_count": 0, "size_bytes": 0, "num_data_files": 0, "partition_values": partition_values_repr}
                            partition_stats[partition_key_string]["gross_record_count"] += record_count
                            partition_stats[partition_key_string]["size_bytes"] += file_size
                            partition_stats[partition_key_string]["num_data_files"] += 1

                            # Get sample path (only first parquet file found)
                            if full_file_s3_path and len(data_file_paths_sample) < 1 and full_file_s3_path.lower().endswith(".parquet"):
                                data_file_paths_sample.append({'bucket': file_bucket, 'key': urlparse(full_file_s3_path).path.lstrip('/')})

                        elif content == 1 or content == 2: # Delete File (V2 only)
                            total_delete_files += 1
                            approx_deleted_records += record_count
                            total_delete_storage_bytes += file_size

                except Exception as manifest_err:
                    print(f"ERROR: Failed to process manifest file {manifest_file_key} from bucket {manifest_bucket}: {manifest_err}")
                    traceback.print_exc()
                    iceberg_manifest_files_info.append({
                        "file_path": manifest_file_s3_uri or manifest_file_path, "size_bytes": None,
                        "size_human": "N/A", "type": "Manifest File (Error Processing)"})

            print("INFO: Finished processing manifest files.")

            # 6. Get Sample Data
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
            avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024)) if total_data_files > 0 else 0

            partition_explorer_data = []
            for k, v in partition_stats.items():
                partition_explorer_data.append({
                    "partition_values": v["partition_values"], "partition_key_string": k,
                    "gross_record_count": v["gross_record_count"], "size_bytes": v["size_bytes"],
                    "size_human": format_bytes(v["size_bytes"]), "num_data_files": v["num_data_files"] })
            partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))

            # --- Assemble Version History with Schemas ---
            snapshots_overview_with_schema = []
            history_limit = 20 # Limit the history shown
            snapshots_to_process = sorted(all_snapshots_in_meta, key=lambda x: x.get('timestamp-ms', 0), reverse=True)

            for snapshot in snapshots_to_process[:min(len(snapshots_to_process), history_limit)]:
                snapshot_schema_id = snapshot.get('schema-id')
                schema_definition = None
                if snapshot_schema_id is not None:
                    schema_definition = schemas_by_id.get(snapshot_schema_id)
                    if not schema_definition:
                        print(f"Warning: Schema definition for schema-id {snapshot_schema_id} (snapshot {snapshot.get('snapshot-id')}) not found in metadata.")
                elif default_schema: # Fallback for snapshots potentially missing schema-id
                    schema_definition = default_schema
                    print(f"Warning: Snapshot {snapshot.get('snapshot-id')} missing schema-id, using default schema (ID: {default_schema.get('schema-id')}).")

                overview_entry = {
                    "snapshot-id": snapshot.get("snapshot-id"),
                    "timestamp-ms": snapshot.get("timestamp-ms"),
                    "sequence-number": snapshot.get("sequence-number"),
                    "summary": snapshot.get("summary", {}),
                    "manifest-list": snapshot.get("manifest-list"),
                    "parent-snapshot-id": snapshot.get("parent-snapshot-id"),
                    "schema_definition": schema_definition # Embed the schema
                }
                snapshots_overview_with_schema.append(overview_entry)

            current_snapshot_summary_for_history = next((s for s in snapshots_overview_with_schema if s['snapshot-id'] == current_snapshot_id), None)

            # Final Result Structure
            result = {
                "table_type": "Iceberg",
                "table_uuid": table_uuid,
                "location": s3_url,
                "format_configuration": format_configuration,
                "iceberg_manifest_files": iceberg_manifest_files_info,
                "format_version": format_version,
                "current_snapshot_id": current_snapshot_id,
                "table_schema": default_schema, # Schema of the CURRENT snapshot
                "partition_spec": partition_spec,
                "sort_order": sort_order,
                "version_history": {
                    "total_snapshots": len(all_snapshots_in_meta),
                    "current_snapshot_summary": current_snapshot_summary_for_history,
                    "snapshots_overview": snapshots_overview_with_schema
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

            add_s3_usage_log(g.user.id, s3_url) # Log after success
            result_serializable = convert_bytes(result)
            end_time = time.time()
            print(f"--- Iceberg Request Completed in {end_time - start_time:.2f} seconds ---")
            return jsonify(result_serializable), 200

    # --- Exception Handling ---
    except NoCredentialsError: # Corrected exception type
        return jsonify({"error": "AWS credentials not found or invalid."}), 401
    except (s3_client.exceptions.NoSuchBucket if s3_client else Exception) as e:
         return jsonify({"error": f"S3 bucket not found or access denied: {bucket_name}"}), 404
    except (s3_client.exceptions.ClientError if s3_client else Exception) as e:
         error_code = e.response.get('Error', {}).get('Code', 'Unknown') if hasattr(e, 'response') else 'Unknown'
         print(f"ERROR: AWS ClientError in Iceberg endpoint: {error_code} - {e}")
         traceback.print_exc()
         return jsonify({"error": f"AWS ClientError ({error_code}): Check logs for details. Message: {str(e)}"}), 500
    except FileNotFoundError as e:
         print(f"ERROR: FileNotFoundError in Iceberg endpoint: {e}")
         return jsonify({"error": f"Required file or resource not found: {e}"}), 404
    except ValueError as e:
        print(f"ERROR: ValueError in Iceberg endpoint: {e}")
        return jsonify({"error": f"Input value or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred in Iceberg endpoint: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    

# --- Delta Lake Endpoint (Modified) ---

@app.route('/Delta', methods=['GET'])
@login_required
def delta_details():
    s3_url = request.args.get('s3_url')
    if not s3_url: 
        return jsonify({"error": "s3_url parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Delta Request for {s3_url} ---")

    bucket_name = None
    table_base_key = None
    delta_log_prefix = None
    s3_client = None

    user_role_arn = g.user.aws_role_arn
    if not user_role_arn: 
        return jsonify({"error": "AWS Role ARN not configured for this user."}), 400
    s3_client = get_s3_client_for_user(user_role_arn) # Get temporary client
    if not s3_client: 
        return jsonify({"error": "Failed to access AWS Role. Please verify configuration."}), 403

    try:
        bucket_name, table_base_key = extract_bucket_and_key(s3_url)
        if not table_base_key.endswith('/'): table_base_key += '/'
        delta_log_prefix = table_base_key + "_delta_log/"
        print(f"INFO User {g.user.id}: Processing Delta Lake table at: s3://{bucket_name}/{table_base_key}")
        
        with tempfile.TemporaryDirectory(prefix="delta_meta_") as temp_dir:
            print(f"DEBUG: Using temporary directory: {temp_dir}")

            # --- 1. Find Delta Log Files ---
            log_files_raw = []
            continuation_token = None
            while True:
                list_kwargs = {'Bucket': bucket_name, 'Prefix': delta_log_prefix}
                if continuation_token: list_kwargs['ContinuationToken'] = continuation_token
                try:
                    list_response = s3_client.list_objects_v2(**list_kwargs)
                except s3_client.exceptions.ClientError as list_err:
                    if list_err.response['Error']['Code'] == 'NoSuchBucket':
                         return jsonify({"error": f"S3 bucket not found: {bucket_name}"}), 404
                    print(f"ERROR: ClientError listing objects under {delta_log_prefix}: {list_err}")
                    return jsonify({"error": f"Error listing Delta log files: {list_err}"}), 500

                if 'Contents' not in list_response and not log_files_raw:
                    try:
                        s3_client.list_objects_v2(Bucket=bucket_name, Prefix=table_base_key, Delimiter='/', MaxKeys=1)
                        return jsonify({"error": f"Delta log prefix '{delta_log_prefix}' is empty."}), 404
                    except s3_client.exceptions.ClientError as head_err:
                          return jsonify({"error": f"Base table path or Delta log not found/accessible: s3://{bucket_name}/{table_base_key}. Error: {head_err}"}), 404

                log_files_raw.extend(list_response.get('Contents', []))
                if list_response.get('IsTruncated'):
                    continuation_token = list_response.get('NextContinuationToken')
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
                if filename == "_last_checkpoint" or json_pattern.match(filename) or checkpoint_pattern.match(filename):
                    delta_log_files_info.append({"file_path": f"s3://{bucket_name}/{key}", "relative_path": key.replace(table_base_key, "", 1), "size_bytes": size, "size_human": format_bytes(size)})

                if filename == "_last_checkpoint":
                    local_last_cp_path = os.path.join(temp_dir, "_last_checkpoint")
                    try:
                        download_s3_file(s3_client, bucket_name, key, local_last_cp_path)
                        with open(local_last_cp_path, 'r') as f: last_checkpoint_data = json.load(f)
                        last_checkpoint_info = {'version': last_checkpoint_data['version'], 'parts': last_checkpoint_data.get('parts'), 'key': key, 'size': size}
                    except Exception as cp_err: print(f"Warning: Failed to read/parse _last_checkpoint {key}: {cp_err}")
                elif (json_match := json_pattern.match(filename)):
                    json_commits[int(json_match.group(1))] = {'key': key, 'last_modified': obj['LastModified'], 'size': size}
                elif (cp_match := checkpoint_pattern.match(filename)):
                    version, part_num, num_parts = int(cp_match.group(1)), cp_match.group(2), cp_match.group(3)
                    part_num = int(part_num) if part_num else 1
                    num_parts = int(num_parts) if num_parts else 1
                    if version not in checkpoint_files: checkpoint_files[version] = {'num_parts': num_parts, 'parts': {}}
                    checkpoint_files[version]['parts'][part_num] = {'key': key, 'last_modified': obj['LastModified'], 'size': size}
                    if checkpoint_files[version]['num_parts'] != num_parts and cp_match.group(2):
                        checkpoint_files[version]['num_parts'] = max(checkpoint_files[version]['num_parts'], num_parts)

            delta_log_files_info.sort(key=lambda x: x.get('relative_path', ''))

            # Determine latest version ID
            current_snapshot_id = -1
            if json_commits: current_snapshot_id = max(json_commits.keys())
            elif last_checkpoint_info: current_snapshot_id = last_checkpoint_info['version']
            elif checkpoint_files:
                 complete_cp_versions = [v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']]
                 if complete_cp_versions: current_snapshot_id = max(complete_cp_versions)
                 elif checkpoint_files: # Fallback to potentially incomplete checkpoint if no JSON commits found
                      current_snapshot_id = max(checkpoint_files.keys())

            if current_snapshot_id == -1:
                 return jsonify({"error": "No Delta commit JSON files or checkpoint files found."}), 404
            print(f"INFO: Latest Delta version (snapshot ID) identified: {current_snapshot_id}")

            # --- 2/3. Process Checkpoint and JSON Commits Incrementally ---
            active_files = {}
            metadata_from_log = None # Tracks metadata from LATEST processed file/checkpoint
            protocol_from_log = None # Tracks protocol from LATEST processed file/checkpoint
            all_commit_info = {}
            processed_versions = set()
            checkpoint_version_used = None # Track which checkpoint was actually loaded
            start_process_version = 0 # Default start
            effective_checkpoint_version = -1

            # Determine starting point (Checkpoint or Version 0)
            cp_version_candidate = -1
            if last_checkpoint_info: cp_version_candidate = last_checkpoint_info['version']
            elif checkpoint_files:
                 available_complete_checkpoints = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts']], reverse=True)
                 if available_complete_checkpoints: cp_version_candidate = available_complete_checkpoints[0]

            if cp_version_candidate > -1:
                 # Verify the candidate checkpoint is actually complete and available
                 if cp_version_candidate in checkpoint_files and len(checkpoint_files[cp_version_candidate]['parts']) == checkpoint_files[cp_version_candidate]['num_parts']:
                      effective_checkpoint_version = cp_version_candidate
                 else: # Fallback if candidate from _last_checkpoint is bad or missing
                     available_complete_checkpoints = sorted([v for v, info in checkpoint_files.items() if len(info['parts']) == info['num_parts'] and v < cp_version_candidate], reverse=True)
                     if available_complete_checkpoints: effective_checkpoint_version = available_complete_checkpoints[0]

            # --- Load State and Initial Schema from Checkpoint if applicable ---
            loaded_schema_from_checkpoint = None
            if effective_checkpoint_version > -1:
                print(f"INFO: Reading state from checkpoint version {effective_checkpoint_version}")
                checkpoint_version_used = effective_checkpoint_version
                cp_info = checkpoint_files[effective_checkpoint_version]
                try:
                    all_checkpoint_actions = []
                    for part_num in sorted(cp_info['parts'].keys()):
                         part_key = cp_info['parts'][part_num]['key']
                         all_checkpoint_actions.extend(read_delta_checkpoint(s3_client, bucket_name, part_key, temp_dir))

                    for action in all_checkpoint_actions:
                         if 'add' in action and action['add']:
                              add_info = action['add']
                              path = add_info['path']
                              stats_parsed = json.loads(add_info['stats']) if isinstance(add_info.get('stats'), str) else add_info.get('stats')
                              active_files[path] = { 'size': add_info.get('size'), 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': add_info.get('modificationTime', 0), 'stats': stats_parsed, 'tags': add_info.get('tags') }
                         elif 'metaData' in action and action['metaData']:
                              metadata_from_log = action['metaData']
                              schema_str = metadata_from_log.get("schemaString")
                              if schema_str:
                                  parsed_schema = _parse_delta_schema_string(schema_str)
                                  if parsed_schema:
                                      loaded_schema_from_checkpoint = parsed_schema
                                      print(f"DEBUG: Loaded schema definition from checkpoint {effective_checkpoint_version}")
                         elif 'protocol' in action and action['protocol']:
                              protocol_from_log = action['protocol']

                    start_process_version = effective_checkpoint_version + 1
                    processed_versions.add(effective_checkpoint_version)

                    cp_total_files = len(active_files)
                    cp_total_bytes = sum(f['size'] for f in active_files.values() if f.get('size'))
                    cp_total_records = sum(int(f['stats']['numRecords']) for f in active_files.values() if f.get('stats') and 'numRecords' in f['stats'])
                    all_commit_info[effective_checkpoint_version] = {
                        'version': effective_checkpoint_version, 'timestamp': None,
                        'operation': 'CHECKPOINT_LOAD', 'operationParameters': {},
                        'num_added_files': cp_total_files, 'num_removed_files': 0,
                        'added_bytes': cp_total_bytes, 'removed_bytes': 0,
                        'metrics': {'numOutputFiles': str(cp_total_files), 'numOutputBytes': str(cp_total_bytes)},
                        'total_files_at_version': cp_total_files,
                        'total_bytes_at_version': cp_total_bytes,
                        'total_records_at_version': cp_total_records,
                        'schema_definition': loaded_schema_from_checkpoint
                    }
                    print(f"INFO: Checkpoint processing complete. Starting JSON processing from version {start_process_version}")

                except Exception as cp_read_err:
                     print(f"ERROR: Failed to read/process checkpoint {effective_checkpoint_version}: {cp_read_err}. Falling back.")
                     active_files = {}; metadata_from_log = None; protocol_from_log = None
                     start_process_version = 0; checkpoint_version_used = None; processed_versions = set()
                     all_commit_info = {}; loaded_schema_from_checkpoint = None
            else:
                print("INFO: No usable checkpoint found. Processing JSON logs from version 0.")
                start_process_version = 0

            # --- Process JSON Commits Incrementally ---
            versions_to_process = sorted([v for v in json_commits if v >= start_process_version])
            print(f"INFO: Processing {len(versions_to_process)} JSON versions from {start_process_version} up to {current_snapshot_id}...")
            removed_file_sizes_by_commit = {}

            for version in versions_to_process:
                 if version in processed_versions: continue
                 commit_file_info = json_commits[version]
                 commit_key = commit_file_info['key']
                 print(f"DEBUG: Processing version {version} ({commit_key})...")
                 removed_file_sizes_by_commit[version] = 0
                 commit_schema_definition = None # Track schema change within this commit

                 try:
                    actions = read_delta_json_lines(s3_client, bucket_name, commit_key, temp_dir)
                    commit_summary = {
                         'version': version, 'timestamp': None, 'operation': 'Unknown',
                         'num_actions': len(actions), 'operationParameters': {},
                         'num_added_files': 0, 'num_removed_files': 0,
                         'added_bytes': 0, 'removed_bytes': 0, 'metrics': {}
                    }
                    op_metrics = {} # Reset for each commit

                    for action in actions:
                        if 'commitInfo' in action and action['commitInfo']:
                            ci = action['commitInfo']
                            commit_summary['timestamp'] = ci.get('timestamp')
                            commit_summary['operation'] = ci.get('operation', 'Unknown')
                            commit_summary['operationParameters'] = ci.get('operationParameters', {})
                            op_metrics = ci.get('operationMetrics', {}) # Get metrics for this commit
                            commit_summary['metrics'] = op_metrics
                            # Prefer explicit metrics if available
                            commit_summary['num_added_files'] = int(op_metrics.get('numOutputFiles', 0))
                            commit_summary['num_removed_files'] = int(op_metrics.get('numTargetFilesRemoved', op_metrics.get('numRemovedFiles', 0))) # Spark 3 vs older
                            commit_summary['added_bytes'] = int(op_metrics.get('numOutputBytes', 0))
                            commit_summary['removed_bytes'] = int(op_metrics.get('numTargetBytesRemoved', op_metrics.get('numRemovedBytes', 0))) # Spark 3 vs older
                        elif 'add' in action and action['add']:
                            add_info = action['add']
                            path = add_info['path']
                            stats_parsed = json.loads(add_info['stats']) if isinstance(add_info.get('stats'), str) else add_info.get('stats')
                            file_size = add_info.get('size', 0)
                            active_files[path] = { 'size': file_size, 'partitionValues': add_info.get('partitionValues', {}), 'modificationTime': add_info.get('modificationTime', 0), 'stats': stats_parsed, 'tags': add_info.get('tags') }
                            # Increment counts/bytes only if metrics were missing in commitInfo
                            if 'numOutputFiles' not in op_metrics: commit_summary['num_added_files'] += 1
                            if 'numOutputBytes' not in op_metrics: commit_summary['added_bytes'] += file_size
                        elif 'remove' in action and action['remove']:
                             remove_info = action['remove']
                             path = remove_info['path']
                             if remove_info.get('dataChange', True): # Check if it affects data
                                 removed_file_info = active_files.pop(path, None)
                                 # Increment counts/bytes only if metrics were missing
                                 if 'numTargetFilesRemoved' not in op_metrics and 'numRemovedFiles' not in op_metrics:
                                     commit_summary['num_removed_files'] += 1
                                 # Track size for potential calculation if specific byte metrics are missing
                                 if removed_file_info and removed_file_info.get('size'):
                                      removed_file_sizes_by_commit[version] += removed_file_info.get('size',0)
                        elif 'metaData' in action and action['metaData']:
                             metadata_from_log = action['metaData']
                             schema_str = metadata_from_log.get("schemaString")
                             if schema_str:
                                 parsed_schema = _parse_delta_schema_string(schema_str)
                                 if parsed_schema:
                                     commit_schema_definition = parsed_schema
                                     print(f"DEBUG: Found schema update in version {version}")
                        elif 'protocol' in action and action['protocol']:
                             protocol_from_log = action['protocol']

                    # Calculate removed bytes if metrics were missing
                    if commit_summary['removed_bytes'] == 0 and removed_file_sizes_by_commit.get(version, 0) > 0:
                         # Only use calculated size if metric wasn't present in commitInfo
                         if 'numTargetBytesRemoved' not in op_metrics and 'numRemovedBytes' not in op_metrics:
                              commit_summary['removed_bytes'] = removed_file_sizes_by_commit[version]

                    # Calculate and store cumulative state
                    current_total_files = len(active_files)
                    current_total_bytes = sum(f['size'] for f in active_files.values() if f.get('size'))
                    current_total_records = sum(int(f['stats']['numRecords']) for f in active_files.values() if f.get('stats') and f['stats'].get('numRecords') is not None) # Safer access

                    commit_summary['total_files_at_version'] = current_total_files
                    commit_summary['total_bytes_at_version'] = current_total_bytes
                    commit_summary['total_records_at_version'] = current_total_records
                    commit_summary['schema_definition'] = commit_schema_definition # Store None if no change this commit

                    all_commit_info[version] = commit_summary
                    processed_versions.add(version)

                 except Exception as json_proc_err:
                      print(f"ERROR: Failed to process commit file {commit_key} for version {version}: {json_proc_err}")
                      traceback.print_exc()
                      all_commit_info[version] = {'version': version, 'error': str(json_proc_err)}
                      processed_versions.add(version) # Mark as processed even with error

            print(f"INFO: Finished incremental processing. Propagating schemas...")

            # --- Propagate Schema Definitions Forward ---
            last_known_schema = loaded_schema_from_checkpoint
            sorted_processed_versions = sorted([v for v in processed_versions if v in all_commit_info and 'error' not in all_commit_info[v]])

            for v in sorted_processed_versions:
                 # This loop iterates through versions we have info for, in ascending order
                 current_commit = all_commit_info[v]

                 # If this commit defined a schema, it becomes the new last_known_schema
                 if 'schema_definition' in current_commit and current_commit['schema_definition']:
                     last_known_schema = current_commit['schema_definition']
                 # If this commit DID NOT define a schema, inherit from the previous version
                 elif last_known_schema:
                     current_commit['schema_definition'] = last_known_schema
                 # If schema is still unknown
                 else:
                     # Check if it's the very first version processed and schema wasn't in checkpoint
                     if v == min(sorted_processed_versions) and not last_known_schema:
                          print(f"Warning: Schema definition missing for initial version {v}.")
                     # Otherwise, it means propagation failed or metadata was missing earlier
                     elif v > min(sorted_processed_versions):
                          print(f"Warning: Inherited schema is missing for version {v}.")
                     current_commit['schema_definition'] = None # Mark as unknown

            # --- Find Definitive Metadata & Protocol (mostly for configuration) ---
            definitive_metadata = metadata_from_log # Use the last one seen during processing
            definitive_protocol = protocol_from_log

            if not definitive_metadata:
                 # Attempt recovery if latest processing failed but older metadata exists
                 latest_successful_v = max(sorted_processed_versions) if sorted_processed_versions else -1
                 if latest_successful_v > -1 and all_commit_info[latest_successful_v].get('schema_definition'):
                     print("Warning: Final metadata action might be missing, relying on schema from last successful version.")
                     # Cannot easily reconstruct definitive_metadata dict here, proceed cautiously
                     pass
                 else:
                     return jsonify({"error": "Could not determine final table metadata."}), 500

            if not definitive_protocol:
                 definitive_protocol = {"minReaderVersion": 1, "minWriterVersion": 2}

            # --- Assemble Format Configuration, Final Schema, Partition Spec ---
            format_configuration = {**definitive_protocol, **(definitive_metadata.get("configuration", {}) if definitive_metadata else {})}

            final_table_schema = all_commit_info.get(current_snapshot_id, {}).get('schema_definition')
            if not final_table_schema:
                 # Last resort: parse from definitive metadata if propagation failed
                 if definitive_metadata and definitive_metadata.get("schemaString"):
                      final_table_schema = _parse_delta_schema_string(definitive_metadata.get("schemaString"))
                 if not final_table_schema:
                      return jsonify({"error": "Failed to determine final table schema."}), 500

            partition_cols = definitive_metadata.get("partitionColumns", []) if definitive_metadata else []
            partition_spec_fields = []
            schema_fields_map = {f['name']: f for f in final_table_schema.get('fields', [])}
            for i, col_name in enumerate(partition_cols):
                source_field = schema_fields_map.get(col_name)
                if source_field: partition_spec_fields.append({ "name": col_name, "transform": "identity", "source-id": source_field.get('id', i+1000), "field-id": 1000 + i }) # Ensure unique field-id
            partition_spec = {"spec-id": 0, "fields": partition_spec_fields}

            # --- 4. Calculate FINAL State Metrics ---
            final_commit_details = all_commit_info.get(current_snapshot_id, {})
            total_data_files = final_commit_details.get('total_files_at_version', 0)
            total_data_storage_bytes = final_commit_details.get('total_bytes_at_version', 0)
            approx_live_records = final_commit_details.get('total_records_at_version', 0)
            gross_records_in_data_files = approx_live_records # Estimate for Delta
            total_delete_files = 0 # Delta doesn't track this way in log
            total_delete_storage_bytes = 0
            approx_deleted_records_in_manifests = 0
            avg_live_records_per_data_file = (approx_live_records / total_data_files) if total_data_files > 0 else 0
            avg_data_file_size_mb = (total_data_storage_bytes / (total_data_files or 1) / (1024*1024)) if total_data_files > 0 else 0
            metrics_note = f"Live record count ({approx_live_records}) is estimated based on 'numRecords' in file stats. Delete file/record metrics common in Iceberg V2 are not tracked in Delta metadata."

            # --- 5. Calculate Partition Stats ---
            partition_stats = {}
            for path, file_info in active_files.items():
                part_values = file_info.get('partitionValues', {})
                part_key_string = json.dumps(dict(sorted(part_values.items())), default=str) if part_values else "<unpartitioned>"
                if part_key_string not in partition_stats: partition_stats[part_key_string] = { "partition_values": part_values, "partition_key_string": part_key_string, "num_data_files": 0, "size_bytes": 0, "gross_record_count": 0 }
                partition_stats[part_key_string]["num_data_files"] += 1
                partition_stats[part_key_string]["size_bytes"] += file_info.get('size', 0)
                if file_info.get('stats') and file_info['stats'].get('numRecords') is not None:
                    try: partition_stats[part_key_string]["gross_record_count"] += int(file_info['stats']['numRecords'])
                    except (ValueError, TypeError): pass
            partition_explorer_data = list(partition_stats.values())
            for p_data in partition_explorer_data: p_data["size_human"] = format_bytes(p_data["size_bytes"])
            partition_explorer_data.sort(key=lambda x: x.get("partition_key_string", ""))

            # --- 6. Get Sample Data ---
            sample_data = []
            if active_files:
                sample_file_path = next((p for p in active_files if p.lower().endswith('.parquet')), list(active_files.keys())[0] if active_files else None)
                if sample_file_path:
                    full_sample_s3_key = os.path.join(table_base_key, sample_file_path).replace("\\", "/") # Use join for robustness
                    print(f"INFO: Attempting sample from: s3://{bucket_name}/{full_sample_s3_key}")
                    try:
                        if sample_file_path.lower().endswith('.parquet'):
                            sample_data = read_parquet_sample(s3_client, bucket_name, full_sample_s3_key, temp_dir, num_rows=10)
                        else: sample_data = [{"error": "Sampling only implemented for Parquet"}]
                    except Exception as sample_err: sample_data = [{"error": "Failed to read sample data", "details": str(sample_err)}]
                else: sample_data = [{"error": "No active Parquet files found for sampling"}]
            else: sample_data = [{"error": "No active files found for sampling"}]

            # --- 7. Assemble Final Result ---
            print("\nINFO: Assembling final Delta result...")

            current_snapshot_details = {
                 "version": current_snapshot_id,
                 "timestamp_ms": final_commit_details.get('timestamp'),
                 "timestamp_iso": format_timestamp_ms(final_commit_details.get('timestamp')),
                 "operation": final_commit_details.get('operation', 'N/A'),
                 "operation_parameters": final_commit_details.get('operationParameters', {}),
                 "num_files_total_snapshot": total_data_files,
                 "total_data_files_snapshot": total_data_files,
                 "total_delete_files_snapshot": total_delete_files,
                 "total_data_storage_bytes_snapshot": total_data_storage_bytes,
                 "total_records_snapshot": approx_live_records,
                 "num_added_files_commit": final_commit_details.get('num_added_files'),
                 "num_removed_files_commit": final_commit_details.get('num_removed_files'),
                 "commit_added_bytes": final_commit_details.get('added_bytes'),
                 "commit_removed_bytes": final_commit_details.get('removed_bytes'),
                 "commit_metrics_raw": final_commit_details.get('metrics', {}),
                 "error": final_commit_details.get('error')
             }

            # Assemble Version History
            snapshots_overview_with_schema = []
            known_versions_sorted = sorted([v for v in processed_versions if v in all_commit_info and 'error' not in all_commit_info[v]], reverse=True)
            history_limit = 20
            versions_in_history = known_versions_sorted[:min(len(known_versions_sorted), history_limit)]
            current_snapshot_summary_for_history = None

            for v in versions_in_history:
                commit_details = all_commit_info.get(v)
                if not commit_details: continue

                summary = {
                     "operation": commit_details.get('operation', 'Unknown'),
                     "added-data-files": str(commit_details.get('num_added_files', 'N/A')),
                     "removed-data-files": str(commit_details.get('num_removed_files', 'N/A')),
                     "added-files-size": str(commit_details.get('added_bytes', 'N/A')),
                     "removed-files-size": str(commit_details.get('removed_bytes', 'N/A')),
                     "operation-parameters": commit_details.get('operationParameters', {}),
                     "total-data-files": str(commit_details.get('total_files_at_version', 'N/A')),
                     "total-files-size": str(commit_details.get('total_bytes_at_version', 'N/A')),
                     "total-records": str(commit_details.get('total_records_at_version', 'N/A')),
                     "total-delete-files": "0",
                 }

                snapshot_entry = {
                    "snapshot-id": v,
                    "timestamp-ms": commit_details.get('timestamp'),
                    "summary": summary,
                    "schema_definition": commit_details.get('schema_definition') # Embed propagated schema
                }
                snapshots_overview_with_schema.append(snapshot_entry)

                if v == current_snapshot_id:
                    current_snapshot_summary_for_history = snapshot_entry

            # Final Result Structure
            result = {
                "table_type": "Delta",
                "table_uuid": definitive_metadata.get("id") if definitive_metadata else None,
                "location": s3_url if s3_url else f"s3://{bucket_name}/{table_base_key}",
                "format_configuration": format_configuration,
                "format_version": definitive_protocol.get('minReaderVersion', 1),
                "delta_log_files": delta_log_files_info,
                "current_snapshot_id": current_snapshot_id,
                "current_snapshot_details": current_snapshot_details,
                "table_schema": final_table_schema,
                "table_properties": definitive_metadata.get("configuration", {}) if definitive_metadata else {},
                "partition_spec": partition_spec,
                "sort_order": {"order-id": 0, "fields": []},
                "version_history": {
                    "total_snapshots": len(known_versions_sorted),
                    "current_snapshot_summary": current_snapshot_summary_for_history,
                    "snapshots_overview": snapshots_overview_with_schema
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
            add_s3_usage_log(g.user.id, s3_url)
            result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))
            end_time = time.time()
            print(f"--- Delta Request Completed in {end_time - start_time:.2f} seconds ---")
            return jsonify(result_serializable), 200

    # --- Exception Handling ---
    except NoCredentialsError: # Corrected exception type
        return jsonify({"error": "AWS credentials not found or invalid."}), 401
    except (s3_client.exceptions.NoSuchBucket if s3_client else Exception) as e:
        return jsonify({"error": f"S3 bucket not found or access denied: {bucket_name}"}), 404
    except (s3_client.exceptions.ClientError if s3_client else Exception) as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown') if hasattr(e, 'response') else 'Unknown'
        print(f"ERROR: AWS ClientError in Delta endpoint: {error_code} - {e}")
        traceback.print_exc()
        return jsonify({"error": f"AWS ClientError ({error_code}): Check logs for details. Message: {str(e)}"}), 500
    except FileNotFoundError as e:
        print(f"ERROR: FileNotFoundError in Delta endpoint: {e}")
        # Add specific check for log dir missing
        if "_delta_log" in str(e) or (delta_log_prefix and delta_log_prefix in str(e)):
             return jsonify({"error": f"Delta log not found or inaccessible for table: {s3_url}"}), 404
        return jsonify({"error": f"Required file or resource not found: {e}"}), 404
    except ValueError as e:
        print(f"ERROR: ValueError in Delta endpoint: {e}")
        return jsonify({"error": f"Input value or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred in Delta endpoint: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500

# --- NEW Schema Comparison Endpoint ---

@app.route('/compare_schema/Iceberg', methods=['GET'])
@login_required
def compare_iceberg_schema_endpoint():
    s3_url = request.args.get('s3_url')
    seq1_str = request.args.get('seq1') # Use 'seq1' for sequence number
    seq2_str = request.args.get('seq2') # Use 'seq2' for sequence number

    if not s3_url: 
        return jsonify({"error": "s3_url parameter is missing"}), 400
    if not seq1_str: 
        return jsonify({"error": "seq1 (sequence number) parameter is missing"}), 400
    if not seq2_str: 
        return jsonify({"error": "seq2 (sequence number) parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Iceberg Schema Comparison Request for {s3_url} (seq {seq1_str} vs seq {seq2_str}) ---")

    bucket_name = None
    table_base_key = None
    s3_client = None
    temp_dir_obj = None

    try:
        seq1 = int(seq1_str)
        seq2 = int(seq2_str)
    except ValueError:
        return jsonify({"error": "seq1 and seq2 must be integers."}), 400

    try:
        bucket_name, table_base_key = extract_bucket_and_key(request.args.get('s3_url')) # Example
        if not table_base_key.endswith('/'): 
            table_base_key += '/'

        temp_dir_obj = tempfile.TemporaryDirectory(prefix="iceberg_schema_comp_")
        temp_dir = temp_dir_obj.name

        # --- Use temporary s3_client for helpers ---
        schema1 = get_iceberg_schema_for_version(s3_client, bucket_name, table_base_key, seq1, temp_dir)
        schema2 = get_iceberg_schema_for_version(s3_client, bucket_name, table_base_key, seq2, temp_dir)
        schema_diff = compare_schemas(schema1, schema2, f"seq {seq1}", f"seq {seq2}")

        # --- Assemble Result ---
        result = {
            "table_type": "Iceberg",
            "location": s3_url,
            "version1": {
                "identifier_type": "sequence_number",
                "identifier": seq1,
                "schema": schema1
            },
            "version2": {
                "identifier_type": "sequence_number",
                "identifier": seq2,
                "schema": schema2
            },
            "schema_comparison": schema_diff
        }

        result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))
        end_time = time.time()
        print(f"--- Iceberg Schema Comparison Request Completed in {end_time - start_time:.2f} seconds ---")
        return jsonify(result_serializable), 200

    # --- Exception Handling (Similar to previous) ---
    except NoCredentialsError: # <--- CHANGE THIS LINE
        return jsonify({"error": "AWS credentials not found or invalid."}), 401
    except (s3_client.exceptions.NoSuchBucket if s3_client else Exception) as e:
        return jsonify({"error": f"S3 bucket not found or access denied: {bucket_name}"}), 404
    except (s3_client.exceptions.ClientError if s3_client else Exception) as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown') if hasattr(e, 'response') else 'Unknown'
        print(f"ERROR: AWS ClientError: {error_code} - {e}")
        traceback.print_exc()
        return jsonify({"error": f"AWS ClientError ({error_code}): Check logs for details. Message: {str(e)}"}), 500
    except FileNotFoundError as e:
        print(f"ERROR: FileNotFoundError: {e}")
        return jsonify({"error": f"Required file or directory not found: {e}"}), 404
    except ValueError as e: # Catch specific value errors from helpers
        print(f"ERROR: ValueError: {e}")
        return jsonify({"error": f"Input or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    finally:
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                print(f"DEBUG: Cleaned up temporary directory: {temp_dir_obj.name}")
            except Exception as cleanup_err:
                print(f"ERROR: Failed to cleanup temporary directory {temp_dir_obj.name}: {cleanup_err}")


# --- NEW Delta Schema Comparison Endpoint ---

@app.route('/compare_schema/Delta', methods=['GET'])
@login_required
def compare_delta_schema_endpoint():
    s3_url = request.args.get('s3_url')
    v1_str = request.args.get('v1') # Use 'v1' for version ID
    v2_str = request.args.get('v2') # Use 'v2' for version ID

    if not s3_url: 
        return jsonify({"error": "s3_url parameter is missing"}), 400
    if not v1_str: 
        return jsonify({"error": "v1 (version ID) parameter is missing"}), 400
    if not v2_str: 
        return jsonify({"error": "v2 (version ID) parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing Delta Schema Comparison Request for {s3_url} (v{v1_str} vs v{v2_str}) ---")

    bucket_name = None
    table_base_key = None
    s3_client = None
    temp_dir_obj = None

    user_role_arn = g.user.aws_role_arn
    if not user_role_arn: return jsonify({"error": "AWS Role ARN not configured."}), 400
    s3_client = get_s3_client_for_user(user_role_arn)
    if not s3_client: 
        return jsonify({"error": "Failed to assume AWS Role."}), 403

    try:
        v1 = int(request.args.get('v1'))
        v2 = int(request.args.get('v2')) # Example
        bucket_name, table_base_key = extract_bucket_and_key(request.args.get('s3_url')) # Example
        if not table_base_key.endswith('/'): 
            table_base_key += '/'

        temp_dir_obj = tempfile.TemporaryDirectory(prefix="delta_schema_comp_")
        temp_dir = temp_dir_obj.name
        print(f"DEBUG: Using temporary directory: {temp_dir}")

        # --- Get Schemas ---
        version1_label = f"version_id {v1}"
        version2_label = f"version_id {v2}"
        schema1 = get_delta_schema_for_version(s3_client, bucket_name, table_base_key, v1, temp_dir)
        schema2 = get_delta_schema_for_version(s3_client, bucket_name, table_base_key, v2, temp_dir)

        # --- Compare Schemas ---
        schema_diff = compare_schemas(schema1, schema2, version1_label, version2_label)

        # --- Assemble Result ---
        result = {
            "table_type": "Delta",
            "location": s3_url,
            "version1": {
                "identifier_type": "version_id",
                "identifier": v1,
                "schema": schema1
            },
            "version2": {
                "identifier_type": "version_id",
                "identifier": v2,
                "schema": schema2
            },
            "schema_comparison": schema_diff
        }

        result_serializable = json.loads(json.dumps(convert_bytes(result), default=str))
        end_time = time.time()
        print(f"--- Delta Schema Comparison Request Completed in {end_time - start_time:.2f} seconds ---")
        return jsonify(result_serializable), 200

    # --- Exception Handling (Similar to previous) ---
    except boto3.exceptions.NoCredentialsError:
        return jsonify({"error": "AWS credentials not found."}), 401
    except (s3_client.exceptions.NoSuchBucket if s3_client else Exception) as e:
        return jsonify({"error": f"S3 bucket not found or access denied: {bucket_name}"}), 404
    except (s3_client.exceptions.ClientError if s3_client else Exception) as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown') if hasattr(e, 'response') else 'Unknown'
        print(f"ERROR: AWS ClientError: {error_code} - {e}")
        traceback.print_exc()
        return jsonify({"error": f"AWS ClientError ({error_code}): Check logs for details. Message: {str(e)}"}), 500
    except FileNotFoundError as e:
        print(f"ERROR: FileNotFoundError: {e}")
        # Check if it's the specific Delta log not found error
        if "_delta_log" in str(e):
             return jsonify({"error": f"Delta log not found or inaccessible for table: {s3_url}"}), 404
        return jsonify({"error": f"Required file or directory not found: {e}"}), 404
    except ValueError as e: # Catch specific value errors from helpers
        print(f"ERROR: ValueError: {e}")
        return jsonify({"error": f"Input or data processing error: {str(e)}"}), 400
    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        traceback.print_exc()
        return jsonify({"error": f"An unexpected error occurred: {str(e)}"}), 500
    finally:
        if temp_dir_obj:
            try:
                temp_dir_obj.cleanup()
                print(f"DEBUG: Cleaned up temporary directory: {temp_dir_obj.name}")
            except Exception as cleanup_err:
                print(f"ERROR: Failed to cleanup temporary directory {temp_dir_obj.name}: {cleanup_err}")


from urllib.parse import urlparse, unquote
import boto3
from botocore.exceptions import NoCredentialsError # Make sure this import is present
# ... other imports ...

# --- NEW Endpoint to List Tables ---
@app.route('/list_tables', methods=['GET'])
@login_required
def list_tables():
    s3_root_path = request.args.get('s3_root_path')
    if not s3_root_path:
        return jsonify({"error": "s3_root_path parameter is missing"}), 400

    start_time = time.time()
    print(f"\n--- Processing List Tables Request for User {g.user.id}, Path {s3_root_path} ---")

    bucket_name = None
    root_prefix = None
    s3_client = None # Will hold the temporary client

    # --- AssumeRole Integration START ---
    user_role_arn = g.user.aws_role_arn
    if not user_role_arn:
         print(f"ERROR User {g.user.id}: AWS Role ARN not configured.")
         return jsonify({"error": "AWS Role ARN not configured for this user."}), 400

    s3_client = get_s3_client_for_user(user_role_arn)
    if not s3_client:
        print(f"ERROR User {g.user.id}: Failed to assume AWS Role {user_role_arn}.")
        # Provide a more user-friendly message maybe
        return jsonify({"error": "Failed to access AWS Role. Please verify configuration in your AWS account (Role ARN, Trust Policy) and ensure it's correctly saved here."}), 403 # Forbidden
    # --- AssumeRole Integration END ---

    try:
        # Use the temporary s3_client obtained above
        if not s3_root_path.endswith('/'): s3_root_path += '/'
        parsed_url = urlparse(s3_root_path)
        bucket_name = parsed_url.netloc
        root_prefix = unquote(parsed_url.path).lstrip('/')

        if not s3_root_path.startswith("s3://") or not bucket_name:
             raise ValueError(f"Invalid S3 root path: {s3_root_path}")

        discovered_tables = []
        paginator = s3_client.get_paginator('list_objects_v2')
        print(f"DEBUG User {g.user.id}: Listing common prefixes under s3://{bucket_name}/{root_prefix} using assumed role")

        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=root_prefix, Delimiter='/')

        processed_prefixes = set() # Handle potential pagination duplicates if any

        for page in page_iterator:
            if 'CommonPrefixes' in page:
                for common_prefix_obj in page['CommonPrefixes']:
                    prefix = common_prefix_obj.get('Prefix')
                    if not prefix or prefix in processed_prefixes: continue
                    processed_prefixes.add(prefix)

                    table_path = f"s3://{bucket_name}/{prefix}"
                    detected_type = "Unknown" # Default
                    print(f"DEBUG User {g.user.id}: Checking prefix {prefix} for table type...")

                    # Check for Delta (_delta_log) - Use temporary client
                    try:
                        delta_check = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}_delta_log/", MaxKeys=1)
                        if 'Contents' in delta_check or delta_check.get('KeyCount', 0) > 0:
                            detected_type = "Delta"
                            print(f"DEBUG User {g.user.id}: Found Delta log for {prefix}")
                            discovered_tables.append({"path": table_path, "type": detected_type})
                            continue # Found type, move to next prefix
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'AccessDenied': # Log if not just access denied
                            print(f"Warning User {g.user.id}: S3 error checking Delta log for {prefix}: {e}")
                        else: 
                            print(f"DEBUG User {g.user.id}: Access denied checking Delta log for {prefix} (expected if no log)")
                    except Exception as e: 
                        print(f"Warning User {g.user.id}: Unexpected error checking Delta log for {prefix}: {e}")

                    # Check for Iceberg (metadata/*.metadata.json) - Use temporary client
                    try:
                        iceberg_check = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}metadata/", MaxKeys=10)
                        if any(item.get('Key', '').endswith('.metadata.json') for item in iceberg_check.get('Contents', [])):
                             detected_type = "Iceberg"
                             print(f"DEBUG User {g.user.id}: Found Iceberg metadata for {prefix}")
                             discovered_tables.append({"path": table_path, "type": detected_type})
                             continue
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'AccessDenied':
                            print(f"Warning User {g.user.id}: S3 error checking Iceberg metadata for {prefix}: {e}")
                        else: 
                            print(f"DEBUG User {g.user.id}: Access denied checking Iceberg metadata for {prefix}")
                    except Exception as e: 
                        print(f"Warning User {g.user.id}: Unexpected error checking Iceberg metadata for {prefix}: {e}")

                    # Check for Hudi (.hoodie) - Use temporary client
                    try:
                        hudi_check = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{prefix}.hoodie/", MaxKeys=1)
                        if 'Contents' in hudi_check or hudi_check.get('KeyCount', 0) > 0:
                            detected_type = "Hudi"
                            print(f"DEBUG User {g.user.id}: Found Hudi metadata for {prefix}")
                            discovered_tables.append({"path": table_path, "type": detected_type})
                            continue
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'AccessDenied':
                            print(f"Warning User {g.user.id}: S3 error checking Hudi metadata for {prefix}: {e}")
                        else: 
                            print(f"DEBUG User {g.user.id}: Access denied checking Hudi metadata for {prefix}")
                    except Exception as e: 
                        print(f"Warning User {g.user.id}: Unexpected error checking Hudi metadata for {prefix}: {e}")

                    # Optionally add Unknown types if needed
                    # if detected_type == "Unknown":
                    #     print(f"DEBUG User {g.user.id}: No specific table type found for {prefix}, listing as Unknown.")
                    #     discovered_tables.append({"path": table_path, "type": detected_type})


        end_time = time.time()
        print(f"INFO User {g.user.id}: Found {len(discovered_tables)} potential tables in {end_time - start_time:.2f} seconds.")
        return jsonify(discovered_tables)

    # --- Exception Handling (uses temporary s3_client context) ---
    except NoCredentialsError: # Should not happen if STS AssumeRole worked
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
    except ValueError as e: # Catch invalid S3 path format
         print(f"ERROR User {g.user.id}: ValueError listing tables: {e}")
         return jsonify({"error": f"Invalid input path: {str(e)}"}), 400
    except Exception as e:
         print(f"ERROR User {g.user.id}: An unexpected error occurred while listing tables: {e}")
         traceback.print_exc()
         return jsonify({"error": f"An unexpected server error occurred: {str(e)}"}), 500

@app.route("/generate-summary", methods=["POST"])
def generate():
    data = request.get_json()
    comparison_details_str = data.get("comparison", "") # Expecting a formatted string
    v1_label = data.get("v1_label", "Older Version") # Optional: Get version labels for context
    v2_label = data.get("v2_label", "Newer Version") # Optional

    if not comparison_details_str:
        return jsonify({"error": "Missing 'comparison' field in request body."}), 400

    # Refined prompt for summarizing statistical changes
    prompt = (
    f"Summarize the key statistical changes between two table snapshots: '{v1_label}' and '{v2_label}', based on the details below.\n\n"
    "Focus on significant increases or decreases in records, files, size, and deletes mentioned in the details. "
    "Keep the summary concise (1 sentence, maximum 2). "
    "The final output must be *only* the HTML paragraph itself, starting with `<p>` and ending with `</p>`. "
    "Use `<strong></strong>` tags for the keywords.\n\n"
    "Change Details:\n"
    f"{comparison_details_str}"
    )

    try:
        # Assuming get_gemini_response handles the API call to Gemini
        summary = get_gemini_response(prompt)
        return jsonify({"summary": summary}) # Return summary under 'summary' key
    except Exception as e:
        print(f"Error generating summary: {e}") # Log the error server-side
        traceback.print_exc() # Print full traceback for debugging
        return jsonify({"error": f"Failed to generate summary: {str(e)}"}), 500
    
@app.route('/recent_usage', methods=['GET'])
@login_required
def recent_usage():
    """Returns the recent S3 paths accessed by the logged-in user."""
    print(f"--- Fetching recent usage for User {g.user.id} ---")
    try:
        # <<< This query already sorts by timestamp descending (most recent first) >>>
        usages = S3Usage.query.filter_by(user_id=g.user.id)\
                              .order_by(S3Usage.timestamp.desc())\
                              .limit(20).all() # Limit to last 20

        usage_list = []
        for usage in usages:
            decrypted_path = decrypt_data(usage.s3_bucket_link_encrypted)
            usage_list.append({
                "path": decrypted_path if decrypted_path else "[Decryption Failed]",
                "timestamp": usage.timestamp.isoformat() + 'Z' # ISO 8601 UTC format
            })
        return jsonify(usage_list)

    except Exception as e:
        print(f"ERROR User {g.user.id}: Failed to retrieve usage history: {e}")
        traceback.print_exc()
        return jsonify({"error": "Failed to retrieve usage history"}), 500

@app.route('/ping', methods=['GET'])
def root():
     # Public endpoint, no login required
     return jsonify(status="API is running", timestamp=datetime.datetime.utcnow().isoformat() + 'Z')

@app.route('/', methods=['GET'])
def hello():
    return """
    Hello! Available endpoints:
     - /Iceberg?s3_url=&lt;s3://your-bucket/path/to/iceberg-table/&gt;
     - /Delta?s3_url=&lt;s3://your-bucket/path/to/delta-table/&gt;
    """

if __name__ == '__main__':
    with app.app_context():
        print("Initializing database...")
        try:
            db.create_all() # Create tables based on models if they don't exist
            print("Database tables checked/created.")
        except Exception as e:
            print(f"ERROR: Could not connect to or initialize database '{app.config['SQLALCHEMY_DATABASE_URI']}': {e}")
            print("Please ensure the database exists and connection details are correct in DATABASE_URL.")
            # Consider exiting if DB is essential: import sys; sys.exit(1)

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

    print("Starting Flask server...")
    app.run(debug=True, host='0.0.0.0', port=5000) # Use debug=False and Gunicorn/uWSGI for production