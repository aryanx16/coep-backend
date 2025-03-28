# src/app/utils/serializers.py
import json
import datetime
import time
# Import numpy if you expect numpy types from pyarrow, otherwise remove
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

def convert_bytes(obj):
    """
    Recursively converts various types within a nested structure for JSON serialization.
    Handles bytes, datetimes, numpy types (if available), and other non-standard types.
    """
    if isinstance(obj, bytes):
        try:
            # Try decoding as UTF-8, replace errors to avoid crashing
            return obj.decode('utf-8', errors='replace')
        except Exception as e:
            # Fallback if decoding fails entirely
            return f"<bytes len={len(obj)} decode_error='{e}'>"
    elif isinstance(obj, dict):
        # Recursively process dictionary keys and values
        return {convert_bytes(k): convert_bytes(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        # Recursively process list items
        return [convert_bytes(item) for item in obj]
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        # Format dates/datetimes to ISO standard
        return obj.isoformat()
    elif isinstance(obj, time.struct_time):
        # Convert time.struct_time to a datetime object first, then format
        try:
            return datetime.datetime.fromtimestamp(time.mktime(obj)).isoformat()
        except Exception:
            return str(obj) # Fallback
    elif HAS_NUMPY and isinstance(obj, np.generic):
        # Convert numpy scalar types (like np.int64) to standard Python types
        try:
            return obj.item()
        except Exception:
            return str(obj) # Fallback
    elif HAS_NUMPY and isinstance(obj, np.ndarray):
        # Convert numpy arrays to lists (potentially large!)
        # Consider summarizing or omitting large arrays in production
        print(f"DEBUG: Converting numpy array (shape {obj.shape}) to list for serialization.")
        return convert_bytes(obj.tolist()) # Recursively convert elements within the list
    elif hasattr(obj, 'isoformat'):
        # General catch for other date/time like objects with an isoformat method
        try:
            return obj.isoformat()
        except Exception:
            return str(obj) # Fallback

    # Final check: If it's not a basic serializable type, convert to string.
    # Basic types (str, int, float, bool, None) are returned as is.
    if not isinstance(obj, (str, int, float, bool, type(None))):
        try:
            # Test if it's already JSON serializable (e.g., a simple tuple)
            json.dumps(obj)
            return obj
        except TypeError:
            # If not serializable, convert to string as a last resort
            print(f"DEBUG: Converting unknown type {type(obj)} to string for serialization.")
            return str(obj)

    # Return basic types or already processed types
    return obj