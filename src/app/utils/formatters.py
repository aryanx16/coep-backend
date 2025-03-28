# src/app/utils/formatters.py
import datetime

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
        # Standard ISO 8601 format ending with Z for UTC
        return dt_object.isoformat().replace('+00:00', 'Z')
    except (ValueError, TypeError):
        # Return original value if conversion fails
        return str(timestamp_ms)