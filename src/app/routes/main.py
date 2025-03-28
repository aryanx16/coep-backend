# src/app/routes/main.py
from flask import Blueprint, current_app, jsonify

# Create a Blueprint object for main routes
main_bp = Blueprint('main', __name__)

@main_bp.route('/', methods=['GET'])
def hello():
    """Provides basic API information."""
    # Use triple quotes for multi-line strings
    info_html = """
    Hello! Available endpoints:
     - /Iceberg?s3_url=&lt;s3://your-bucket/path/to/iceberg-table/&gt;
     - /Delta?s3_url=&lt;s3://your-bucket/path/to/delta-table/&gt;
    """
    # Returning HTML directly might be okay for simple info,
    # but a JSON response is more typical for APIs.
    # return info_html
    return jsonify({
        "message": "Welcome to the Table Explorer API!",
        "endpoints": {
            "iceberg": "/Iceberg?s3_url=<s3://bucket/path/to/iceberg/>",
            "delta": "/Delta?s3_url=<s3://bucket/path/to/delta/>"
        }
    })

# Add other general routes here if needed