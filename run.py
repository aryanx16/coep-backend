# src/run.py
import os
from dotenv import load_dotenv

# Load environment variables from .env BEFORE creating the app
# Ensures that the Config class in src/app/config.py reads the variables
print("Loading environment variables from .env file...")
load_dotenv()
print(f".env loaded. AWS_REGION from env: {os.getenv('AWS_REGION')}") # Debug print

# Import the application factory function AFTER loading .env
from src.app import create_app

# Create the Flask app instance using the factory
# This will load config, register blueprints, etc.
print("Creating Flask app instance...")
app = create_app()
print("Flask app instance created.")


if __name__ == '__main__':
    # Get host and port from environment variables or use defaults
    host = os.environ.get('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.environ.get('FLASK_RUN_PORT', 5000))
    # Get debug mode from environment variable (e.g., FLASK_DEBUG=1) or default to True for development
    debug_mode = os.environ.get('FLASK_DEBUG', '1').lower() in ['true', '1', 't']

    print(f"Starting Flask server on {host}:{port} (Debug Mode: {debug_mode})")
    # Run the Flask development server
    # Use debug=False in production environments!
    app.run(host=host, port=port, debug=debug_mode)