# src/app/__init__.py
from flask import Flask
from flask_cors import CORS
import boto3

# Import configuration
from .config import Config

# Import blueprint objects
from .routes.main import main_bp
from .routes.iceberg import iceberg_bp
from .routes.delta import delta_bp

def create_app(config_class=Config):
    """
    Application factory function to create and configure the Flask app.
    """
    app = Flask(__name__, instance_relative_config=True)

    # Load configuration from config object
    app.config.from_object(config_class)

    # Load instance config if it exists (e.g., instance/config.py)
    # app.config.from_pyfile('config.py', silent=True) # Optional

    print("--- Application Configuration ---")
    print(f"AWS Region: {app.config.get('AWS_REGION')}")
    # Avoid printing secrets!
    # print(f"AWS Key ID Loaded: {bool(app.config.get('AWS_ACCESS_KEY_ID'))}")
    print("-------------------------------")


    # --- Initialize Extensions ---
    # Enable CORS using configuration
    CORS(app, resources={r"/*": {"origins": app.config.get("CORS_ORIGINS", "*")}})
    print(f"CORS enabled for origins: {app.config.get('CORS_ORIGINS', '*')}")


    # --- Register Blueprints (Routes) ---
    # Blueprints organize groups of routes
    app.register_blueprint(main_bp)      # For '/' route
    app.register_blueprint(iceberg_bp)   # For '/Iceberg' routes
    app.register_blueprint(delta_bp)     # For '/Delta' routes
    print("Registered blueprints: main, iceberg, delta")

    # --- Add Context Processors or Error Handlers if needed ---
    # Example: Make S3 client available globally (alternative to creating in routes)
    # @app.before_request
    # def before_request():
    #     g.s3_client = boto3.client(
    #            's3',
    #             aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
    #             aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY'],
    #             region_name=app.config['AWS_REGION']
    #     )

    # --- Return the configured app instance ---
    return app