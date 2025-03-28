# src/app/config.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Base configuration class."""
    # Secret key for Flask sessions, CSRF protection, etc. (Good practice, though not strictly needed for this API)
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'a-default-secret-key-for-dev'

    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

    # CORS Configuration (Allow all origins for simplicity, adjust for production)
    CORS_ORIGINS = "*"

    # Add other configurations if needed