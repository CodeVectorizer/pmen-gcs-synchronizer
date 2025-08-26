# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

def get_env_variable(name: str, default: str = None) -> str:
    """Get environment variable or return default."""
    value = os.getenv(name, default)
    if value is None:
        raise ValueError(f"Environment variable {name} is not set")
    return value

def get_absolute_path(path: str) -> str:
    """Convert relative path to absolute path relative to config file."""
    if not path:
        return path
    return str((Path(__file__).parent / path).resolve())

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET_NAME = get_env_variable('GCS_BUCKET_NAME')
GCS_SERVICE_ACCOUNT_KEY = get_absolute_path(
    get_env_variable('GCS_SERVICE_ACCOUNT_KEY')
)


# PostgreSQL Configuration
POSTGRES_HOST = get_env_variable('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = int(get_env_variable('POSTGRES_PORT', '5432'))
POSTGRES_USER = get_env_variable('POSTGRES_USER', 'pmendika')
POSTGRES_PASSWORD = get_env_variable('POSTGRES_PASSWORD', 'AppPm3n2025')
POSTGRES_DB = get_env_variable('POSTGRES_DB', 'pmen')
POSTGRES_SCHEMA = get_env_variable('POSTGRES_SCHEMA', 'transaksi')
POSTGRES_VIEW = get_env_variable('POSTGRES_VIEW', 'v_dokumen')

# Folder to Watch
WATCHED_FOLDER = get_absolute_path(
    get_env_variable('WATCHED_FOLDER', 'documents')
)

# Cache File Paths
CACHE_DIR = get_absolute_path('.cache')
PROCESSED_FILES_LOG = os.path.join(CACHE_DIR, 'processed.log')
UPDATE_SOON_CACHE = os.path.join(CACHE_DIR, 'update-soon.log')

# Create cache directory if it doesn't exist
os.makedirs(CACHE_DIR, exist_ok=True)
