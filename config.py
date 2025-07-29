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

# ClickHouse Configuration
CLICKHOUSE_HOST = get_env_variable('CLICKHOUSE_HOST', 'localhost')
# The clickhouse-driver library uses the native TCP protocol, which defaults to port 9000.
# Port 8123 is for the HTTP interface and will cause an EOFError with this driver.
CLICKHOUSE_PORT = int(get_env_variable('CLICKHOUSE_PORT', '9000'))
CLICKHOUSE_USER = get_env_variable('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = get_env_variable('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = get_env_variable('CLICKHOUSE_DATABASE', 'default')

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
