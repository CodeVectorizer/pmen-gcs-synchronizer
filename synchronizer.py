# synchronizer.py

import os
import time
import logging
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from google.cloud import storage
from clickhouse_driver import Client
import config

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- GCS Connection ---
def get_gcs_client():
    """Initializes and returns a GCS client."""
    if config.GCS_SERVICE_ACCOUNT_KEY:
        return storage.Client.from_service_account_json(config.GCS_SERVICE_ACCOUNT_KEY)
    return storage.Client()

# --- ClickHouse Connection ---
def get_clickhouse_client():
    """Initializes and returns a ClickHouse client."""
    return Client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DATABASE
    )

# --- File Validation ---
def get_db_filepath(local_file_path, clickhouse_client):
    """Fetches the file_path from ClickHouse that corresponds to the local file."""
    # Get the relative path from the 'document' directory
    relative_path = os.path.relpath(local_file_path, config.WATCHED_FOLDER).replace('\\', '/')

    # Query to find a matching file_path in the database
    # This assumes your table is named 'dokumen' and the column is 'file_path'
    query = f"SELECT file_path FROM dokumen WHERE file_path = '{relative_path}'"

    try:
        result = clickhouse_client.execute(query)
        if result:
            # Return the file_path from the database
            return result[0][0]
    except Exception as e:
        logging.error(f"Error validating file {relative_path} in ClickHouse: {e}")
    
    return None

# --- Cache Management (JSON) ---
def read_cache(cache_file):
    """Reads a list of file paths from a JSON cache file."""
    if not os.path.exists(cache_file):
        return []
    try:
        with open(cache_file, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []

def write_cache(data, cache_file):
    """Writes a list of file paths to a JSON cache file."""
    with open(cache_file, 'w') as f:
        json.dump(data, f, indent=4)

def add_to_cache(file_path, cache_file):
    """Adds a file path to a JSON cache file, avoiding duplicates."""
    cache_data = read_cache(cache_file)
    if file_path not in cache_data:
        cache_data.append(file_path)
        write_cache(cache_data, cache_file)

def is_in_cache(file_path, cache_file):
    """Checks if a file path is in a JSON cache file."""
    return file_path in read_cache(cache_file)

# --- File System Event Handler ---
class Watcher(FileSystemEventHandler):
    """Handles file system events."""
    def on_any_event(self, event):
        if event.is_directory:
            return

        # On created or modified, add to the 'update-soon' cache
        if event.event_type in ['created', 'modified']:
            logging.info(f"Detected {event.event_type}: {event.src_path}")
            if not is_in_cache(event.src_path, config.UPDATE_SOON_CACHE):
                add_to_cache(event.src_path, config.UPDATE_SOON_CACHE)
                logging.info(f"Added to update-soon cache: {event.src_path}")

        # On deleted, you might want to handle this as well.
        # For now, we'll just log it.
        if event.event_type == 'deleted':
            logging.info(f"Detected deleted file: {event.src_path}")

# --- Main Synchronization Logic ---
def process_pending_files():
    """Processes files listed in the 'update-soon' cache."""
    if not os.path.exists(config.UPDATE_SOON_CACHE):
        logging.info("No update-soon cache file found. Nothing to process.")
        return

    gcs_client = get_gcs_client()
    clickhouse_client = get_clickhouse_client()
    bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)

    pending_files = read_cache(config.UPDATE_SOON_CACHE)
    if not pending_files:
        logging.info("No pending files to process.")
        return

    gcs_client = get_gcs_client()
    clickhouse_client = get_clickhouse_client()
    bucket = gcs_client.bucket(config.GCS_BUCKET_NAME)

    processed_in_session = []

    for file_path in pending_files:
        if not os.path.exists(file_path):
            logging.warning(f"File not found, skipping: {file_path}")
            processed_in_session.append(file_path)
            continue

        if is_in_cache(file_path, config.PROCESSED_FILES_LOG):
            logging.info(f"Already processed, skipping: {file_path}")
            processed_in_session.append(file_path)
            continue

        db_filepath = get_db_filepath(file_path, clickhouse_client)
        if db_filepath:
            logging.info(f"File is valid, uploading to GCS path: {db_filepath}")
            try:
                blob = bucket.blob(db_filepath)
                blob.upload_from_filename(file_path)
                logging.info(f"Successfully uploaded: {file_path}")
                add_to_cache(file_path, config.PROCESSED_FILES_LOG)
                processed_in_session.append(file_path)
            except Exception as e:
                logging.error(f"Failed to upload {file_path}: {e}")
        else:
            logging.warning(f"File not found in ClickHouse, skipping: {file_path}")
            processed_in_session.append(file_path)

    # Clean up the update-soon cache
    remaining_files = [p for p in pending_files if p not in processed_in_session]
    write_cache(remaining_files, config.UPDATE_SOON_CACHE)

if __name__ == '__main__':
    # This part handles manual triggering and file watching.
    # The scheduler will call process_pending_files() directly.

    # First, process any files that are already in the cache
    process_pending_files()

    # Then, start watching for new changes
    event_handler = Watcher()
    observer = Observer()
    observer.schedule(event_handler, config.WATCHED_FOLDER, recursive=True)
    observer.start()
    logging.info(f"Watching for file changes in {config.WATCHED_FOLDER}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
