import os
import json
import glob
import logging
import time
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from dotenv import load_dotenv

# Try to import Linux-specific modules
try:
    import pyinotify
    from systemd.journal import JournalHandler
    LINUX = True
except ImportError:
    LINUX = False
    
from google.cloud import storage
from google.oauth2 import service_account
from clickhouse_driver import Client

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)

# Configuration from environment variables with defaults
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'default')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_SERVICE_ACCOUNT_KEY = os.getenv('GCS_SERVICE_ACCOUNT_KEY')
WATCHED_FOLDER = os.getenv('WATCHED_FOLDER', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'documents'))

# Ensure required environment variables are set
if not GCS_BUCKET_NAME or not GCS_SERVICE_ACCOUNT_KEY:
    raise ValueError("GCS_BUCKET_NAME and GCS_SERVICE_ACCOUNT_KEY must be set in environment variables")

# Set up logging
if LINUX and 'journal' in os.getenv('LOG_TARGET', '').lower():
    # Use systemd journal for logging
    logger = logging.getLogger('pmen-sync')
    logger.addHandler(JournalHandler())
    logger.setLevel(logging.INFO)
else:
    # File-based logging
    log_dir = os.getenv('LOG_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs'))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'pmen-sync.log')
    
    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO'),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_clickhouse_connection():
    """Establish and test connection to ClickHouse."""
    try:
        logger.info("Attempting to connect to ClickHouse...")
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
            settings={'use_numpy': True}
        )
        # Test connection with a simple query
        client.execute('SELECT 1')
        logger.info("ClickHouse connection successful.")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}", exc_info=True)
        raise

def get_gcs_client():
    """Create, test, and return a GCS client."""
    try:
        logger.info("Attempting to connect to GCS...")
        credentials = service_account.Credentials.from_service_account_file(
            GCS_SERVICE_ACCOUNT_KEY
        )
        gcs_client = storage.Client(credentials=credentials)
        # Test connection by listing buckets
        gcs_client.list_buckets()
        logger.info("GCS connection successful.")
        return gcs_client
    except Exception as e:
        logger.error(f"Failed to connect to GCS: {e}", exc_info=True)
        raise

def query_clickhouse(client, query, params=None):
    """
    Execute query and return results as a list of dictionaries.
    
    Args:
        client: ClickHouse client instance
        query: SQL query string with %(param_name)s placeholders
        params: Dictionary of parameters to substitute in the query
    """
    if params:
        # Format the query with parameters
        query = query % params
    
    result = client.execute(query, with_column_types=True)
    columns = [col[0] for col in result[1]]
    return [dict(zip(columns, row)) for row in result[0]]

def upload_to_gcs(gcs_client, bucket_name: str, file_path: str, destination_blob_name: str) -> bool:
    """Upload a file to GCS bucket with detailed logging."""
    try:
        # Validate that the file path exists and is actually a file
        if not os.path.exists(file_path):
            logger.error(f"File does not exist: {file_path}")
            return False
            
        if not os.path.isfile(file_path):
            logger.error(f"Path is not a file (possibly a directory): {file_path}")
            return False
        
        start_time = datetime.now()
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
        
        logger.info(f"Starting upload: {file_path} (Size: {file_size:.2f}MB)")
        logger.debug(f"Destination: gs://{bucket_name}/{destination_blob_name}")
        
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        # Upload the file
        blob.upload_from_filename(file_path)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Upload successful: {file_path} -> gs://{bucket_name}/{destination_blob_name} "
                  f"(Duration: {duration:.2f}s, Speed: {file_size/max(0.1, duration):.2f}MB/s)")
        return True
        
    except Exception as e:
        logger.error(f"Failed to upload {file_path} to GCS: {str(e)}", exc_info=True)
        return False

def find_local_file(file_path: str, search_dir: str) -> Optional[str]:
    """Search for a file in the documents directory using the file_path."""
    # Get just the filename from the path
    filename = os.path.basename(file_path)
    
    # Skip if filename is empty or just whitespace
    if not filename or not filename.strip():
        return None
    
    # Search in the documents directory
    search_pattern = os.path.join(search_dir, '**', filename)
    matches = glob.glob(search_pattern, recursive=True)
    
    # Filter to only return actual files, not directories
    file_matches = [match for match in matches if os.path.isfile(match)]
    
    if file_matches:
        return os.path.abspath(file_matches[0])
    return None

def get_documents_from_clickhouse(ch_client, limit: int = 3000) -> list:
    """
    Retrieve documents from ClickHouse.
    Returns a list of document dictionaries.
    """
    query = """
    SELECT id_base, id_relasi, id_dokumen, kode_jenis_file, nomor, tahun, 
           judul, file, file_path, link
    FROM data.pmen_dokumen
    ORDER BY id_dokumen DESC
    LIMIT %(limit)s
    """
    
    logger.info(f"Retrieving up to {limit} documents from ClickHouse...")
    results = query_clickhouse(ch_client, query, params={'limit': limit})
    logger.info(f"Retrieved {len(results)} documents from ClickHouse")
    return results

def filter_unprocessed_documents(documents: list, processed_cache: set) -> tuple[list, dict]:
    """
    Filter out already processed documents.
    Returns a tuple of (unprocessed_docs, stats)
    """
    stats = {
        'total_documents': len(documents),
        'already_processed': 0,
        'to_process': 0
    }
    
    unprocessed = []
    for doc in documents:
        doc_id = doc.get('id_dokumen')
        if doc_id in processed_cache:
            stats['already_processed'] += 1
            logger.debug(f"Document {doc_id} already processed, skipping")
        else:
            unprocessed.append(doc)
    
    stats['to_process'] = len(unprocessed)
    logger.info(f"Filtered documents: {stats['already_processed']} already processed, "
              f"{stats['to_process']} to process")
    
    return unprocessed, stats

def process_documents(documents: list, gcs_client, search_dir: str, batch_size: int = 50) -> dict:
    """
    Process a list of documents and upload them to GCS.
    Returns a dictionary with processing statistics.
    """
    stats = {
        'total_documents': len(documents),
        'processed': 0,
        'skipped_no_path': 0,
        'not_found': 0,
        'upload_errors': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'processed_files': []
    }
    
    try:
        if not documents:
            logger.warning("No documents to process")
            return stats
        
        # Process documents in batches
        for i in range(0, len(documents), batch_size):
            batch = documents[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(documents)-1)//batch_size + 1} "
                      f"(Documents {i+1}-{min(i+batch_size, len(documents))})")
            
            for doc in batch:
                doc_id = doc.get('id_dokumen')
                file_path = doc.get('file_path', '')
                
                if not file_path:
                    stats['skipped_no_path'] += 1
                    logger.warning(f"Skipping document {doc_id} - No file path in database")
                    continue
                
                logger.debug(f"Processing document {doc_id} with file_path: '{file_path}'")
                
                # Find the local file
                local_file = find_local_file(file_path, search_dir)
                if not local_file:
                    stats['not_found'] += 1
                    logger.warning(f"File not found locally: {file_path} (Document ID: {doc_id})")
                    continue
                
                # Upload to GCS with the same path structure
                gcs_path = f"documents/main/{os.path.relpath(local_file, search_dir).replace(os.path.sep, '/')}"
                logger.debug(f"Processing document {doc_id}: {local_file} -> {gcs_path}")
                
                if upload_to_gcs(gcs_client, GCS_BUCKET_NAME, local_file, gcs_path):
                    stats['processed'] += 1
                    stats['processed_files'].append({
                        'id_dokumen': doc_id,
                        'local_path': local_file,
                        'gcs_path': gcs_path,
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.info(f"Successfully processed document {doc_id}")
                else:
                    stats['upload_errors'] += 1
                    logger.error(f"Failed to upload document {doc_id}")
        
        return stats
        
    except Exception as e:
        logger.critical(f"Critical error processing documents: {str(e)}", exc_info=True)
        raise
    finally:
        stats['end_time'] = datetime.now()
        duration = (stats['end_time'] - stats['start_time']).total_seconds()
        logger.info(f"Processing completed in {duration:.2f} seconds")
        logger.info(f"Summary: {stats['processed']} processed, "
                  f"{stats['skipped_no_path']} skipped (no path), "
                  f"{stats['not_found']} not found, "
                  f"{stats['upload_errors']} upload errors")

def save_processing_report(stats: dict) -> str:
    """
    Save processing statistics to a JSON file.
    Always uses the same filename and cleans up old reports.
    """
    try:
        # Create reports directory if it doesn't exist
        reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
        os.makedirs(reports_dir, exist_ok=True)
        
        # Use fixed filename
        filename = "sync_report.json"
        filepath = os.path.join(reports_dir, filename)
        
        # Clean up any old report files with timestamps
        for old_file in os.listdir(reports_dir):
            if old_file.startswith("sync_report_") and old_file.endswith(".json") and old_file != filename:
                try:
                    os.remove(os.path.join(reports_dir, old_file))
                    logger.info(f"Removed old report file: {old_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove old report file {old_file}: {str(e)}")
        
        # Save the report
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False, default=str)
            
        logger.info(f"Saved processing report to {filepath}")
        return filepath
        
    except Exception as e:
        logger.error(f"Failed to save processing report: {str(e)}", exc_info=True)
        return ""

def load_processed_cache() -> set:
    """Loads the most recent processing report and returns a set of processed document IDs."""
    try:
        report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
        if not os.path.exists(report_dir):
            logger.info("Reports directory not found, starting with an empty cache.")
            return set()

        # Check for both filename patterns
        list_of_reports = glob.glob(os.path.join(report_dir, 'sync_report.json'))
        if not list_of_reports:
            # Fall back to old pattern if new one not found
            list_of_reports = glob.glob(os.path.join(report_dir, 'sync_report_*.json'))
            
        if not list_of_reports:
            logger.info("No previous reports found, starting with an empty cache.")
            return set()

        latest_report = max(list_of_reports, key=os.path.getctime)
        logger.info(f"Loading cache from report: {latest_report}")

        with open(latest_report, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        processed_files = report_data.get('processed_files', [])
        processed_ids = {str(item['id_dokumen']) for item in processed_files if 'id_dokumen' in item}
        
        logger.info(f"Loaded {len(processed_ids)} processed document IDs into cache.")
        return processed_ids

    except Exception as e:
        logger.error(f"Failed to load processing cache: {e}", exc_info=True)
        return set()

def main():
    # Base directory for document search
    base_dir = os.path.dirname(os.path.abspath(__file__))
    documents_dir = WATCHED_FOLDER or os.path.join(base_dir, 'documents')
    
    # Create necessary directories
    os.makedirs(documents_dir, exist_ok=True)
    
    try:
        logger.info("=" * 80)
        logger.info(f"Starting GCS Synchronizer at {datetime.now().isoformat()}")
        logger.info(f"Document directory: {documents_dir}")
        logger.info(f"GCS Bucket: {GCS_BUCKET_NAME}")
        logger.info("-" * 80)
        
        # Initialize clients
        logger.info("Initializing ClickHouse client...")
        ch_client = get_clickhouse_connection()
        logger.info("Connected to ClickHouse")
        
        logger.info("Initializing GCS client...")
        gcs_client = get_gcs_client()
        logger.info("Initialized GCS client")
        
        # Step 1: Load processed cache
        logger.info("Loading processed documents cache...")
        processed_cache = load_processed_cache()
        
        # Step 2: Get all documents from ClickHouse
        logger.info("Retrieving documents from ClickHouse...")
        all_documents = get_documents_from_clickhouse(ch_client)
        
        if not all_documents:
            logger.warning("No documents found in ClickHouse")
            return
        
        # Step 3: Filter out already processed documents
        unprocessed_docs, filter_stats = filter_unprocessed_documents(all_documents, processed_cache)
        
        if not unprocessed_docs:
            logger.info("No new documents to process. All documents have already been processed.")
            return
            
        # Step 4: Process only the unprocessed documents
        logger.info(f"Processing {len(unprocessed_docs)} new documents...")
        process_stats = process_documents(unprocessed_docs, gcs_client, documents_dir)
        
        # Load existing report to get previously processed files
        try:
            report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
            report_file = os.path.join(report_dir, 'sync_report.json')
            if os.path.exists(report_file):
                with open(report_file, 'r', encoding='utf-8') as f:
                    existing_report = json.load(f)
                existing_processed = existing_report.get('processed_files', [])
                # Only keep existing entries that aren't in our new processed files
                existing_processed_ids = {str(item['id_dokumen']) for item in existing_processed if 'id_dokumen' in item}
                new_processed_ids = {str(item['id_dokumen']) for item in process_stats.get('processed_files', []) if 'id_dokumen' in item}
                
                # Keep only the existing entries that aren't in our new processed files
                existing_processed = [item for item in existing_processed 
                                   if str(item.get('id_dokumen', '')) not in new_processed_ids]
            else:
                existing_processed = []
        except Exception as e:
            logger.warning(f"Could not load existing report to merge processed files: {e}")
            existing_processed = []
        
        # Combine statistics and merge processed files
        stats = {
            **filter_stats,
            **process_stats,
            'start_time': datetime.now(),
            'end_time': None,
            'processed_files': existing_processed + process_stats.get('processed_files', [])
        }
        
        # Save processing report
        report_file = save_processing_report(stats)
        if report_file:
            logger.info(f"Processing report saved to: {report_file}")
        
        logger.info("GCS Synchronizer completed successfully")
        
    except Exception as e:
        logger.critical(f"Fatal error in main process: {str(e)}", exc_info=True)
        raise
    finally:
        if 'ch_client' in locals():
            ch_client.disconnect()
            logger.info("Disconnected from ClickHouse")
        
        logger.info("=" * 80)

class PMENFileWatcher:
    def __init__(self, watch_dir):
        self.watch_dir = watch_dir
        self.wm = pyinotify.WatchManager()
        self.mask = pyinotify.IN_CREATE | pyinotify.IN_MODIFY
        
    def process_event(self, event):
        if not event.dir and event.pathname.endswith(('.pdf', '.PDF')):
            logger.info(f"Detected change in file: {event.pathname}")
            # Trigger document processing
            main()
    
    def watch(self):
        logger.info(f"Starting file watcher on directory: {self.watch_dir}")
        handler = lambda event: self.process_event(event)
        self.notifier = pyinotify.ThreadedNotifier(self.wm, pyinotify.ProcessEvent())
        self.wm.add_watch(self.watch_dir, self.mask, rec=True, auto_add=True)
        self.notifier.start()
        
        # Keep the main thread alive
        signal.signal(signal.SIGTERM, self.shutdown)
        signal.signal(signal.SIGINT, self.shutdown)
        
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.shutdown()
    
    def shutdown(self, signum=None, frame=None):
        logger.info("Shutting down file watcher...")
        if hasattr(self, 'notifier'):
            self.notifier.stop()
        sys.exit(0)

def run_as_service():
    """Run the script as a service with file watching"""
    if not LINUX:
        logger.error("File watching is only supported on Linux with pyinotify")
        return main()
    
    # Initial run
    main()
    
    # Start file watcher
    watcher = PMENFileWatcher(WATCHED_FOLDER)
    watcher.watch()

def resync_documents(ch_client, gcs_client, documents_dir: str) -> dict:
    """
    Resync by comparing ClickHouse data with local documents one by one.
    Skips documents already recorded in sync_report.json.
    """
    stats = {
        'total_documents': 0,
        'already_synced': 0,
        'file_not_found': 0,
        'newly_synced': 0,
        'sync_errors': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'processed_files': []
    }
    
    try:
        logger.info("Starting resync operation...")
        
        # Load processed cache from sync_report.json
        processed_cache = load_processed_cache()
        logger.info(f"Loaded {len(processed_cache)} already processed documents from cache")
        
        # Get all documents from ClickHouse
        logger.info("Retrieving all documents from ClickHouse...")
        all_documents = get_documents_from_clickhouse(ch_client)
        stats['total_documents'] = len(all_documents)
        
        if not all_documents:
            logger.warning("No documents found in ClickHouse")
            return stats
        
        logger.info(f"Found {len(all_documents)} documents in ClickHouse")
        logger.info("Starting one-by-one comparison...")
        
        # Process each document one by one
        for i, doc in enumerate(all_documents, 1):
            doc_id = doc.get('id_dokumen')
            file_path = doc.get('file_path', '').strip()
            
            logger.info(f"[{i}/{len(all_documents)}] Processing document {doc_id}")
            
            # Skip if already in sync report
            if str(doc_id) in processed_cache:
                stats['already_synced'] += 1
                logger.info(f"  [SKIP] Document {doc_id} already synced, skipping")
                continue
            
            # Skip if no file path
            if not file_path:
                logger.warning(f"  [WARN] Document {doc_id} has no file_path, skipping")
                continue
            
            logger.info(f"  [SEARCH] Looking for file: '{file_path}'")
            
            # Find the local file
            local_file = find_local_file(file_path, documents_dir)
            if not local_file:
                stats['file_not_found'] += 1
                logger.warning(f"  [NOT FOUND] File not found locally: '{file_path}'")
                continue
            
            logger.info(f"  [FOUND] Found local file: {local_file}")
            
            # Upload to GCS
            gcs_path = f"documents/main/{os.path.relpath(local_file, documents_dir).replace(os.path.sep, '/')}"
            logger.info(f"  [UPLOAD] Uploading to GCS: {gcs_path}")
            
            if upload_to_gcs(gcs_client, GCS_BUCKET_NAME, local_file, gcs_path):
                stats['newly_synced'] += 1
                stats['processed_files'].append({
                    'id_dokumen': doc_id,
                    'local_path': local_file,
                    'gcs_path': gcs_path,
                    'timestamp': datetime.now().isoformat()
                })
                logger.info(f"  [SUCCESS] Successfully synced document {doc_id}")
            else:
                stats['sync_errors'] += 1
                logger.error(f"  [ERROR] Failed to sync document {doc_id}")
            
            # Add a small delay to avoid overwhelming the system
            time.sleep(0.1)
        
        return stats
        
    except Exception as e:
        logger.critical(f"Critical error during resync: {str(e)}", exc_info=True)
        raise
    finally:
        stats['end_time'] = datetime.now()
        duration = (stats['end_time'] - stats['start_time']).total_seconds()
        logger.info(f"Resync completed in {duration:.2f} seconds")
        logger.info(f"Resync Summary: {stats['newly_synced']} newly synced, "
                  f"{stats['already_synced']} already synced, "
                  f"{stats['file_not_found']} files not found, "
                  f"{stats['sync_errors']} sync errors")

def main_resync():
    """Main function for resync operation"""
    # Base directory for document search
    base_dir = os.path.dirname(os.path.abspath(__file__))
    documents_dir = WATCHED_FOLDER or os.path.join(base_dir, 'documents')
    
    # Create necessary directories
    os.makedirs(documents_dir, exist_ok=True)
    
    try:
        logger.info("=" * 80)
        logger.info(f"Starting GCS RESYNC at {datetime.now().isoformat()}")
        logger.info(f"Document directory: {documents_dir}")
        logger.info(f"GCS Bucket: {GCS_BUCKET_NAME}")
        logger.info("-" * 80)
        
        # Initialize clients
        logger.info("Initializing ClickHouse client...")
        ch_client = get_clickhouse_connection()
        logger.info("Connected to ClickHouse")
        
        logger.info("Initializing GCS client...")
        gcs_client = get_gcs_client()
        logger.info("Initialized GCS client")
        
        # Perform enhanced resync operation with file validation
        resync_stats = resync_with_file_validation(ch_client, gcs_client, documents_dir)
        
        # Load existing report to merge with new results
        try:
            report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
            report_file = os.path.join(report_dir, 'sync_report.json')
            if os.path.exists(report_file):
                with open(report_file, 'r', encoding='utf-8') as f:
                    existing_report = json.load(f)
                existing_processed = existing_report.get('processed_files', [])
            else:
                existing_processed = []
        except Exception as e:
            logger.warning(f"Could not load existing report: {e}")
            existing_processed = []
        
        # Merge the newly processed files with existing ones
        all_processed_files = existing_processed + resync_stats.get('processed_files', [])
        
        # Create final stats for the report
        final_stats = {
            **resync_stats,
            'processed_files': all_processed_files,
            'total_processed_files': len(all_processed_files)
        }
        
        # Save updated processing report
        report_file = save_processing_report(final_stats)
        if report_file:
            logger.info(f"Updated processing report saved to: {report_file}")
        
        logger.info("GCS Resync completed successfully")
        
    except Exception as e:
        logger.critical(f"Fatal error in resync process: {str(e)}", exc_info=True)
        raise
    finally:
        if 'ch_client' in locals():
            ch_client.disconnect()
            logger.info("Disconnected from ClickHouse")
        
        logger.info("=" * 80)

def resync_with_file_validation(ch_client, gcs_client, documents_dir: str) -> dict:
    """
    Enhanced resync that validates file paths between ClickHouse and sync report.
    Updates records when file paths have changed in the database.
    """
    stats = {
        'total_documents': 0,
        'already_synced': 0,
        'file_path_changed': 0,
        'file_not_found': 0,
        'newly_synced': 0,
        'sync_errors': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'processed_files': []
    }
    
    try:
        logger.info("Starting enhanced resync with file validation...")
        
        # Load processed cache from sync_report.json
        processed_cache = load_processed_cache()
        logger.info(f"Loaded {len(processed_cache)} already processed documents from cache")
        
        # Load the full sync report to check file paths
        report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
        report_file = os.path.join(report_dir, 'sync_report.json')
        existing_records = {}
        
        if os.path.exists(report_file):
            with open(report_file, 'r', encoding='utf-8') as f:
                existing_report = json.load(f)
            # Create a map of id_dokumen -> file info from existing report
            for item in existing_report.get('processed_files', []):
                doc_id = str(item.get('id_dokumen', ''))
                if doc_id:
                    existing_records[doc_id] = item
        
        # Get all documents from ClickHouse
        logger.info("Retrieving all documents from ClickHouse...")
        all_documents = get_documents_from_clickhouse(ch_client)
        stats['total_documents'] = len(all_documents)
        
        if not all_documents:
            logger.warning("No documents found in ClickHouse")
            return stats
        
        logger.info(f"Found {len(all_documents)} documents in ClickHouse")
        logger.info("Starting enhanced one-by-one comparison...")
        
        # Process each document one by one
        for i, doc in enumerate(all_documents, 1):
            doc_id = doc.get('id_dokumen')
            file_path = doc.get('file_path', '').strip()
            
            logger.info(f"[{i}/{len(all_documents)}] Processing document {doc_id}")
            
            # Skip if no file path
            if not file_path:
                logger.warning(f"  [WARN] Document {doc_id} has no file_path, skipping")
                continue
            
            # Check if this document is in our cache
            if str(doc_id) in processed_cache:
                # Validate if the file path has changed
                existing_record = existing_records.get(str(doc_id))
                if existing_record:
                    # Extract the relative path from the existing local_path
                    existing_local_path = existing_record.get('local_path', '')
                    if existing_local_path:
                        # Convert to relative path for comparison
                        try:
                            existing_rel_path = os.path.relpath(existing_local_path, documents_dir).replace(os.path.sep, '/')
                        except:
                            existing_rel_path = ""
                        
                        # Compare with current file_path
                        if existing_rel_path != file_path:
                            stats['file_path_changed'] += 1
                            logger.warning(f"  [CHANGED] File path changed for document {doc_id}:")
                            logger.warning(f"    Old: {existing_rel_path}")
                            logger.warning(f"    New: {file_path}")
                            # Remove from cache so it gets reprocessed
                            processed_cache.discard(str(doc_id))
                        else:
                            stats['already_synced'] += 1
                            logger.info(f"  [SKIP] Document {doc_id} already synced, skipping")
                            continue
                    else:
                        # No local path in existing record, treat as needs reprocessing
                        logger.warning(f"  [NO PATH] Existing record for {doc_id} has no local_path, reprocessing")
                        processed_cache.discard(str(doc_id))
                else:
                    # In cache but no record found, treat as needs reprocessing
                    logger.warning(f"  [NO RECORD] Document {doc_id} in cache but no record found, reprocessing")
                    processed_cache.discard(str(doc_id))
            
            # If we reach here, document needs to be processed
            logger.info(f"  [SEARCH] Looking for file: '{file_path}'")
            
            # Find the local file
            local_file = find_local_file(file_path, documents_dir)
            if not local_file:
                stats['file_not_found'] += 1
                logger.warning(f"  [NOT FOUND] File not found locally: '{file_path}'")
                continue
            
            logger.info(f"  [FOUND] Found local file: {local_file}")
            
            # Upload to GCS
            gcs_path = f"documents/main/{os.path.relpath(local_file, documents_dir).replace(os.path.sep, '/')}"
            logger.info(f"  [UPLOAD] Uploading to GCS: {gcs_path}")
            
            if upload_to_gcs(gcs_client, GCS_BUCKET_NAME, local_file, gcs_path):
                stats['newly_synced'] += 1
                stats['processed_files'].append({
                    'id_dokumen': doc_id,
                    'local_path': local_file,
                    'gcs_path': gcs_path,
                    'timestamp': datetime.now().isoformat()
                })
                logger.info(f"  [SUCCESS] Successfully synced document {doc_id}")
            else:
                stats['sync_errors'] += 1
                logger.error(f"  [ERROR] Failed to sync document {doc_id}")
            
            # Add a small delay to avoid overwhelming the system
            time.sleep(0.1)
        
        return stats
        
    except Exception as e:
        logger.critical(f"Critical error during enhanced resync: {str(e)}", exc_info=True)
        raise
    finally:
        stats['end_time'] = datetime.now()
        duration = (stats['end_time'] - stats['start_time']).total_seconds()
        logger.info(f"Enhanced resync completed in {duration:.2f} seconds")
        logger.info(f"Enhanced Resync Summary: {stats['newly_synced']} newly synced, "
                  f"{stats['already_synced']} already synced, "
                  f"{stats['file_path_changed']} file paths changed, "
                  f"{stats['file_not_found']} files not found, "
                  f"{stats['sync_errors']} sync errors")

if __name__ == "__main__":
    if '--resync' in sys.argv:
        main_resync()
    elif '--resync-validate' in sys.argv:
        # For backward compatibility, --resync now uses validation by default
        main_resync()
    elif '--service' in sys.argv or os.getenv('RUN_AS_SERVICE', '').lower() == 'true':
        run_as_service()
    else:
        main()




