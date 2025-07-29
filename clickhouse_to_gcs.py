import os
import json
import glob
import logging
from datetime import datetime
from pathlib import Path
from google.cloud import storage
from google.oauth2 import service_account
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, 
    CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE,
    GCS_BUCKET_NAME, GCS_SERVICE_ACCOUNT_KEY, WATCHED_FOLDER
)
from typing import List, Dict, Optional

# Set up logging
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f'sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

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

def query_clickhouse(client, query):
    """Execute query and return results as a list of dictionaries."""
    result = client.execute(query, with_column_types=True)
    columns = [col[0] for col in result[1]]
    return [dict(zip(columns, row)) for row in result[0]]

def upload_to_gcs(gcs_client, bucket_name: str, file_path: str, destination_blob_name: str) -> bool:
    """Upload a file to GCS bucket with detailed logging."""
    try:
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
    
    # Search in the documents directory
    search_pattern = os.path.join(search_dir, '**', filename)
    matches = glob.glob(search_pattern, recursive=True)
    
    if matches:
        return os.path.abspath(matches[0])
    return None

def get_documents_from_clickhouse(ch_client, limit: int = 1000) -> list:
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
                
                # Find the local file
                local_file = find_local_file(file_path, search_dir)
                if not local_file:
                    stats['not_found'] += 1
                    logger.warning(f"File not found locally: {file_path} (Document ID: {doc_id})")
                    continue
                
                # Upload to GCS with the same path structure
                gcs_path = os.path.relpath(local_file, search_dir).replace(os.path.sep, '/')
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
    """Save processing statistics to a JSON report file."""
    try:
        report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reports')
        os.makedirs(report_dir, exist_ok=True)
        
        report_file = os.path.join(report_dir, f"sync_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        # Calculate duration
        if stats['start_time'] and stats['end_time']:
            stats['duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()
        
        # Convert datetime objects to strings for JSON serialization
        for time_key in ['start_time', 'end_time']:
            if stats.get(time_key):
                stats[time_key] = stats[time_key].isoformat()
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
            
        logger.info(f"Saved processing report to {report_file}")
        return report_file
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

        list_of_reports = glob.glob(os.path.join(report_dir, 'sync_report_*.json'))
        if not list_of_reports:
            logger.info("No previous reports found, starting with an empty cache.")
            return set()

        latest_report = max(list_of_reports, key=os.path.getctime)
        logger.info(f"Loading cache from the latest report: {latest_report}")

        with open(latest_report, 'r', encoding='utf-8') as f:
            report_data = json.load(f)
        
        processed_files = report_data.get('processed_files', [])
        processed_ids = {item['id_dokumen'] for item in processed_files}
        
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
        
        # Combine statistics
        stats = {
            **filter_stats,
            **process_stats,
            'start_time': datetime.now(),
            'end_time': None,
            'processed_files': process_stats.get('processed_files', [])
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

if __name__ == "__main__":
    main()


    

