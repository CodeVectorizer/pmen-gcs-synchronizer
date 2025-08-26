"""
Test cases for the PostgreSQL-based GCS Synchronizer
"""
import unittest
import os
import tempfile
import shutil
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import psycopg2

# Import the modules to test
import clickhouse_to_gcs
import config


class TestPostgreSQLConnection(unittest.TestCase):
    """Test PostgreSQL connection functionality"""
    
    @patch('clickhouse_to_gcs.psycopg2.connect')
    def test_postgres_connection_success(self, mock_connect):
        """Test successful PostgreSQL connection"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        conn = clickhouse_to_gcs.get_postgres_connection()
        
        mock_connect.assert_called_once_with(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
            dbname=config.POSTGRES_DB
        )
        mock_cursor.execute.assert_called_once_with('SELECT 1')
        self.assertEqual(conn, mock_conn)
    
    @patch('clickhouse_to_gcs.psycopg2.connect')
    def test_postgres_connection_failure(self, mock_connect):
        """Test PostgreSQL connection failure"""
        mock_connect.side_effect = psycopg2.Error("Connection failed")
        
        with self.assertRaises(psycopg2.Error):
            clickhouse_to_gcs.get_postgres_connection()


class TestQueryPostgreSQL(unittest.TestCase):
    """Test PostgreSQL query functionality"""
    
    def test_query_postgres_success(self):
        """Test successful PostgreSQL query execution"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        # Mock query results
        mock_row1 = {'id': 1, 'name': 'Document 1'}
        mock_row2 = {'id': 2, 'name': 'Document 2'}
        mock_cursor.fetchall.return_value = [mock_row1, mock_row2]
        
        query = "SELECT * FROM test_table"
        results = clickhouse_to_gcs.query_postgres(mock_conn, query)
        
        mock_cursor.execute.assert_called_once_with(query, None)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], mock_row1)
        self.assertEqual(results[1], mock_row2)
    
    def test_query_postgres_with_params(self):
        """Test PostgreSQL query with parameters"""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        
        query = "SELECT * FROM test_table WHERE id = %(id)s"
        params = {'id': 1}
        
        clickhouse_to_gcs.query_postgres(mock_conn, query, params)
        
        mock_cursor.execute.assert_called_once_with(query, params)


class TestGetDocumentsFromPostgreSQL(unittest.TestCase):
    """Test document retrieval from PostgreSQL"""
    
    @patch('clickhouse_to_gcs.query_postgres')
    def test_get_documents_success(self, mock_query):
        """Test successful document retrieval"""
        mock_conn = Mock()
        mock_documents = [
            {
                'id_base': 1,
                'id_relasi': 1,
                'id_dokumen': 123,
                'kode_jenis_file': 'PDF',
                'nomor': '001/2025',
                'tahun': 2025,
                'judul': 'Test Document',
                'file': 'test.pdf',
                'file_path': '/documents/test.pdf',
                'link': 'http://example.com'
            }
        ]
        mock_query.return_value = mock_documents
        
        result = clickhouse_to_gcs.get_documents_from_postgres(mock_conn, limit=100)
        
        expected_query = f"""
    SELECT id_base, id_relasi, id_dokumen, kode_jenis_file, nomor, tahun,
           judul, file, file_path, link
    FROM {config.POSTGRES_SCHEMA}.{config.POSTGRES_VIEW}
    ORDER BY id_dokumen DESC
    LIMIT %(limit)s
    """
        mock_query.assert_called_once_with(mock_conn, expected_query, params={'limit': 100})
        self.assertEqual(result, mock_documents)
    
    @patch('clickhouse_to_gcs.query_postgres')
    def test_get_documents_empty_result(self, mock_query):
        """Test document retrieval with empty result"""
        mock_conn = Mock()
        mock_query.return_value = []
        
        result = clickhouse_to_gcs.get_documents_from_postgres(mock_conn)
        
        self.assertEqual(result, [])


class TestGCSClient(unittest.TestCase):
    """Test GCS client functionality"""
    
    @patch('clickhouse_to_gcs.storage.Client')
    @patch('clickhouse_to_gcs.service_account.Credentials.from_service_account_file')
    def test_gcs_client_success(self, mock_credentials, mock_storage_client):
        """Test successful GCS client creation"""
        mock_creds = Mock()
        mock_credentials.return_value = mock_creds
        mock_client = Mock()
        mock_storage_client.return_value = mock_client
        
        client = clickhouse_to_gcs.get_gcs_client()
        
        mock_credentials.assert_called_once_with(config.GCS_SERVICE_ACCOUNT_KEY)
        mock_storage_client.assert_called_once_with(credentials=mock_creds)
        mock_client.list_buckets.assert_called_once()
        self.assertEqual(client, mock_client)


class TestUploadToGCS(unittest.TestCase):
    """Test GCS upload functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, 'test.pdf')
        with open(self.test_file, 'w') as f:
            f.write('Test content')
    
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.test_dir)
    
    def test_upload_success(self):
        """Test successful file upload"""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        result = clickhouse_to_gcs.upload_to_gcs(
            mock_client, 'test-bucket', self.test_file, 'test/path.pdf'
        )
        
        self.assertTrue(result)
        mock_client.bucket.assert_called_once_with('test-bucket')
        mock_bucket.blob.assert_called_once_with('test/path.pdf')
        mock_blob.upload_from_filename.assert_called_once_with(self.test_file)
    
    def test_upload_file_not_exists(self):
        """Test upload with non-existent file"""
        mock_client = Mock()
        
        result = clickhouse_to_gcs.upload_to_gcs(
            mock_client, 'test-bucket', '/nonexistent/file.pdf', 'test/path.pdf'
        )
        
        self.assertFalse(result)
    
    def test_upload_gcs_error(self):
        """Test upload with GCS error"""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename.side_effect = Exception("Upload failed")
        
        result = clickhouse_to_gcs.upload_to_gcs(
            mock_client, 'test-bucket', self.test_file, 'test/path.pdf'
        )
        
        self.assertFalse(result)


class TestFindLocalFile(unittest.TestCase):
    """Test local file search functionality"""
    
    def setUp(self):
        """Set up test environment with directory structure"""
        self.test_dir = tempfile.mkdtemp()
        self.sub_dir = os.path.join(self.test_dir, 'subdirectory')
        os.makedirs(self.sub_dir)
        
        # Create test files
        self.test_file1 = os.path.join(self.test_dir, 'test1.pdf')
        self.test_file2 = os.path.join(self.sub_dir, 'test2.pdf')
        
        with open(self.test_file1, 'w') as f:
            f.write('Test content 1')
        with open(self.test_file2, 'w') as f:
            f.write('Test content 2')
    
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.test_dir)
    
    def test_find_file_in_root(self):
        """Test finding file in root directory"""
        result = clickhouse_to_gcs.find_local_file('test1.pdf', self.test_dir)
        self.assertEqual(result, os.path.abspath(self.test_file1))
    
    def test_find_file_in_subdirectory(self):
        """Test finding file in subdirectory"""
        result = clickhouse_to_gcs.find_local_file('test2.pdf', self.test_dir)
        self.assertEqual(result, os.path.abspath(self.test_file2))
    
    def test_find_file_not_found(self):
        """Test file not found"""
        result = clickhouse_to_gcs.find_local_file('nonexistent.pdf', self.test_dir)
        self.assertIsNone(result)
    
    def test_find_file_empty_filename(self):
        """Test with empty filename"""
        result = clickhouse_to_gcs.find_local_file('', self.test_dir)
        self.assertIsNone(result)


class TestFilterUnprocessedDocuments(unittest.TestCase):
    """Test document filtering functionality"""
    
    def test_filter_documents(self):
        """Test filtering unprocessed documents"""
        documents = [
            {'id_dokumen': 1, 'title': 'Doc 1'},
            {'id_dokumen': 2, 'title': 'Doc 2'},
            {'id_dokumen': 3, 'title': 'Doc 3'},
        ]
        processed_cache = {'1', '3'}  # Documents 1 and 3 already processed
        
        unprocessed, stats = clickhouse_to_gcs.filter_unprocessed_documents(
            documents, processed_cache
        )
        
        self.assertEqual(len(unprocessed), 1)
        self.assertEqual(unprocessed[0]['id_dokumen'], 2)
        self.assertEqual(stats['total_documents'], 3)
        self.assertEqual(stats['already_processed'], 2)
        self.assertEqual(stats['to_process'], 1)


class TestProcessingReport(unittest.TestCase):
    """Test processing report functionality"""
    
    def setUp(self):
        """Set up test environment"""
        self.test_dir = tempfile.mkdtemp()
        self.reports_dir = os.path.join(self.test_dir, 'reports')
        os.makedirs(self.reports_dir)
    
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.test_dir)
    
    @patch('clickhouse_to_gcs.os.path.dirname')
    def test_save_processing_report(self, mock_dirname):
        """Test saving processing report"""
        mock_dirname.return_value = self.test_dir
        
        stats = {
            'total_documents': 10,
            'processed': 5,
            'start_time': datetime.now(),
            'end_time': datetime.now(),
            'processed_files': [
                {'id_dokumen': 1, 'file_path': '/test/file1.pdf'},
                {'id_dokumen': 2, 'file_path': '/test/file2.pdf'}
            ]
        }
        
        result_path = clickhouse_to_gcs.save_processing_report(stats)
        
        expected_path = os.path.join(self.reports_dir, 'sync_report.json')
        self.assertEqual(result_path, expected_path)
        self.assertTrue(os.path.exists(expected_path))
        
        # Verify content
        with open(expected_path, 'r') as f:
            saved_data = json.load(f)
        
        self.assertEqual(saved_data['total_documents'], 10)
        self.assertEqual(saved_data['processed'], 5)
        self.assertEqual(len(saved_data['processed_files']), 2)
    
    @patch('clickhouse_to_gcs.os.path.dirname')
    def test_load_processed_cache(self, mock_dirname):
        """Test loading processed cache from report"""
        mock_dirname.return_value = self.test_dir
        
        # Create a test report
        report_data = {
            'processed_files': [
                {'id_dokumen': 1, 'file_path': '/test/file1.pdf'},
                {'id_dokumen': 2, 'file_path': '/test/file2.pdf'},
                {'id_dokumen': '3', 'file_path': '/test/file3.pdf'}  # String ID
            ]
        }
        
        report_path = os.path.join(self.reports_dir, 'sync_report.json')
        with open(report_path, 'w') as f:
            json.dump(report_data, f)
        
        cache = clickhouse_to_gcs.load_processed_cache()
        
        expected_cache = {'1', '2', '3'}
        self.assertEqual(cache, expected_cache)


class TestMainFunction(unittest.TestCase):
    """Test main function integration"""
    
    @patch('clickhouse_to_gcs.get_postgres_connection')
    @patch('clickhouse_to_gcs.get_gcs_client')
    @patch('clickhouse_to_gcs.get_documents_from_postgres')
    @patch('clickhouse_to_gcs.load_processed_cache')
    @patch('clickhouse_to_gcs.filter_unprocessed_documents')
    @patch('clickhouse_to_gcs.process_documents')
    @patch('clickhouse_to_gcs.save_processing_report')
    @patch('clickhouse_to_gcs.os.makedirs')
    def test_main_function_success(self, mock_makedirs, mock_save_report, 
                                 mock_process_docs, mock_filter_docs, 
                                 mock_load_cache, mock_get_docs, 
                                 mock_gcs_client, mock_pg_conn):
        """Test successful execution of main function"""
        # Setup mocks
        mock_conn = Mock()
        mock_pg_conn.return_value = mock_conn
        mock_client = Mock()
        mock_gcs_client.return_value = mock_client
        
        mock_documents = [{'id_dokumen': 1, 'file_path': '/test/file1.pdf'}]
        mock_get_docs.return_value = mock_documents
        
        mock_cache = set()
        mock_load_cache.return_value = mock_cache
        
        mock_filter_stats = {'already_processed': 0, 'to_process': 1}
        mock_filter_docs.return_value = (mock_documents, mock_filter_stats)
        
        mock_process_stats = {'processed': 1, 'processed_files': []}
        mock_process_docs.return_value = mock_process_stats
        
        mock_save_report.return_value = '/test/report.json'
        
        # Execute main function
        clickhouse_to_gcs.main()
        
        # Verify calls
        mock_pg_conn.assert_called_once()
        mock_gcs_client.assert_called_once()
        mock_get_docs.assert_called_once_with(mock_conn)
        mock_load_cache.assert_called_once()
        mock_filter_docs.assert_called_once_with(mock_documents, mock_cache)
        mock_process_docs.assert_called_once()
        mock_save_report.assert_called_once()
        mock_conn.close.assert_called_once()


class TestConfigModule(unittest.TestCase):
    """Test configuration module"""
    
    def test_postgres_config_values(self):
        """Test PostgreSQL configuration values"""
        self.assertEqual(config.POSTGRES_HOST, 'localhost')
        self.assertEqual(config.POSTGRES_PORT, 5432)
        self.assertEqual(config.POSTGRES_USER, 'pmendika')
        self.assertEqual(config.POSTGRES_PASSWORD, 'AppPm3n2025')
        self.assertEqual(config.POSTGRES_DB, 'pmen')
        self.assertEqual(config.POSTGRES_SCHEMA, 'transaksi')
        self.assertEqual(config.POSTGRES_VIEW, 'v_dokumen')


if __name__ == '__main__':
    # Create a test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestPostgreSQLConnection,
        TestQueryPostgreSQL,
        TestGetDocumentsFromPostgreSQL,
        TestGCSClient,
        TestUploadToGCS,
        TestFindLocalFile,
        TestFilterUnprocessedDocuments,
        TestProcessingReport,
        TestMainFunction,
        TestConfigModule
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    if result.wasSuccessful():
        print(f"\n✅ All {result.testsRun} tests passed!")
    else:
        print(f"\n❌ {len(result.failures)} failures, {len(result.errors)} errors out of {result.testsRun} tests")
