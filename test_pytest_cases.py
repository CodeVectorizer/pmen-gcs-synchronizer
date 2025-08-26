"""
Additional pytest-based test cases for better test organization
"""
import pytest
import os
import json
from unittest.mock import Mock, patch, call
import psycopg2

import clickhouse_to_gcs


class TestPostgreSQLIntegration:
    """Test PostgreSQL integration with pytest"""
    
    def test_postgres_connection_with_correct_params(self, mock_postgres_connection):
        """Test PostgreSQL connection with correct parameters"""
        mock_conn, mock_cursor = mock_postgres_connection
        
        result = clickhouse_to_gcs.get_postgres_connection()
        
        assert result == mock_conn
        mock_cursor.execute.assert_called_once_with('SELECT 1')
    
    def test_postgres_query_execution(self, mock_postgres_connection):
        """Test PostgreSQL query execution"""
        mock_conn, mock_cursor = mock_postgres_connection
        
        # Setup mock cursor to return dict-like objects
        mock_cursor.fetchall.return_value = [
            {'id': 1, 'name': 'Test Doc'},
            {'id': 2, 'name': 'Another Doc'}
        ]
        
        query = "SELECT * FROM test_table"
        results = clickhouse_to_gcs.query_postgres(mock_conn, query)
        
        assert len(results) == 2
        assert results[0]['id'] == 1
        assert results[1]['name'] == 'Another Doc'
    
    def test_get_documents_query_format(self, mock_postgres_connection):
        """Test that get_documents_from_postgres formats query correctly"""
        mock_conn, mock_cursor = mock_postgres_connection
        mock_cursor.fetchall.return_value = []
        
        with patch('clickhouse_to_gcs.query_postgres') as mock_query:
            clickhouse_to_gcs.get_documents_from_postgres(mock_conn, limit=500)
            
            # Check that query contains the correct schema and view
            call_args = mock_query.call_args
            query = call_args[0][1]
            assert 'transaksi.v_dokumen' in query
            assert 'ORDER BY id_dokumen DESC' in query
            assert 'LIMIT %(limit)s' in query


class TestFileHandling:
    """Test file handling functionality"""
    
    def test_find_local_file_success(self, temp_directory):
        """Test finding an existing local file"""
        # Create test file structure
        sub_dir = os.path.join(temp_directory, 'subdirectory')
        os.makedirs(sub_dir)
        test_file = os.path.join(sub_dir, 'document.pdf')
        with open(test_file, 'w') as f:
            f.write('test content')
        
        result = clickhouse_to_gcs.find_local_file('document.pdf', temp_directory)
        
        assert result is not None
        assert result.endswith('document.pdf')
        assert os.path.exists(result)
    
    def test_find_local_file_not_found(self, temp_directory):
        """Test searching for non-existent file"""
        result = clickhouse_to_gcs.find_local_file('nonexistent.pdf', temp_directory)
        assert result is None
    
    def test_upload_to_gcs_success(self, test_file, mock_gcs_client):
        """Test successful GCS upload"""
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_gcs_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        result = clickhouse_to_gcs.upload_to_gcs(
            mock_gcs_client, 'test-bucket', test_file, 'test/path.pdf'
        )
        
        assert result is True
        mock_gcs_client.bucket.assert_called_once_with('test-bucket')
        mock_bucket.blob.assert_called_once_with('test/path.pdf')
        mock_blob.upload_from_filename.assert_called_once_with(test_file)
    
    def test_upload_to_gcs_file_not_exists(self, mock_gcs_client):
        """Test GCS upload with non-existent file"""
        result = clickhouse_to_gcs.upload_to_gcs(
            mock_gcs_client, 'test-bucket', '/nonexistent/file.pdf', 'test/path.pdf'
        )
        
        assert result is False


class TestDocumentProcessing:
    """Test document processing workflow"""
    
    def test_filter_unprocessed_documents(self, sample_documents):
        """Test filtering out already processed documents"""
        processed_cache = {'123'}  # Document 123 already processed
        
        unprocessed, stats = clickhouse_to_gcs.filter_unprocessed_documents(
            sample_documents, processed_cache
        )
        
        assert len(unprocessed) == 1
        assert unprocessed[0]['id_dokumen'] == 124
        assert stats['total_documents'] == 2
        assert stats['already_processed'] == 1
        assert stats['to_process'] == 1
    
    def test_filter_all_processed(self, sample_documents):
        """Test when all documents are already processed"""
        processed_cache = {'123', '124'}
        
        unprocessed, stats = clickhouse_to_gcs.filter_unprocessed_documents(
            sample_documents, processed_cache
        )
        
        assert len(unprocessed) == 0
        assert stats['already_processed'] == 2
        assert stats['to_process'] == 0
    
    def test_load_processed_cache_empty(self, temp_directory):
        """Test loading cache when no reports exist"""
        with patch('clickhouse_to_gcs.os.path.dirname', return_value=temp_directory):
            cache = clickhouse_to_gcs.load_processed_cache()
            assert cache == set()
    
    def test_load_processed_cache_with_data(self, temp_directory):
        """Test loading cache from existing report"""
        # Create reports directory and file
        reports_dir = os.path.join(temp_directory, 'reports')
        os.makedirs(reports_dir)
        
        report_data = {
            'processed_files': [
                {'id_dokumen': 123, 'file_path': '/test/file1.pdf'},
                {'id_dokumen': '124', 'file_path': '/test/file2.pdf'}
            ]
        }
        
        report_file = os.path.join(reports_dir, 'sync_report.json')
        with open(report_file, 'w') as f:
            json.dump(report_data, f)
        
        with patch('clickhouse_to_gcs.os.path.dirname', return_value=temp_directory):
            cache = clickhouse_to_gcs.load_processed_cache()
            
        assert cache == {'123', '124'}


class TestReportGeneration:
    """Test processing report functionality"""
    
    def test_save_processing_report(self, temp_directory):
        """Test saving processing report"""
        stats = {
            'total_documents': 5,
            'processed': 3,
            'processed_files': [
                {'id_dokumen': 1, 'file_path': '/test/file1.pdf'},
                {'id_dokumen': 2, 'file_path': '/test/file2.pdf'}
            ]
        }
        
        with patch('clickhouse_to_gcs.os.path.dirname', return_value=temp_directory):
            result_path = clickhouse_to_gcs.save_processing_report(stats)
        
        expected_path = os.path.join(temp_directory, 'reports', 'sync_report.json')
        assert result_path == expected_path
        assert os.path.exists(expected_path)
        
        # Verify content
        with open(expected_path, 'r') as f:
            saved_data = json.load(f)
        
        assert saved_data['total_documents'] == 5
        assert saved_data['processed'] == 3
        assert len(saved_data['processed_files']) == 2


class TestMainWorkflow:
    """Test main workflow integration"""
    
    @patch('clickhouse_to_gcs.save_processing_report')
    @patch('clickhouse_to_gcs.process_documents')
    @patch('clickhouse_to_gcs.filter_unprocessed_documents')
    @patch('clickhouse_to_gcs.load_processed_cache')
    @patch('clickhouse_to_gcs.get_documents_from_postgres')
    @patch('clickhouse_to_gcs.get_gcs_client')
    @patch('clickhouse_to_gcs.get_postgres_connection')
    @patch('clickhouse_to_gcs.os.makedirs')
    def test_main_workflow_success(self, mock_makedirs, mock_pg_conn, mock_gcs_client,
                                 mock_get_docs, mock_load_cache, mock_filter_docs,
                                 mock_process_docs, mock_save_report, sample_documents):
        """Test successful main workflow execution"""
        # Setup mocks
        mock_conn = Mock()
        mock_pg_conn.return_value = mock_conn
        
        mock_client = Mock()
        mock_gcs_client.return_value = mock_client
        
        mock_get_docs.return_value = sample_documents
        mock_load_cache.return_value = set()
        
        mock_filter_stats = {'already_processed': 0, 'to_process': 2}
        mock_filter_docs.return_value = (sample_documents, mock_filter_stats)
        
        mock_process_stats = {'processed': 2, 'processed_files': []}
        mock_process_docs.return_value = mock_process_stats
        
        mock_save_report.return_value = '/test/report.json'
        
        # Execute
        clickhouse_to_gcs.main()
        
        # Verify workflow
        mock_pg_conn.assert_called_once()
        mock_gcs_client.assert_called_once()
        mock_get_docs.assert_called_once_with(mock_conn)
        mock_load_cache.assert_called_once()
        mock_filter_docs.assert_called_once_with(sample_documents, set())
        mock_process_docs.assert_called_once()
        mock_save_report.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('clickhouse_to_gcs.get_documents_from_postgres')
    @patch('clickhouse_to_gcs.get_gcs_client')
    @patch('clickhouse_to_gcs.get_postgres_connection')
    @patch('clickhouse_to_gcs.os.makedirs')
    def test_main_no_documents(self, mock_makedirs, mock_pg_conn, mock_gcs_client, mock_get_docs):
        """Test main workflow when no documents found"""
        mock_conn = Mock()
        mock_pg_conn.return_value = mock_conn
        
        mock_client = Mock()
        mock_gcs_client.return_value = mock_client
        
        mock_get_docs.return_value = []  # No documents
        
        # Should not raise exception, just return early
        clickhouse_to_gcs.main()
        
        mock_conn.close.assert_called_once()


class TestErrorHandling:
    """Test error handling scenarios"""
    
    def test_postgres_connection_error(self):
        """Test PostgreSQL connection error handling"""
        with patch('clickhouse_to_gcs.psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.Error("Connection failed")
            
            with pytest.raises(psycopg2.Error):
                clickhouse_to_gcs.get_postgres_connection()
    
    def test_gcs_client_error(self):
        """Test GCS client creation error"""
        with patch('clickhouse_to_gcs.service_account.Credentials.from_service_account_file') as mock_creds:
            mock_creds.side_effect = Exception("Invalid credentials")
            
            with pytest.raises(Exception):
                clickhouse_to_gcs.get_gcs_client()
    
    def test_upload_gcs_error(self, test_file):
        """Test GCS upload error handling"""
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename.side_effect = Exception("Upload failed")
        
        result = clickhouse_to_gcs.upload_to_gcs(
            mock_client, 'test-bucket', test_file, 'test/path.pdf'
        )
        
        assert result is False
