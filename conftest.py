"""
Pytest configuration and test fixtures for PostgreSQL GCS Synchronizer
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch
import psycopg2


@pytest.fixture
def temp_directory():
    """Create a temporary directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_documents():
    """Sample document data for testing"""
    return [
        {
            'id_base': 1,
            'id_relasi': 1,
            'id_dokumen': 123,
            'kode_jenis_file': 'PDF',
            'nomor': '001/2025',
            'tahun': 2025,
            'judul': 'Test Document 1',
            'file': 'test1.pdf',
            'file_path': '/documents/test1.pdf',
            'link': 'http://example.com/test1'
        },
        {
            'id_base': 2,
            'id_relasi': 2,
            'id_dokumen': 124,
            'kode_jenis_file': 'PDF',
            'nomor': '002/2025',
            'tahun': 2025,
            'judul': 'Test Document 2',
            'file': 'test2.pdf',
            'file_path': '/documents/test2.pdf',
            'link': 'http://example.com/test2'
        }
    ]


@pytest.fixture
def mock_postgres_connection():
    """Mock PostgreSQL connection"""
    with patch('clickhouse_to_gcs.psycopg2.connect') as mock_connect:
        mock_conn = Mock()
        mock_cursor = Mock()
        
        # Create a mock context manager for cursor
        mock_cursor_context = Mock()
        mock_cursor_context.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = Mock(return_value=None)
        mock_conn.cursor.return_value = mock_cursor_context
        
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def mock_gcs_client():
    """Mock GCS client"""
    with patch('clickhouse_to_gcs.storage.Client') as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def test_file(temp_directory):
    """Create a test file"""
    test_file_path = os.path.join(temp_directory, 'test.pdf')
    with open(test_file_path, 'w') as f:
        f.write('Test PDF content')
    return test_file_path


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for testing"""
    env_vars = {
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_USER': 'pmendika',
        'POSTGRES_PASSWORD': 'AppPm3n2025',
        'POSTGRES_DB': 'pmen',
        'POSTGRES_SCHEMA': 'transaksi',
        'POSTGRES_VIEW': 'v_dokumen',
        'GCS_BUCKET_NAME': 'test-bucket',
        'GCS_SERVICE_ACCOUNT_KEY': '/path/to/key.json',
        'WATCHED_FOLDER': '/test/documents'
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars
