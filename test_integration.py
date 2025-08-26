"""
Integration test for PostgreSQL connection
This test attempts to connect to the actual PostgreSQL database
Only run this when you have a PostgreSQL instance running with the configured credentials
"""
import psycopg2
from config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_SCHEMA, POSTGRES_VIEW


def test_postgres_connection():
    """Test actual PostgreSQL connection"""
    try:
        print(f"🔌 Attempting to connect to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}")
        print(f"📊 Database: {POSTGRES_DB}, User: {POSTGRES_USER}")
        
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )
        
        print("✅ PostgreSQL connection successful!")
        
        # Test basic query
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"📋 PostgreSQL version: {version}")
        
        # Test schema and view access
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT table_name 
                FROM information_schema.views 
                WHERE table_schema = '{POSTGRES_SCHEMA}' 
                AND table_name = '{POSTGRES_VIEW}';
            """)
            result = cur.fetchone()
            
            if result:
                print(f"✅ View {POSTGRES_SCHEMA}.{POSTGRES_VIEW} exists and is accessible")
                
                # Test querying the view
                cur.execute(f"SELECT COUNT(*) FROM {POSTGRES_SCHEMA}.{POSTGRES_VIEW};")
                count = cur.fetchone()[0]
                print(f"📊 Total documents in view: {count}")
                
                # Test getting sample data
                cur.execute(f"SELECT * FROM {POSTGRES_SCHEMA}.{POSTGRES_VIEW} LIMIT 1;")
                sample = cur.fetchone()
                if sample:
                    print(f"📄 Sample document ID: {sample[2] if len(sample) > 2 else 'N/A'}")
                
            else:
                print(f"❌ View {POSTGRES_SCHEMA}.{POSTGRES_VIEW} not found or not accessible")
        
        conn.close()
        print("🔌 Connection closed successfully")
        
    except psycopg2.Error as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False
    
    return True


def test_view_structure():
    """Test the structure of the view to ensure it has expected columns"""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB
        )
        
        expected_columns = [
            'id_base', 'id_relasi', 'id_dokumen', 'kode_jenis_file', 
            'nomor', 'tahun', 'judul', 'file', 'file_path', 'link'
        ]
        
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{POSTGRES_SCHEMA}' 
                AND table_name = '{POSTGRES_VIEW}'
                ORDER BY ordinal_position;
            """)
            
            actual_columns = [row[0] for row in cur.fetchall()]
            print(f"📋 View columns: {actual_columns}")
            
            missing_columns = [col for col in expected_columns if col not in actual_columns]
            if missing_columns:
                print(f"⚠️  Missing expected columns: {missing_columns}")
            else:
                print("✅ All expected columns are present")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking view structure: {e}")


if __name__ == "__main__":
    print("🧪 PostgreSQL Integration Test")
    print("=" * 40)
    
    if test_postgres_connection():
        print("\n🔍 Testing view structure...")
        test_view_structure()
        print("\n🎉 Integration test completed successfully!")
    else:
        print("\n💥 Integration test failed!")
        print("\n💡 Make sure:")
        print("   - PostgreSQL server is running")
        print("   - Database 'pmen' exists")
        print("   - User 'pmendika' has access")
        print("   - Schema 'transaksi' exists")
        print("   - View 'v_dokumen' exists in the schema")
