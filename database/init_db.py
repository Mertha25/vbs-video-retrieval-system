#!/usr/bin/env python3
"""
Database initialization script adapted for your Video Retrieval System

"""

import os
import sys
import json
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path
from dotenv import load_dotenv
import logging
from datetime import datetime

# Import your existing settings
try:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))
    from settings import (
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
        FRAME_DATA_TABLE_NAME, DATASET_ROOT_DIR,
        EXTRACTED_FEATURES_JSON_FILENAME
    )
    HAS_BACKEND_SETTINGS = True
except ImportError as e:
    print(f"Warning: Could not import backend settings: {e}")
    print("Using environment variables only")
    HAS_BACKEND_SETTINGS = False

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration (use your settings.py values as defaults)
DB_CONFIG = {
    'host': os.getenv('DATABASE_HOST', 'localhost'),
    'database': os.getenv('DATABASE_NAME', 'videodb_creative_v2'),
    'user': os.getenv('DATABASE_USER', 'postgres'),
    'password': os.getenv('DATABASE_PASSWORD', 'admin123'),
    'port': os.getenv('DATABASE_PORT', '5432')
}

DATABASE_NAME = os.getenv('DATABASE_NAME', DB_NAME if HAS_BACKEND_SETTINGS else 'videodb_creative_v2')
TABLE_NAME = FRAME_DATA_TABLE_NAME if HAS_BACKEND_SETTINGS else 'video_moments'

def create_database():
    """Create the database if it doesn't exist"""
    try:
        # Connect to PostgreSQL server
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", 
            (DATABASE_NAME,)
        )
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f'CREATE DATABASE "{DATABASE_NAME}"')
            logger.info(f"Database '{DATABASE_NAME}' created successfully")
        else:
            logger.info(f"Database '{DATABASE_NAME}' already exists")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error creating database: {e}")
        return False

def execute_schema():
    """Execute the SQL schema adapted to your structure"""
    try:
        # Connect to the created database
        conn = psycopg2.connect(
            **DB_CONFIG,
            database=DATABASE_NAME
        )
        cursor = conn.cursor()
        
        # Read and execute schema file
        schema_path = Path(__file__).parent / 'schema.sql'
        
        if schema_path.exists():
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
                # Replace placeholder table name with your actual table name
                schema_sql = schema_sql.replace('video_moments', TABLE_NAME)
                cursor.execute(schema_sql)
        else:
            # If schema file doesn't exist, create basic structure
            logger.warning("Schema file not found, creating basic structure")
            cursor.execute(get_basic_schema_sql())
        
        conn.commit()
        logger.info("Database schema created successfully")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error executing schema: {e}")
        return False

def get_basic_schema_sql():
    """Return basic schema SQL if file is not available"""
    return f"""
    -- Enable pgvector extension
    CREATE EXTENSION IF NOT EXISTS vector;
    
    -- Videos table
    CREATE TABLE IF NOT EXISTS videos (
        video_id VARCHAR(255) PRIMARY KEY,
        original_filename VARCHAR(255) NOT NULL,
        compressed_filename VARCHAR(255),
        duration_seconds FLOAT NOT NULL,
        fps FLOAT NOT NULL,
        compressed_file_size_bytes BIGINT,
        processing_date_utc TIMESTAMP,
        scene_change_timestamps FLOAT[],
        keyframes_analyzed_count INTEGER DEFAULT 0,
        analysis_status VARCHAR(50) DEFAULT 'pending',
        error_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Video moments table (your keyframes)
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        moment_id VARCHAR(512) PRIMARY KEY,
        video_id VARCHAR(255) NOT NULL,
        frame_identifier VARCHAR(255) NOT NULL,
        timestamp_seconds FLOAT NOT NULL,
        keyframe_image_path VARCHAR(500),
        clip_embedding VECTOR(768),
        detected_object_names TEXT[],
        extracted_search_words TEXT[],
        average_color_rgb INTEGER[3],
        detailed_features JSONB,
        extraction_success BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
    );
    
    -- Basic indexes
    CREATE INDEX IF NOT EXISTS idx_moments_video_id ON {TABLE_NAME}(video_id);
    CREATE INDEX IF NOT EXISTS idx_moments_timestamp ON {TABLE_NAME}(timestamp_seconds);
    CREATE INDEX IF NOT EXISTS idx_moments_clip_embedding 
    ON {TABLE_NAME} USING ivfflat (clip_embedding vector_cosine_ops) WITH (lists = 100);
    """

def test_connection():
    """Test database connection and show stats"""
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            database=DATABASE_NAME
        )
        cursor = conn.cursor()
        
        # Test basic queries
        cursor.execute("SELECT COUNT(*) FROM videos")
        video_count = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        moment_count = cursor.fetchone()[0]
        
        # Check for embeddings
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE clip_embedding IS NOT NULL")
        embedding_count = cursor.fetchone()[0]
        
        logger.info("Database connection test successful:")
        logger.info(f"  - Videos: {video_count}")
        logger.info(f"  - Moments: {moment_count}")
        logger.info(f"  - CLIP embeddings: {embedding_count}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False

def check_pgvector():
    """Check if pgvector extension is available"""
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            database=DATABASE_NAME
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM pg_extension WHERE extname = 'vector'")
        result = cursor.fetchone()
        
        if result:
            logger.info("✓ pgvector extension is installed and ready")
        else:
            logger.warning("✗ pgvector extension is not installed")
            logger.info("Please install pgvector for vector similarity search")
            logger.info("Run: sudo apt install postgresql-15-pgvector")
        
        cursor.close()
        conn.close()
        return result is not None
        
    except Exception as e:
        logger.error(f"Error checking pgvector: {e}")
        return False

def import_sample_data():
    """Import a few sample records from your analysis reports"""
    if not HAS_BACKEND_SETTINGS:
        logger.warning("Backend settings not available, skipping sample data import")
        return False
    
    dataset_path = Path(DATASET_ROOT_DIR)
    if not dataset_path.exists():
        logger.warning(f"Dataset path not found: {dataset_path}")
        return False
    
    # Find first few video analysis reports
    sample_reports = []
    for video_dir in dataset_path.iterdir():
        if video_dir.is_dir():
            report_file = video_dir / EXTRACTED_FEATURES_JSON_FILENAME
            if report_file.exists():
                sample_reports.append(report_file)
                if len(sample_reports) >= 2:  # Limit to 2 for testing
                    break
    
    if not sample_reports:
        logger.warning("No analysis reports found for sample import")
        return False
    
    try:
        conn = psycopg2.connect(
            **DB_CONFIG,
            database=DATABASE_NAME
        )
        cursor = conn.cursor()
        
        imported_videos = 0
        imported_moments = 0
        
        for report_file in sample_reports:
            try:
                with open(report_file, 'r', encoding='utf-8') as f:
                    report_data = json.load(f)
                
                # Insert video record
                video_sql = """
                INSERT INTO videos (
                    video_id, original_filename, compressed_filename, 
                    duration_seconds, fps, compressed_file_size_bytes,
                    processing_date_utc, scene_change_timestamps,
                    keyframes_analyzed_count, analysis_status, error_message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (video_id) DO NOTHING
                """
                
                # Parse processing date
                processing_date = None
                if report_data.get('processing_date_utc'):
                    try:
                        processing_date = datetime.fromisoformat(
                            report_data['processing_date_utc'].replace('Z', '+00:00')
                        )
                    except:
                        pass
                
                cursor.execute(video_sql, (
                    report_data['video_id'],
                    report_data.get('original_filename'),
                    report_data.get('compressed_filename'),
                    report_data.get('duration_seconds', 0),
                    report_data.get('fps', 0),
                    report_data.get('compressed_file_size_bytes', 0),
                    processing_date,
                    report_data.get('scene_change_timestamps', []),
                    report_data.get('keyframes_analyzed_count', 0),
                    report_data.get('analysis_status', 'unknown'),
                    report_data.get('error_message')
                ))
                
                imported_videos += 1
                
                # Insert first few moments for testing
                analyzed_keyframes = report_data.get('analyzed_keyframes', [])
                for moment_data in analyzed_keyframes[:3]:  # Limit to 3 moments per video
                    moment_sql = f"""
                    INSERT INTO {TABLE_NAME} (
                        moment_id, video_id, frame_identifier, timestamp_seconds,
                        keyframe_image_path, clip_embedding, detected_object_names,
                        extracted_search_words, average_color_rgb, detailed_features
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (moment_id) DO NOTHING
                    """
                    
                    cursor.execute(moment_sql, (
                        moment_data['moment_id'],
                        moment_data['video_id'],
                        moment_data['frame_identifier'],
                        moment_data['timestamp_seconds'],
                        moment_data.get('keyframe_image_path'),
                        moment_data.get('clip_embedding'),
                        moment_data.get('detected_object_names', []),
                        moment_data.get('extracted_search_words', []),
                        moment_data.get('average_color_rgb', [0, 0, 0]),
                        json.dumps(moment_data.get('detailed_features', {}))
                    ))
                    
                    imported_moments += 1
                
            except Exception as e:
                logger.warning(f"Error importing {report_file}: {e}")
                continue
        
        conn.commit()
        
        logger.info(f"Sample data imported:")
        logger.info(f"  - Videos: {imported_videos}")
        logger.info(f"  - Moments: {imported_moments}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error importing sample data: {e}")
        return False

def validate_configuration():
    """Validate that the configuration matches your backend"""
    logger.info("=== Configuration Validation ===")
    
    if HAS_BACKEND_SETTINGS:
        logger.info(f"✓ Backend settings imported successfully")
        logger.info(f"  - Database: {DATABASE_NAME}")
        logger.info(f"  - Table: {TABLE_NAME}")
        logger.info(f"  - Dataset: {DATASET_ROOT_DIR}")
    else:
        logger.warning("✗ Backend settings not available")
        logger.info("  Make sure your backend folder is properly set up")
    
    # Check dataset path
    if HAS_BACKEND_SETTINGS:
        dataset_path = Path(DATASET_ROOT_DIR)
        if dataset_path.exists():
            video_dirs = [d for d in dataset_path.iterdir() if d.is_dir()]
            logger.info(f"✓ Dataset path exists with {len(video_dirs)} video directories")
        else:
            logger.warning(f"✗ Dataset path not found: {DATASET_ROOT_DIR}")
    
    logger.info("================================")

def main():
    """Main initialization function adapted to your system"""
    logger.info("=== Video Retrieval Database Initialization ===")
    logger.info("Adapted for your existing backend structure")
    
    # Validate configuration
    validate_configuration()
    
    # Check environment variables
    if DB_CONFIG['password'] in ['admin', 'your_password_here']:
        logger.warning("Using default database password. Consider changing it.")
    
    # Step 1: Create database
    logger.info("1. Creating database...")
    if not create_database():
        sys.exit(1)
    
    # Step 2: Execute schema
    logger.info("2. Creating schema...")
    if not execute_schema():
        sys.exit(1)
    
    # Step 3: Check pgvector
    logger.info("3. Checking pgvector extension...")
    pgvector_available = check_pgvector()
    
    # Step 4: Test connection
    logger.info("4. Testing connection...")
    if not test_connection():
        sys.exit(1)
    
    # Step 5: Import sample data (optional)
    if HAS_BACKEND_SETTINGS:
        import_sample = input("Import sample data from analysis reports? (y/N): ").lower() == 'y'
        if import_sample:
            logger.info("5. Importing sample data...")
            import_sample_data()
    
    logger.info("=== Database initialization completed successfully ===")
    logger.info(f"Database '{DATABASE_NAME}' is ready to use")
    logger.info("")
    logger.info("Next steps:")
    logger.info("1. Start the query server: python query_server/app.py")
    logger.info("2. Import your analysis reports: POST /api/import/batch")
    if not pgvector_available:
        logger.info("3. Install pgvector for similarity search functionality")

if __name__ == "__main__":
    main()