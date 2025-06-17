#!/usr/bin/env python3
"""
Simple database initialization script
File: database/simple_init.py
"""

import psycopg2
from pathlib import Path

# Configuration directe (qui fonctionne)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'admin123',
    'database': 'videodb_creative_v2'
}

def create_tables():
    """Create tables directly"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("✓ Connected to PostgreSQL")
        
        # Simple schema without pgvector
        schema_sql = """
        -- Videos table
        CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(255) PRIMARY KEY,
            original_filename VARCHAR(255) NOT NULL,
            compressed_filename VARCHAR(255),
            duration_seconds FLOAT NOT NULL,
            fps FLOAT NOT NULL,
            compressed_file_size_bytes BIGINT,
            processing_date_utc TIMESTAMP,
            scene_change_timestamps TEXT, -- JSON array as text
            keyframes_analyzed_count INTEGER DEFAULT 0,
            analysis_status VARCHAR(50) DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Video moments table (keyframes) 
        CREATE TABLE IF NOT EXISTS video_moments (
            moment_id VARCHAR(512) PRIMARY KEY,
            video_id VARCHAR(255) NOT NULL,
            frame_identifier VARCHAR(255) NOT NULL,
            timestamp_seconds FLOAT NOT NULL,
            keyframe_image_path VARCHAR(500),
            clip_embedding TEXT, -- JSON array as text instead of VECTOR
            detected_object_names TEXT, -- JSON array as text
            extracted_search_words TEXT, -- JSON array as text
            average_color_rgb TEXT, -- JSON array as text
            detailed_features TEXT, -- JSON as text
            extraction_success BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
        );

        -- Basic indexes
        CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(analysis_status);
        CREATE INDEX IF NOT EXISTS idx_moments_video_id ON video_moments(video_id);
        CREATE INDEX IF NOT EXISTS idx_moments_timestamp ON video_moments(timestamp_seconds);
        CREATE INDEX IF NOT EXISTS idx_moments_frame_id ON video_moments(frame_identifier);
        """
        
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✓ Tables created successfully")
        
        # Test the tables
        cursor.execute("SELECT COUNT(*) FROM videos")
        video_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM video_moments")
        moment_count = cursor.fetchone()[0]
        
        print(f"✓ Database test successful:")
        print(f"  - Videos: {video_count}")
        print(f"  - Moments: {moment_count}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return False

if __name__ == "__main__":
    print("=== Simple Database Initialization ===")
    print(f"Connecting to: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"User: {DB_CONFIG['user']}")
    print()
    
    if create_tables():
        print("\n=== Initialization completed successfully ===")
        print("✓ Database is ready to use")
        print("✓ You can now start the query server")
    else:
        print("\n✗ Initialization failed")