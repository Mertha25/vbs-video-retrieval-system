
from pathlib import Path

import psycopg2
from config import DB_CONFIG

def create_tables():
    """Create tables for videos and video moments in PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        schema_sql = """
        CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(255) PRIMARY KEY,
            original_filename VARCHAR(255) NOT NULL,
            compressed_filename VARCHAR(255),
            duration_seconds FLOAT NOT NULL,
            fps FLOAT NOT NULL,
            compressed_file_size_bytes BIGINT,
            processing_date_utc TIMESTAMP,
            scene_change_timestamps TEXT,
            keyframes_analyzed_count INTEGER DEFAULT 0,
            analysis_status VARCHAR(50) DEFAULT 'pending',
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS video_moments (
            moment_id VARCHAR(512) PRIMARY KEY,
            video_id VARCHAR(255) NOT NULL,
            frame_identifier VARCHAR(255) NOT NULL,
            timestamp_seconds FLOAT NOT NULL,
            keyframe_image_path VARCHAR(500),
            clip_embedding TEXT,
            detected_object_names TEXT,
            extracted_search_words TEXT,
            average_color_rgb TEXT,
            detailed_features TEXT,
            extraction_success BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(analysis_status);
        CREATE INDEX IF NOT EXISTS idx_moments_video_id ON video_moments(video_id);
        CREATE INDEX IF NOT EXISTS idx_moments_timestamp ON video_moments(timestamp_seconds);
        CREATE INDEX IF NOT EXISTS idx_moments_frame_id ON video_moments(frame_identifier);
        """

        cursor.execute(schema_sql)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM videos")
        video_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM video_moments")
        moment_count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            "videos": video_count,
            "moments": moment_count,
            "status": "success"
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

if __name__ == "__main__":
    print("=== Database Initialization ===")
    result = create_tables()
    if result["status"] == "success":
        print(f"✓ Videos: {result['videos']} | Moments: {result['moments']}")
        print("✓ Database is ready to use.")
    else:
        print(f"✗ Initialization failed: {result['message']}")
