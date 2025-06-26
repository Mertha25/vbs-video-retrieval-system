# fix_video_filenames.py
# Fix the video filenames in the database to match actual files

import psycopg2
import logging

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'admin123',
    'database': 'videodb_creative_v2'
}

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"Database connection failed: {e}")
        return None

def fix_video_filenames():
    logger = setup_logging()
    logger.info("Fixing video filenames in database...")
    
    conn = get_db_connection()
    if not conn:
        logger.error("Failed to connect to database")
        return
    
    cursor = conn.cursor()
    try:
        # Update all videos to use the correct filename format
        cursor.execute("""
            UPDATE videos 
            SET compressed_filename = video_id || '.mp4'
            WHERE compressed_filename = 'compressed_for_web.mp4'
        """)
        
        updated_count = cursor.rowcount
        conn.commit()
        
        logger.info(f"Updated {updated_count} video records")
        
        # Verify the changes
        cursor.execute("SELECT video_id, compressed_filename FROM videos LIMIT 10")
        results = cursor.fetchall()
        
        logger.info("Sample updated records:")
        for video_id, filename in results:
            logger.info(f"  Video {video_id}: {filename}")
            
    except Exception as e:
        logger.error(f"Error updating filenames: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    fix_video_filenames() 