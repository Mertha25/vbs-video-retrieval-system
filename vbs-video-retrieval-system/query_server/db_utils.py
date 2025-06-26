import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG

def get_db_connection():
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")

def fetch_all_moments_with_colors_and_embeddings(conn):
    """Retrieve all moments with non-null color and embedding data."""
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("""
        SELECT 
            m.moment_id,
            m.video_id,
            m.frame_identifier,
            m.timestamp_seconds,
            m.keyframe_image_path,
            m.detected_object_names,
            m.extracted_search_words,
            m.average_color_rgb,
            m.clip_embedding,
            v.original_filename,
            v.duration_seconds
        FROM video_moments m
        JOIN videos v ON m.video_id = v.video_id
        WHERE m.average_color_rgb IS NOT NULL AND m.clip_embedding IS NOT NULL
        ORDER BY m.video_id, m.timestamp_seconds
    """)
    return cursor.fetchall()
