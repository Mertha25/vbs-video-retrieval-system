#!/usr/bin/env python3
"""
Simple Query Server for Video Retrieval System

"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime

# Create Flask app
app = Flask(__name__)
CORS(app)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'admin123',
    'database': 'videodb_creative_v2'
}

def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'Video Retrieval Query Server (Simple)',
        'database': DB_CONFIG['database']
    })

@app.route('/api/stats', methods=['GET'])
def get_system_stats():
    """Get system statistics"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM videos")
        video_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM video_moments")
        moment_count = cursor.fetchone()[0]
        
        # Get analysis status breakdown
        cursor.execute("""
            SELECT analysis_status, COUNT(*) 
            FROM videos 
            GROUP BY analysis_status
        """)
        status_results = cursor.fetchall()
        status_breakdown = dict(status_results) if status_results else {}
        
        stats = {
            'videos': video_count,
            'moments': moment_count,
            'analysis_status': status_breakdown,
            'last_updated': datetime.now().isoformat(),
            'database_type': 'PostgreSQL (without pgvector)'
        }
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/videos', methods=['GET'])
def list_videos():
    """List all videos"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        sql = """
        SELECT 
            v.*,
            COUNT(m.moment_id) as moment_count
        FROM videos v
        LEFT JOIN video_moments m ON v.video_id = m.video_id
        GROUP BY v.video_id
        ORDER BY v.created_at DESC
        """
        
        cursor.execute(sql)
        videos = cursor.fetchall()
        
        return jsonify({
            'videos': [dict(video) for video in videos],
            'count': len(videos)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/text', methods=['POST'])
def search_by_text():
    """Simple text search"""
    data = request.get_json()
    if not data or not data.get('query'):
        return jsonify({'error': 'Query text is required'}), 400
    
    query = data['query']
    limit = data.get('limit', 50)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Simple text search in extracted words and object names
        sql = """
        SELECT DISTINCT
            m.moment_id,
            m.video_id,
            m.frame_identifier,
            m.timestamp_seconds,
            m.keyframe_image_path,
            m.detected_object_names,
            m.extracted_search_words,
            v.original_filename,
            v.duration_seconds
        FROM video_moments m
        JOIN videos v ON m.video_id = v.video_id
        WHERE 
            m.extracted_search_words ILIKE %s 
            OR m.detected_object_names ILIKE %s
            OR v.original_filename ILIKE %s
        ORDER BY m.video_id, m.timestamp_seconds
        LIMIT %s
        """
        
        search_pattern = f'%{query}%'
        cursor.execute(sql, [search_pattern] * 3 + [limit])
        results = cursor.fetchall()
        
        return jsonify({
            'results': [dict(row) for row in results],
            'count': len(results),
            'query': query,
            'type': 'text_search'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/video/<video_id>', methods=['GET'])
def get_video_details(video_id):
    """Get video details"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get video info
        cursor.execute("SELECT * FROM videos WHERE video_id = %s", (video_id,))
        video_info = cursor.fetchone()
        
        if not video_info:
            return jsonify({'error': 'Video not found'}), 404
        
        # Get moments
        cursor.execute("""
            SELECT * FROM video_moments 
            WHERE video_id = %s 
            ORDER BY timestamp_seconds
        """, (video_id,))
        moments = cursor.fetchall()
        
        return jsonify({
            'video': dict(video_info),
            'moments': [dict(moment) for moment in moments],
            'moment_count': len(moments)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/test/insert', methods=['POST'])
def test_insert():
    """Test endpoint to insert sample data"""
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor()
        
        # Insert test video
        video_id = f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        cursor.execute("""
            INSERT INTO videos (video_id, original_filename, duration_seconds, fps, analysis_status)
            VALUES (%s, %s, %s, %s, %s)
        """, (video_id, f"test_video_{video_id}.mp4", 120.0, 25.0, "completed"))
        
        # Insert test moment
        moment_id = f"{video_id}_frame_001"
        cursor.execute("""
            INSERT INTO video_moments (
                moment_id, video_id, frame_identifier, timestamp_seconds,
                detected_object_names, extracted_search_words
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            moment_id, video_id, "frame_001", 10.5,
            '["person", "car"]', '["test", "sample", "video"]'
        ))
        
        conn.commit()
        
        return jsonify({
            'success': True,
            'video_id': video_id,
            'moment_id': moment_id,
            'message': 'Test data inserted successfully'
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    print("Starting Video Retrieval Query Server...")
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print("API will be available at: http://localhost:5000")
    print("Health check: http://localhost:5000/health")
    print("Stats: http://localhost:5000/api/stats")
    
    app.run(debug=True, host='0.0.0.0', port=5000)