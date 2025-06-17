#!/usr/bin/env python3
"""
Complete Information Retrieval Server for Video Retrieval System

"""

import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import logging

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

def parse_json_field(field_value):
    """Parse JSON field from database"""
    if field_value is None:
        return []
    if isinstance(field_value, str):
        try:
            return json.loads(field_value)
        except:
            return []
    return field_value

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'Complete IR Video Retrieval Server',
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
        
        # Get duration statistics
        cursor.execute("SELECT SUM(duration_seconds), AVG(duration_seconds) FROM videos WHERE duration_seconds > 0")
        duration_stats = cursor.fetchone()
        total_duration = duration_stats[0] or 0
        avg_duration = duration_stats[1] or 0
        
        stats = {
            'videos': video_count,
            'moments': moment_count,
            'analysis_status': status_breakdown,
            'total_duration_seconds': float(total_duration),
            'total_duration_hours': round(float(total_duration) / 3600, 2),
            'average_video_duration': round(float(avg_duration), 2),
            'last_updated': datetime.now().isoformat(),
            'database_type': 'PostgreSQL with IR capabilities'
        }
        
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/text', methods=['POST'])
def search_by_text():
    """Text search in video descriptions and extracted text"""
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
        
        # Search in video descriptions and extracted text from moments
        sql = """
        SELECT DISTINCT
            m.moment_id,
            m.video_id,
            m.frame_identifier,
            m.timestamp_seconds,
            m.keyframe_image_path,
            m.detected_object_names,
            m.extracted_search_words,
            m.average_color_rgb,
            v.original_filename,
            v.duration_seconds,
            v.fps,
            'text_match' as search_method
        FROM video_moments m
        JOIN videos v ON m.video_id = v.video_id
        WHERE 
            m.extracted_search_words ILIKE %s 
            OR v.original_filename ILIKE %s
        ORDER BY m.video_id, m.timestamp_seconds
        LIMIT %s
        """
        
        search_pattern = f'%{query}%'
        cursor.execute(sql, [search_pattern, search_pattern, limit])
        results = cursor.fetchall()
        
        # Format results
        formatted_results = []
        for row in results:
            result = dict(row)
            result['detected_object_names'] = parse_json_field(result['detected_object_names'])
            result['extracted_search_words'] = parse_json_field(result['extracted_search_words'])
            result['average_color_rgb'] = parse_json_field(result['average_color_rgb'])
            formatted_results.append(result)
        
        return jsonify({
            'results': formatted_results,
            'count': len(formatted_results),
            'query': query,
            'type': 'text_search'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/keywords', methods=['POST'])
def search_by_keywords():
    """Search by specific keywords in extracted text"""
    data = request.get_json()
    if not data or not data.get('keywords'):
        return jsonify({'error': 'Keywords array is required'}), 400
    
    keywords = data['keywords']
    limit = data.get('limit', 50)
    match_all = data.get('match_all', False)  # AND vs OR logic
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build keyword search conditions
        conditions = []
        params = []
        
        for keyword in keywords:
            conditions.append("m.extracted_search_words ILIKE %s")
            params.append(f'%{keyword.lower()}%')
        
        # Join with AND or OR
        operator = " AND " if match_all else " OR "
        where_clause = operator.join(conditions)
        
        sql = f"""
        SELECT 
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
        WHERE {where_clause}
        ORDER BY m.video_id, m.timestamp_seconds
        LIMIT %s
        """
        
        params.append(limit)
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        formatted_results = []
        for row in results:
            result = dict(row)
            result['detected_object_names'] = parse_json_field(result['detected_object_names'])
            result['extracted_search_words'] = parse_json_field(result['extracted_search_words'])
            formatted_results.append(result)
        
        return jsonify({
            'results': formatted_results,
            'count': len(formatted_results),
            'keywords': keywords,
            'match_all': match_all,
            'type': 'keyword_search'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/objects', methods=['POST'])
def search_by_objects():
    """Search by detected objects"""
    data = request.get_json()
    if not data or not data.get('objects'):
        return jsonify({'error': 'Objects array is required'}), 400
    
    objects = [obj.lower() for obj in data['objects']]
    limit = data.get('limit', 50)
    match_all = data.get('match_all', False)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build object search conditions
        conditions = []
        params = []
        
        for obj in objects:
            conditions.append("m.detected_object_names ILIKE %s")
            params.append(f'%{obj}%')
        
        operator = " AND " if match_all else " OR "
        where_clause = operator.join(conditions)
        
        sql = f"""
        SELECT 
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
        WHERE {where_clause}
        ORDER BY m.video_id, m.timestamp_seconds
        LIMIT %s
        """
        
        params.append(limit)
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        formatted_results = []
        for row in results:
            result = dict(row)
            result['detected_object_names'] = parse_json_field(result['detected_object_names'])
            result['extracted_search_words'] = parse_json_field(result['extracted_search_words'])
            formatted_results.append(result)
        
        return jsonify({
            'results': formatted_results,
            'count': len(formatted_results),
            'searched_objects': objects,
            'match_all': match_all,
            'type': 'object_search'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/temporal', methods=['POST'])
def search_by_time_range():
    """Search moments within specific time ranges"""
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request data required'}), 400
    
    video_id = data.get('video_id')  # Optional: search in specific video
    start_time = data.get('start_time', 0)  # Start time in seconds
    end_time = data.get('end_time')  # End time in seconds
    limit = data.get('limit', 50)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build temporal query
        where_conditions = ["m.timestamp_seconds >= %s"]
        params = [start_time]
        
        if end_time is not None:
            where_conditions.append("m.timestamp_seconds <= %s")
            params.append(end_time)
        
        if video_id:
            where_conditions.append("m.video_id = %s")
            params.append(video_id)
        
        where_clause = " AND ".join(where_conditions)
        
        sql = f"""
        SELECT 
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
        WHERE {where_clause}
        ORDER BY m.video_id, m.timestamp_seconds
        LIMIT %s
        """
        
        params.append(limit)
        cursor.execute(sql, params)
        results = cursor.fetchall()
        
        formatted_results = []
        for row in results:
            result = dict(row)
            result['detected_object_names'] = parse_json_field(result['detected_object_names'])
            result['extracted_search_words'] = parse_json_field(result['extracted_search_words'])
            formatted_results.append(result)
        
        return jsonify({
            'results': formatted_results,
            'count': len(formatted_results),
            'video_id': video_id,
            'start_time': start_time,
            'end_time': end_time,
            'type': 'temporal_search'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/video_segment', methods=['POST'])
def search_video_segment():
    """Search for a specific moment in a specific video at a specific time"""
    data = request.get_json()
    if not data or not data.get('video_id') or data.get('timestamp') is None:
        return jsonify({'error': 'video_id and timestamp are required'}), 400
    
    video_id = data['video_id']
    timestamp = float(data['timestamp'])
    tolerance = data.get('tolerance', 5.0)  # +/- seconds tolerance
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        sql = """
        SELECT 
            m.moment_id,
            m.video_id,
            m.frame_identifier,
            m.timestamp_seconds,
            m.keyframe_image_path,
            m.detected_object_names,
            m.extracted_search_words,
            m.average_color_rgb,
            v.original_filename,
            v.duration_seconds,
            v.fps,
            ABS(m.timestamp_seconds - %s) as time_distance
        FROM video_moments m
        JOIN videos v ON m.video_id = v.video_id
        WHERE m.video_id = %s
        AND ABS(m.timestamp_seconds - %s) <= %s
        ORDER BY time_distance
        LIMIT 10
        """
        
        cursor.execute(sql, [timestamp, video_id, timestamp, tolerance])
        results = cursor.fetchall()
        
        formatted_results = []
        for row in results:
            result = dict(row)
            result['detected_object_names'] = parse_json_field(result['detected_object_names'])
            result['extracted_search_words'] = parse_json_field(result['extracted_search_words'])
            result['average_color_rgb'] = parse_json_field(result['average_color_rgb'])
            result['time_distance'] = float(result['time_distance'])
            formatted_results.append(result)
        
        return jsonify({
            'results': formatted_results,
            'count': len(formatted_results),
            'video_id': video_id,
            'target_timestamp': timestamp,
            'tolerance': tolerance,
            'type': 'video_segment_search'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/videos', methods=['GET'])
def list_videos():
    """List all videos with filtering options"""
    page = int(request.args.get('page', 1))
    per_page = min(int(request.args.get('per_page', 50)), 200)
    min_duration = request.args.get('min_duration', type=float)
    max_duration = request.args.get('max_duration', type=float)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Build filtering conditions
        where_conditions = []
        params = []
        
        if min_duration is not None:
            where_conditions.append("v.duration_seconds >= %s")
            params.append(min_duration)
        
        if max_duration is not None:
            where_conditions.append("v.duration_seconds <= %s")
            params.append(max_duration)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Get total count
        count_sql = f"SELECT COUNT(*) FROM videos v {where_clause}"
        cursor.execute(count_sql, params)
        total_count = cursor.fetchone()[0]
        
        # Get videos with moment counts
        offset = (page - 1) * per_page
        sql = f"""
        SELECT 
            v.*,
            COUNT(m.moment_id) as moment_count
        FROM videos v
        LEFT JOIN video_moments m ON v.video_id = m.video_id
        {where_clause}
        GROUP BY v.video_id
        ORDER BY v.video_id
        LIMIT %s OFFSET %s
        """
        
        params.extend([per_page, offset])
        cursor.execute(sql, params)
        videos = cursor.fetchall()
        
        return jsonify({
            'videos': [dict(video) for video in videos],
            'count': len(videos),
            'total_count': total_count,
            'page': page,
            'per_page': per_page,
            'total_pages': (total_count + per_page - 1) // per_page
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/video/<video_id>', methods=['GET'])
def get_video_details(video_id):
    """Get detailed information about a specific video"""
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
        
        # Get moments for this video
        cursor.execute("""
            SELECT * FROM video_moments 
            WHERE video_id = %s 
            ORDER BY timestamp_seconds
        """, (video_id,))
        moments = cursor.fetchall()
        
        # Format moments
        formatted_moments = []
        for moment in moments:
            moment_dict = dict(moment)
            moment_dict['detected_object_names'] = parse_json_field(moment_dict['detected_object_names'])
            moment_dict['extracted_search_words'] = parse_json_field(moment_dict['extracted_search_words'])
            moment_dict['average_color_rgb'] = parse_json_field(moment_dict['average_color_rgb'])
            formatted_moments.append(moment_dict)
        
        return jsonify({
            'video': dict(video_info),
            'moments': formatted_moments,
            'moment_count': len(formatted_moments)
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
    print(" Starting Complete IR Video Retrieval Server...")
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print("Available endpoints:")
    print("   GET  /api/stats - System statistics")
    print("   GET  /api/videos - List videos (with filtering)")
    print("   POST /api/search/text - Text search")
    print("    POST /api/search/keywords - Keyword search")
    print("    POST /api/search/objects - Object search")
    print("    POST /api/search/temporal - Time range search")
    print("   POST /api/search/video_segment - Specific video moment")
    print("   GET  /api/video/{id} - Video details")
    print("\nAPI available at: http://localhost:5000")
    
    app.run(debug=True, host='0.0.0.0', port=5000)