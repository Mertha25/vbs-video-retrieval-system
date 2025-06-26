# server.py

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import psycopg2.extras
from datetime import datetime
import os
import json

from db_utils import get_db_connection, fetch_all_moments_with_colors_and_embeddings
from utils_server import color_distance, cosine_similarity_score, parse_json_field, extract_keywords_from_sentence

# Import DRES client
try:
    from dres_client import get_dres_client, submit_to_dres, test_dres_connection
    DRES_AVAILABLE = True
except ImportError:
    DRES_AVAILABLE = False
    print("Warning: DRES client not available. VBS competition features will be disabled.")

app = Flask(__name__)
CORS(app)

# Video dataset path - this should match the path in docker-compose
VIDEO_DATASET_PATH = os.environ.get('VIDEO_DATASET_PATH', 'Dataset')
API_URL_BASE = os.environ.get('API_URL_BASE', 'http://localhost:5000')

@app.route('/')
def home():
    return 'Welcome to the VBS Video Retrieval System!'

@app.route('/health')
def health():
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'IR Video Retrieval API',
        'dres_available': DRES_AVAILABLE
    })

@app.route('/api/videos/<video_id>/<filename>')
def serve_video(video_id, filename):
    """Serve video files from the dataset directory."""
    video_dir = os.path.join(VIDEO_DATASET_PATH, "V3C1-200", video_id)
    if not os.path.isdir(video_dir):
        return jsonify({'error': f'Video directory for {video_id} not found'}), 404
    
    video_path = os.path.join(video_dir, filename)
    if not os.path.exists(video_path):
        return jsonify({
            'error': 'Video file not found',
            'message': f'Video {filename} not available in {video_dir}.',
            'video_id': video_id,
        }), 404
    
    # Add proper headers for video streaming
    response = send_from_directory(video_dir, filename)
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    response.headers['Accept-Ranges'] = 'bytes'
    return response

def transform_result(row):
    """Transforms a database row to the format expected by the frontend."""
    video_id = row.get('video_id')
    # Always use the video_id.mp4 format since that's how the files are actually named
    filename = f"{video_id}.mp4"

    transformed = {
        'id': row.get('moment_id'),
        'title': f"Video {video_id} at {row.get('timestamp_seconds', 0):.1f}s",
        'video_path': f"{API_URL_BASE}/api/videos/{video_id}/{filename}",
        'duration': row.get('duration_seconds', 0),
        'score': row.get('similarity_score') or row.get('score') or 0.0,
        'timestamp': row.get('timestamp_seconds', 0.0),
        'objects': parse_json_field(row.get('detected_object_names', '[]')),
        'text': parse_json_field(row.get('extracted_search_words', '[]')),
        'dominant_colors': []
    }
    if row.get('average_color_rgb'):
        transformed['dominant_colors'].append(parse_json_field(row.get('average_color_rgb')))
    
    return transformed

@app.route('/api/stats', methods=['GET'])
def get_system_stats():
    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("SELECT COUNT(*) AS count FROM videos")
        video_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) AS count FROM video_moments")
        moment_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) AS count FROM video_moments WHERE average_color_rgb IS NOT NULL")
        color_count = cursor.fetchone()['count']

        cursor.execute("SELECT COUNT(*) AS count FROM video_moments WHERE clip_embedding IS NOT NULL")
        vector_count = cursor.fetchone()['count']

        cursor.execute("SELECT SUM(duration_seconds) AS total, AVG(duration_seconds) AS avg FROM videos")
        result = cursor.fetchone()
        total_duration = result['total']
        avg_duration = result['avg']

        return jsonify({
            'videos': video_count,
            'moments': moment_count,
            'moments_with_color': color_count,
            'moments_with_embedding': vector_count,
            'total_duration_seconds': float(total_duration or 0),
            'average_duration_seconds': float(avg_duration or 0),
            'last_updated': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/keywords', methods=['POST'])
def search_by_keywords():
    data = request.get_json()
    keywords = data.get('keywords', [])
    match_all = data.get('match_all', False)
    limit = data.get('limit', 50)

    if not keywords:
        return jsonify({'error': 'keywords array is required'}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where_clauses = []
        params = []

        for word in keywords:
            where_clauses.append("array_to_string(m.extracted_search_words, ' ') ILIKE %s")
            params.append(f'%{word}%')

        clause = " AND ".join(where_clauses) if match_all else " OR ".join(where_clauses)
        sql = f"""
            SELECT m.*, v.original_filename FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE {clause}
            ORDER BY m.timestamp_seconds
            LIMIT %s
        """
        params.append(limit)
        cursor.execute(sql, params)
        results = cursor.fetchall()

        formatted = [transform_result(row) for row in results]

        return jsonify({'results': formatted, 'count': len(formatted)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/text', methods=['POST'])
def search_by_text():
    data = request.get_json()
    query = data.get('query')
    limit = data.get('limit', 50)

    if not query:
        return jsonify({'error': 'Missing query'}), 400

    # Extract keywords from the user's sentence
    keywords = extract_keywords_from_sentence(query)
    
    if not keywords:
        return jsonify({'error': 'No meaningful keywords found in query'}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Build SQL query to search for any of the extracted keywords
        where_clauses = []
        params = []
        
        for keyword in keywords:
            # Search in extracted_search_words, detected_object_names, and filename
            where_clauses.append("""
                (array_to_string(m.extracted_search_words, ' ') ILIKE %s
                 OR array_to_string(m.detected_object_names, ' ') ILIKE %s
                 OR v.original_filename ILIKE %s)
            """)
            params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])
        
        # Use OR to match any of the keywords and order by pre-computed text relevance score
        sql = f"""
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds,
                   m.text_relevance_score as score
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE {' OR '.join(where_clauses)}
            ORDER BY m.text_relevance_score DESC, m.timestamp_seconds
            LIMIT %s
        """
        params.append(limit)
        
        cursor.execute(sql, params)
        results = cursor.fetchall()

        formatted = [transform_result(row) for row in results]

        return jsonify({
            'results': formatted, 
            'count': len(formatted),
            'extracted_keywords': keywords,
            'query': query,
            'score_type': 'pre_computed_text_relevance'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/color', methods=['POST'])
def search_by_color():
    data = request.get_json()
    color = data.get('color')
    threshold = data.get('threshold', 50)
    limit = data.get('limit', 50)

    if not color or len(color) != 3:
        return jsonify({'error': 'Invalid RGB color'}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.average_color_rgb IS NOT NULL
        """)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            moment_color = parse_json_field(row['average_color_rgb'])
            distance = color_distance(color, moment_color)
            if distance <= threshold:
                row['score'] = 1.0 - (distance / 100.0) # Convert distance to similarity
                results.append(transform_result(row))

        results.sort(key=lambda r: -r['score'])
        return jsonify({'results': results[:limit], 'count': len(results)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/vector', methods=['POST'])
def search_by_vector():
    data = request.get_json()
    embedding = data.get('embedding')
    threshold = data.get('threshold', 0.7)
    limit = data.get('limit', 50)

    if not embedding:
        return jsonify({'error': 'Missing embedding'}), 400

    conn = get_db_connection()
    try:
        rows = fetch_all_moments_with_colors_and_embeddings(conn)
        results = []

        for row in rows:
            moment_embedding = parse_json_field(row['clip_embedding'])
            score = cosine_similarity_score(embedding, moment_embedding)
            if score >= threshold:
                row['similarity_score'] = score
                results.append(transform_result(row))

        results.sort(key=lambda r: -r['score'])
        return jsonify({'results': results[:limit], 'count': len(results)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/multimodal', methods=['POST'])
def multimodal_search():
    data = request.get_json()
    text = data.get('text')
    color = data.get('color')
    objects = data.get('objects', [])
    words = data.get('words')
    start_time = data.get('start_time')
    end_time = data.get('end_time')
    embedding = data.get('embedding')
    limit = int(data.get('limit', 50))
    color_threshold = int(data.get('color_threshold', 50))
    sim_threshold = float(data.get('similarity_threshold', 0.7))

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where_clauses = []
        params = []
        keywords = []

        if text:
            # Extract keywords from the text input
            keywords = extract_keywords_from_sentence(text)
            if keywords:
                # Build keyword search clauses
                keyword_clauses = []
                for keyword in keywords:
                    keyword_clauses.append("""
                        (array_to_string(m.extracted_search_words, ' ') ILIKE %s
                         OR array_to_string(m.detected_object_names, ' ') ILIKE %s
                         OR v.original_filename ILIKE %s)
                    """)
                    params.extend([f'%{keyword}%', f'%{keyword}%', f'%{keyword}%'])
                
                # Use OR to match any of the keywords
                where_clauses.append(f"({' OR '.join(keyword_clauses)})")
        
        if objects:
            for obj in objects:
                where_clauses.append("m.detected_object_names @> ARRAY[%s]")
                params.append(obj)
        if words:
            where_clauses.append("m.extracted_search_words @> ARRAY[%s]")
            params.append(words)
        if start_time is not None and end_time is not None:
            where_clauses.append("m.timestamp_seconds BETWEEN %s AND %s")
            params.extend([start_time, end_time])

        sql = """
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
        """
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " LIMIT %s"
        params.append(limit)

        cursor.execute(sql, params)
        rows = cursor.fetchall()

        # Now filter in Python for color and embedding if needed
        results = []
        for row in rows:
            ok = True
            # Score for text-based search
            if keywords:
                match_count = 0
                total_keywords = len(keywords)
                for kw in keywords:
                    if 'extracted_search_words' in row and row['extracted_search_words'] and kw in row['extracted_search_words']:
                        match_count += 1
                    elif 'detected_object_names' in row and row['detected_object_names'] and kw in row['detected_object_names']:
                        match_count += 1
                    elif 'original_filename' in row and row['original_filename'] and kw in row['original_filename'].lower():
                        match_count += 1
                row['score'] = match_count / total_keywords if total_keywords else 0
            if color and row.get('average_color_rgb'):
                dist = color_distance(color, parse_json_field(row['average_color_rgb']))
                if dist > color_threshold:
                    ok = False
                else:
                    row['score'] = 1.0 - (dist / 100.0)
            if embedding and row.get('clip_embedding'):
                sim = cosine_similarity_score(embedding, parse_json_field(row['clip_embedding']))
                if sim < sim_threshold:
                    ok = False
                else:
                    # If both color and vector are present, average their scores? For now, just use vector sim
                    row['similarity_score'] = sim
            if ok:
                results.append(transform_result(row))
        
        # Sort by score if available
        if results and results[0].get('score', 0.0) > 0.0:
            results.sort(key=lambda r: -r['score'])

        response_data = {'results': results, 'count': len(results)}
        
        # Add extracted keywords to response if text search was used
        if text:
            response_data['extracted_keywords'] = keywords
            response_data['original_text'] = text

        return jsonify(response_data)
    except Exception as e:
        app.logger.error(f"Error in multimodal_search: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/search/temporal', methods=['POST'])
def search_by_time():
    data = request.get_json()
    start = data.get('start_time', 0)
    end = data.get('end_time')
    video_id = data.get('video_id')
    limit = data.get('limit', 50)

    if end is None:
        return jsonify({'error': 'end_time is required'}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.timestamp_seconds BETWEEN %s AND %s
        """
        params = [start, end]
        if video_id:
            sql += " AND m.video_id = %s"
            params.append(video_id)

        sql += " ORDER BY m.timestamp_seconds LIMIT %s"
        params.append(limit)

        cursor.execute(sql, params)
        results = cursor.fetchall()
        formatted = [transform_result(row) for row in results]

        return jsonify({'results': formatted, 'count': len(formatted)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/objects', methods=['POST'])
def search_by_objects():
    data = request.get_json()
    objects = data.get('objects', [])
    match_all = data.get('match_all', False)
    limit = data.get('limit', 50)

    if not objects:
        return jsonify({'error': 'objects array is required'}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        where_clauses = []
        params = []

        for obj in objects:
            where_clauses.append("array_to_string(m.detected_object_names, ' ') ILIKE %s")
            params.append(f'%{obj}%')

        clause = " AND ".join(where_clauses) if match_all else " OR ".join(where_clauses)
        sql = f"""
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE {clause}
            ORDER BY m.timestamp_seconds
            LIMIT %s
        """
        params.append(limit)
        cursor.execute(sql, params)
        results = cursor.fetchall()

        formatted = [transform_result(row) for row in results]

        return jsonify({'results': formatted, 'count': len(formatted)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/search/segment', methods=['POST'])
def search_video_segment():
    data = request.get_json()
    video_id = data.get('video_id')
    timestamp = data.get('timestamp')
    tolerance = data.get('tolerance', 5.0)

    if not video_id or timestamp is None:
        return jsonify({'error': 'video_id and timestamp are required'}), 400

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds,
            ABS(m.timestamp_seconds - %s) as time_diff
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.video_id = %s
            AND ABS(m.timestamp_seconds - %s) <= %s
            ORDER BY time_diff
            LIMIT 10
        """, [timestamp, video_id, timestamp, tolerance])

        results = cursor.fetchall()
        formatted = [transform_result(row) for row in results]

        return jsonify({'results': formatted, 'count': len(formatted)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

# ============================================================================
# DRES (VBS Competition) Endpoints
# ============================================================================

@app.route('/api/dres/status', methods=['GET'])
def dres_status():
    """Get DRES connection status and competition information."""
    if not DRES_AVAILABLE:
        return jsonify({
            'error': 'DRES client not available',
            'message': 'DRES integration is not configured'
        }), 503
    
    try:
        client = get_dres_client()
        connection_status = client.test_connection()
        
        status_info = {
            'connected': connection_status,
            'timestamp': datetime.now().isoformat()
        }
        
        if connection_status:
            # Get additional competition information
            competition_status = client.get_competition_status()
            if competition_status:
                status_info['competition'] = competition_status
            
            active_queries = client.get_active_queries()
            status_info['active_queries_count'] = len(active_queries)
        
        return jsonify(status_info)
        
    except Exception as e:
        return jsonify({
            'error': 'DRES status check failed',
            'message': str(e)
        }), 500

@app.route('/api/dres/submit', methods=['POST'])
def dres_submit():
    """Submit a Known-Item Search (KIS) result to DRES."""
    if not DRES_AVAILABLE:
        return jsonify({
            'error': 'DRES client not available',
            'message': 'DRES integration is not configured'
        }), 503
    
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['query_id', 'video_id', 'timestamp']
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'error': f'Missing required field: {field}'
                }), 400
        
        query_id = data['query_id']
        video_id = data['video_id']
        timestamp = float(data['timestamp'])
        confidence = float(data.get('confidence', 1.0))
        segment_start = data.get('segment_start')
        segment_end = data.get('segment_end')
        
        # Validate timestamp
        if timestamp < 0:
            return jsonify({
                'error': 'Timestamp must be non-negative'
            }), 400
        
        # Validate confidence
        if not 0.0 <= confidence <= 1.0:
            return jsonify({
                'error': 'Confidence must be between 0.0 and 1.0'
            }), 400
        
        # Submit to DRES
        client = get_dres_client()
        success = client.submit_result(
            query_id=query_id,
            video_id=video_id,
            timestamp=timestamp,
            confidence=confidence,
            segment_start=segment_start,
            segment_end=segment_end
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Successfully submitted result for query {query_id}',
                'submission': {
                    'query_id': query_id,
                    'video_id': video_id,
                    'timestamp': timestamp,
                    'confidence': confidence
                }
            })
        else:
            return jsonify({
                'error': 'DRES submission failed',
                'message': 'Failed to submit result to DRES server'
            }), 500
            
    except ValueError as e:
        return jsonify({
            'error': 'Invalid data format',
            'message': str(e)
        }), 400
    except Exception as e:
        return jsonify({
            'error': 'Submission failed',
            'message': str(e)
        }), 500

@app.route('/api/dres/queries', methods=['GET'])
def dres_queries():
    """Get active queries from DRES."""
    if not DRES_AVAILABLE:
        return jsonify({
            'error': 'DRES client not available',
            'message': 'DRES integration is not configured'
        }), 503
    
    try:
        client = get_dres_client()
        queries = client.get_active_queries()
        
        return jsonify({
            'queries': queries,
            'count': len(queries)
        })
        
    except Exception as e:
        return jsonify({
            'error': 'Failed to get queries',
            'message': str(e)
        }), 500

@app.route('/api/dres/query/<query_id>', methods=['GET'])
def dres_query_info(query_id):
    """Get information about a specific query from DRES."""
    if not DRES_AVAILABLE:
        return jsonify({
            'error': 'DRES client not available',
            'message': 'DRES integration is not configured'
        }), 503
    
    try:
        client = get_dres_client()
        query_info = client.get_query_info(query_id)
        
        if query_info:
            return jsonify(query_info)
        else:
            return jsonify({
                'error': 'Query not found',
                'message': f'Query {query_id} not found or not accessible'
            }), 404
            
    except Exception as e:
        return jsonify({
            'error': 'Failed to get query info',
            'message': str(e)
        }), 500

@app.route('/api/dres/history', methods=['GET'])
def dres_submission_history():
    """Get submission history from DRES."""
    if not DRES_AVAILABLE:
        return jsonify({
            'error': 'DRES client not available',
            'message': 'DRES integration is not configured'
        }), 503
    
    try:
        query_id = request.args.get('query_id')
        client = get_dres_client()
        history = client.get_submission_history(query_id)
        
        return jsonify({
            'history': history,
            'count': len(history)
        })
        
    except Exception as e:
        return jsonify({
            'error': 'Failed to get submission history',
            'message': str(e)
        }), 500

@app.route('/api/dres/submit-batch', methods=['POST'])
def dres_submit_batch():
    """Submit multiple results to DRES at once."""
    if not DRES_AVAILABLE:
        return jsonify({
            'error': 'DRES client not available',
            'message': 'DRES integration is not configured'
        }), 503
    
    try:
        data = request.get_json()
        
        if not isinstance(data, list):
            return jsonify({
                'error': 'Invalid data format',
                'message': 'Expected a list of submission objects'
            }), 400
        
        # Validate each submission
        for i, submission in enumerate(data):
            required_fields = ['query_id', 'video_id', 'timestamp']
            for field in required_fields:
                if field not in submission:
                    return jsonify({
                        'error': f'Missing required field {field} in submission {i}'
                    }), 400
            
            # Validate timestamp and confidence
            try:
                timestamp = float(submission['timestamp'])
                if timestamp < 0:
                    return jsonify({
                        'error': f'Invalid timestamp in submission {i}'
                    }), 400
            except (ValueError, TypeError):
                return jsonify({
                    'error': f'Invalid timestamp format in submission {i}'
                }), 400
            
            confidence = submission.get('confidence', 1.0)
            try:
                confidence = float(confidence)
                if not 0.0 <= confidence <= 1.0:
                    return jsonify({
                        'error': f'Invalid confidence in submission {i}'
                    }), 400
            except (ValueError, TypeError):
                return jsonify({
                    'error': f'Invalid confidence format in submission {i}'
                }), 400
        
        # Submit batch to DRES
        client = get_dres_client()
        results = client.submit_multiple_results(data)
        
        # Count successes and failures
        successful = sum(1 for success in results.values() if success)
        failed = len(results) - successful
        
        return jsonify({
            'success': True,
            'message': f'Batch submission completed: {successful} successful, {failed} failed',
            'results': results,
            'summary': {
                'total': len(results),
                'successful': successful,
                'failed': failed
            }
        })
        
    except Exception as e:
        return jsonify({
            'error': 'Batch submission failed',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
