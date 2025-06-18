# server.py

from flask import Flask, request, jsonify
from flask_cors import CORS
import psycopg2.extras

from db_utils import get_db_connection, fetch_all_moments_with_colors_and_embeddings
from utils_server import color_distance, cosine_similarity_score, parse_json_field

app = Flask(__name__)
CORS(app)

@app.route('/')
def home():
    return 'Welcome to the VBS Video Retrieval System!'

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat(),
        'service': 'IR Video Retrieval API'
    })

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
            where_clauses.append("m.extracted_search_words ILIKE %s")
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

        formatted = []
        for row in results:
            row['detected_object_names'] = parse_json_field(row.get('detected_object_names'))
            row['extracted_search_words'] = parse_json_field(row.get('extracted_search_words'))
            formatted.append(row)

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

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        sql = """
            SELECT m.*, v.original_filename
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.extracted_search_words ILIKE %s
               OR m.detected_object_names ILIKE %s
               OR v.original_filename ILIKE %s
            ORDER BY m.timestamp_seconds
            LIMIT %s
        """
        # Ajout du paramètre pour detected_object_names
        cursor.execute(sql, [f'%{query}%', f'%{query}%', f'%{query}%', limit])
        results = cursor.fetchall()

        formatted = []
        for row in results:
            row['detected_object_names'] = parse_json_field(row.get('detected_object_names'))
            row['extracted_search_words'] = parse_json_field(row.get('extracted_search_words'))
            formatted.append(row)

        return jsonify({'results': formatted, 'count': len(formatted)})
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
            SELECT m.*, v.original_filename
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
                row['color_distance'] = round(distance, 2)
                row['detected_object_names'] = parse_json_field(row.get('detected_object_names'))
                row['extracted_search_words'] = parse_json_field(row.get('extracted_search_words'))
                results.append(row)

        results.sort(key=lambda r: r['color_distance'])
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
                row['similarity_score'] = round(score, 4)
                row['detected_object_names'] = parse_json_field(row.get('detected_object_names'))
                row['extracted_search_words'] = parse_json_field(row.get('extracted_search_words'))
                row.pop('clip_embedding', None)
                results.append(row)

        results.sort(key=lambda r: -r['similarity_score'])
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
    embedding = data.get('embedding')
    threshold = data.get('threshold', 50)  # For color distance
    sim_threshold = data.get('similarity_threshold', 0.7)
    limit = data.get('limit', 50)

    # Optional: weights for scoring
    weight_sim = 0.6
    weight_color = 0.4

    conn = get_db_connection()
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute("""
            SELECT m.*, v.original_filename
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
        """)
        rows = cursor.fetchall()
        results = []

        for row in rows:
            ok = True
            score_components = {}
            total_score = 0

            # Parse fields once
            extracted_words = parse_json_field(row.get('extracted_search_words')) or []
            detected_objects = parse_json_field(row.get('detected_object_names')) or []
            all_texts = extracted_words + detected_objects

            if text:
                if not any(text.lower() in str(t).lower() for t in all_texts):
                    ok = False

            if color and ok:
                moment_color = parse_json_field(row.get('average_color_rgb'))
                if moment_color:
                    dist = color_distance(color, moment_color)
                    if dist > threshold:
                        ok = False
                    else:
                        row['color_distance'] = round(dist, 2)
                        score_components['color_distance'] = dist
                else:
                    ok = False

            if embedding and ok:
                moment_embedding = parse_json_field(row.get('clip_embedding'))
                if moment_embedding:
                    sim = cosine_similarity_score(embedding, moment_embedding)
                    if sim < sim_threshold:
                        ok = False
                    else:
                        row['similarity_score'] = round(sim, 4)
                        score_components['similarity_score'] = sim
                else:
                    ok = False

            if ok:
                # Compute a combined score (only if both are available)
                sim_score = score_components.get('similarity_score', 0)
                color_dist = score_components.get('color_distance', 255)
                normalized_color_score = 1 - min(color_dist / 255, 1)  # Normalize to [0-1]

                total_score = weight_sim * sim_score + weight_color * normalized_color_score
                row['total_score'] = round(total_score, 4)

                row['detected_object_names'] = detected_objects
                row['extracted_search_words'] = extracted_words
                row.pop('clip_embedding', None)

                results.append(row)

        # Sort by total_score descending
        results = sorted(results, key=lambda r: -r.get('total_score', 0))[:limit]

        return jsonify({'results': results, 'count': len(results)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

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
            SELECT m.*, v.original_filename FROM video_moments m
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
        formatted = []
        for row in results:
            row['detected_object_names'] = parse_json_field(row.get('detected_object_names'))
            row['extracted_search_words'] = parse_json_field(row.get('extracted_search_words'))
            formatted.append(row)

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
            where_clauses.append("m.detected_object_names ILIKE %s")
            params.append(f'%{obj}%')

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

        formatted = []
        for row in results:
            row['detected_object_names'] = parse_json_field(row.get('detected_object_names'))
            row['extracted_search_words'] = parse_json_field(row.get('extracted_search_words'))
            formatted.append(row)

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
            SELECT m.*, v.original_filename,
            ABS(m.timestamp_seconds - %s) as time_diff
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.video_id = %s
            AND ABS(m.timestamp_seconds - %s) <= %s
            ORDER BY time_diff
            LIMIT 10
        """, [timestamp, video_id, timestamp, tolerance])

        results = cursor.fetchall()
        formatted = []
        for row in results:
            row['time_diff'] = float(row['time_diff'])
            row['detected_object_names'] = parse_json_field(row.get('detected_object_names'))
            row['extracted_search_words'] = parse_json_field(row.get('extracted_search_words'))
            formatted.append(row)

        return jsonify({'results': formatted, 'count': len(formatted)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=5000, debug=False)
