# server.py

from flask import Flask, request, jsonify, send_from_directory, Response
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
VIDEO_DATASET_PATH = r'C:\Users\Hp\Desktop\vbs-video-retrieval-system\data'
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
    """Serve video files from the dataset directory with better streaming support."""
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

    # Obtenir la taille du fichier
    file_size = os.path.getsize(video_path)

    # Gérer les requêtes Range (important pour le seeking dans les vidéos)
    range_header = request.headers.get('Range', None)
    if range_header:
        # Parser le header Range
        byte_start = 0
        byte_end = file_size - 1

        if range_header.startswith('bytes='):
            range_value = range_header[6:]
            if '-' in range_value:
                start, end = range_value.split('-', 1)
                if start:
                    byte_start = int(start)
                if end:
                    byte_end = min(int(end), file_size - 1)

        # Lire et envoyer la partie demandée
        def generate_range():
            with open(video_path, 'rb') as f:
                f.seek(byte_start)
                remaining = byte_end - byte_start + 1
                while remaining > 0:
                    chunk_size = min(8192, remaining)
                    data = f.read(chunk_size)
                    if not data:
                        break
                    remaining -= len(data)
                    yield data

        response = Response(
            generate_range(),
            206,  # Partial Content
            headers={
                'Content-Type': 'video/mp4',
                'Accept-Ranges': 'bytes',
                'Content-Range': f'bytes {byte_start}-{byte_end}/{file_size}',
                'Content-Length': str(byte_end - byte_start + 1),
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Range',
                'Cache-Control': 'no-cache'
            }
        )
        return response

    # Envoyer le fichier complet si pas de Range
    def generate():
        with open(video_path, 'rb') as f:
            while True:
                data = f.read(8192)
                if not data:
                    break
                yield data

    response = Response(
        generate(),
        200,
        headers={
            'Content-Type': 'video/mp4',
            'Accept-Ranges': 'bytes',
            'Content-Length': str(file_size),
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Range',
        }
    )
    return response


def transform_result(row):
    """Transforms a database row to the format expected by the frontend."""
    video_id = row.get('video_id')
    timestamp = row.get('timestamp_seconds', 0.0)

    # Always use the video_id.mp4 format since that's how the files are actually named
    filename = f"{video_id}.mp4"

    # CORRECTION PRINCIPALE: Ajouter le timestamp à l'URL pour démarrage automatique
    base_video_url = f"{API_URL_BASE}/api/videos/{video_id}/{filename}"

    # Ajouter le paramètre de timestamp pour démarrage automatique au bon moment
    if timestamp > 0:
        video_url = f"{base_video_url}#t={timestamp}"
    else:
        video_url = base_video_url

    transformed = {
        'id': row.get('moment_id'),
        'title': f"Video {video_id} at {timestamp:.1f}s",
        'video_path': video_url,  # Maintenant avec timestamp
        'duration': row.get('duration_seconds', 0),
        'score': row.get('similarity_score') or row.get('score') or 0.0,
        'timestamp': timestamp,
        'objects': parse_json_field(row.get('detected_object_names', '[]')),
        'text': parse_json_field(row.get('extracted_search_words', '[]')),
        'dominant_colors': []
    }

    if row.get('average_color_rgb'):
        transformed['dominant_colors'].append(parse_json_field(row.get('average_color_rgb')))

    return transformed


@app.route('/api/videos/<video_id>/<filename>/segment')
def serve_video_segment(video_id, filename):
    """Serve a specific segment of video starting at a timestamp."""
    start_time = request.args.get('t', 0, type=float)
    duration = request.args.get('duration', 30, type=float)  # 30 secondes par défaut

    video_dir = os.path.join(VIDEO_DATASET_PATH, "V3C1-200", video_id)
    video_path = os.path.join(video_dir, filename)

    if not os.path.exists(video_path):
        return jsonify({'error': 'Video file not found'}), 404

    # Retourner l'URL avec timestamp pour que le player démarre au bon moment
    segment_url = f"{API_URL_BASE}/api/videos/{video_id}/{filename}#t={start_time}"

    return jsonify({
        'segment_url': segment_url,
        'start_time': start_time,
        'duration': duration,
        'video_id': video_id,
        'filename': filename
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

    keywords = extract_keywords_from_sentence(query)
    if not keywords:
        return jsonify({'error': 'No meaningful keywords found in query'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # MÊME REQUÊTE SQL
        sql = """
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.extraction_success = true
            ORDER BY m.timestamp_seconds
        """

        cursor.execute(sql)
        all_results = cursor.fetchall()

        # AMÉLIORATION INTERNE : Utiliser les données riches
        scored_results = []
        for row in all_results:

            # 1. EXTRACTION améliorée des données
            detailed_features = parse_json_field(row.get('detailed_features') or '{}')

            # Objets avec confiance
            high_confidence_objects = []
            if isinstance(detailed_features, dict) and 'detected_objects_detailed' in detailed_features:
                for obj_detail in detailed_features['detected_objects_detailed']:
                    if isinstance(obj_detail, dict) and obj_detail.get('confidence', 0) >= 0.3:
                        high_confidence_objects.append(obj_detail.get('name', '').lower())

            # Fallback sur les objets simples si pas de détails
            if not high_confidence_objects:
                extracted_words = parse_json_field(row.get('extracted_search_words') or '[]')
                detected_objects = parse_json_field(row.get('detected_object_names') or '[]')
                high_confidence_objects = [obj.lower() for obj in detected_objects if obj.strip()]
            else:
                extracted_words = parse_json_field(row.get('extracted_search_words') or '[]')

            # 2. SCORING amélioré
            all_searchable = high_confidence_objects + [word.lower() for word in extracted_words]
            filename = row.get('original_filename') or ''
            all_text = ' '.join(all_searchable + [filename]).lower()

            # Calculer les correspondances
            keyword_matches = 0
            total_keywords = len(keywords)
            matched_terms = []

            for keyword in keywords:
                keyword_lower = keyword.lower()

                # Correspondance exacte
                if keyword_lower in all_searchable:
                    keyword_matches += 1
                    matched_terms.append(keyword)
                # Correspondance sémantique simple
                elif keyword_lower in ['man', 'person'] and any('person' in obj for obj in all_searchable):
                    keyword_matches += 0.8
                    matched_terms.append(f"{keyword}→person")
                elif keyword_lower in ['car', 'vehicle'] and any(
                        v in obj for obj in all_searchable for v in ['car', 'vehicle']):
                    keyword_matches += 0.8
                    matched_terms.append(f"{keyword}→vehicle")
                # Correspondance partielle
                elif keyword_lower in all_text:
                    keyword_matches += 0.5
                    matched_terms.append(f"{keyword}(partial)")

            # 3. SCORE final amélioré
            if keyword_matches > 0:
                relevance_score = keyword_matches / total_keywords

                # Bonus pour objets détectés avec confiance
                if any(kw.lower() in high_confidence_objects for kw in keywords):
                    relevance_score += 0.3

                # Bonus pour combinaisons logiques
                if any('person' in obj for obj in high_confidence_objects) and any(
                        v in obj for obj in high_confidence_objects for v in ['car', 'bicycle']):
                    relevance_score += 0.2

                # Assurer score visible
                if relevance_score < 0.2:
                    relevance_score = 0.2

                row['score'] = min(relevance_score, 1.0)
                row['matched_terms'] = matched_terms
                scored_results.append(row)

        # MÊME LOGIQUE de tri et formatage
        scored_results.sort(key=lambda x: x['score'], reverse=True)
        top_results = scored_results[:limit]
        formatted = [transform_result(row) for row in top_results]

        # MÊME FORMAT de réponse
        return jsonify({
            'results': formatted,
            'count': len(formatted),
            'extracted_keywords': keywords,
            'query': query,
            'score_type': 'keyword_relevance'
        })
    except Exception as e:
        app.logger.error(f"Error in search_by_text: {e}")
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
    """MÊME NOM D'API - logique interne améliorée."""
    data = request.get_json()
    text = data.get('text')
    color = data.get('color')
    objects = data.get('objects', [])
    words = data.get('words', [])
    start_time = data.get('start_time')
    end_time = data.get('end_time')
    embedding = data.get('embedding')
    limit = int(data.get('limit', 50))
    color_threshold = int(data.get('color_threshold', 50))
    sim_threshold = float(data.get('similarity_threshold', 0.7))

    try:
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500

        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # MÊME REQUÊTE SQL de base
        sql = """
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.extraction_success = true
        """
        params = []

        if start_time is not None and end_time is not None:
            sql += " AND m.timestamp_seconds BETWEEN %s AND %s"
            params.extend([float(start_time), float(end_time)])

        sql += " ORDER BY m.timestamp_seconds"
        cursor.execute(sql, params)
        all_results = cursor.fetchall()

        # AMÉLIORATION INTERNE : Keywords si texte fourni
        keywords = []
        if text and text.strip():
            keywords = extract_keywords_from_sentence(text.strip())

        # LOGIQUE améliorée de filtrage
        filtered_results = []

        for row in all_results:
            include_result = True
            scores = []

            # AMÉLIORATION : Utiliser les données riches
            detailed_features = parse_json_field(row.get('detailed_features') or '{}')

            # Extraire objets avec confiance
            high_conf_objects = []
            if isinstance(detailed_features, dict) and 'detected_objects_detailed' in detailed_features:
                for obj_detail in detailed_features['detected_objects_detailed']:
                    if isinstance(obj_detail, dict) and obj_detail.get('confidence', 0) >= 0.3:
                        high_conf_objects.append(obj_detail.get('name', '').lower())

            # Fallback
            if not high_conf_objects:
                detected_objects = parse_json_field(row.get('detected_object_names') or '[]')
                high_conf_objects = [obj.lower() for obj in detected_objects]

            extracted_words = parse_json_field(row.get('extracted_search_words') or '[]')

            # 1. FILTRE TEXTE amélioré
            if keywords:
                all_searchable = [item.lower() for item in extracted_words + high_conf_objects]
                keyword_matches = sum(1 for kw in keywords if kw.lower() in ' '.join(all_searchable))

                if keyword_matches > 0:
                    text_score = keyword_matches / len(keywords)
                    # Bonus pour objets haute confiance
                    if any(kw.lower() in high_conf_objects for kw in keywords):
                        text_score += 0.2
                    scores.append(text_score)
                elif keywords:
                    include_result = False

            # 2. FILTRE OBJETS - MÊME LOGIQUE
            if objects and include_result:
                object_matches = sum(1 for obj in objects if obj.lower() in high_conf_objects)
                if object_matches > 0:
                    scores.append(object_matches / len(objects))
                else:
                    include_result = False

            # 3. FILTRE MOTS - MÊME LOGIQUE
            if words and include_result:
                words_lower = [word.lower() for word in extracted_words]
                word_matches = sum(1 for word in words if word.lower() in words_lower)
                if word_matches > 0:
                    scores.append(word_matches / len(words))
                else:
                    include_result = False

            # 4. FILTRE COULEUR - MÊME LOGIQUE
            if color and include_result:
                avg_color = row.get('average_color_rgb')
                if avg_color:
                    try:
                        if isinstance(avg_color, str):
                            avg_color = parse_json_field(avg_color)
                        if avg_color and len(avg_color) >= 3:
                            dist = color_distance(color, avg_color[:3])
                            if dist <= color_threshold:
                                scores.append(1.0 - (dist / 100.0))
                            else:
                                include_result = False
                        else:
                            include_result = False
                    except:
                        include_result = False

            # 5. FILTRE VECTOR - MÊME LOGIQUE
            if embedding and include_result:
                clip_emb = row.get('clip_embedding')
                if clip_emb:
                    try:
                        if isinstance(clip_emb, str):
                            clip_emb = parse_json_field(clip_emb)
                        if clip_emb:
                            sim = cosine_similarity_score(embedding, clip_emb)
                            if sim >= sim_threshold:
                                scores.append(sim)
                            else:
                                include_result = False
                        else:
                            include_result = False
                    except:
                        include_result = False

            # SCORE final amélioré
            if include_result:
                if scores:
                    final_score = sum(scores) / len(scores)
                    # Assurer score visible
                    if final_score < 0.2:
                        final_score = 0.2
                    row['score'] = final_score
                else:
                    row['score'] = 0.5

                filtered_results.append(row)

        # MÊME LOGIQUE de tri et formatage
        filtered_results.sort(key=lambda r: r.get('score', 0.0), reverse=True)
        final_results = filtered_results[:limit]
        formatted_results = [transform_result(row) for row in final_results]

        # MÊME FORMAT de réponse
        response_data = {
            'results': formatted_results,
            'count': len(formatted_results),
            'extracted_keywords': keywords,
            'original_text': text if text else "",
            'filters_applied': {
                'text': bool(text and keywords),
                'color': bool(color),
                'objects': bool(objects),
                'words': bool(words),
                'time_range': bool(start_time is not None and end_time is not None),
                'vector': bool(embedding)
            },
            'total_before_filtering': len(all_results),
            'total_after_filtering': len(filtered_results)
        }

        return jsonify(response_data)

    except Exception as e:
        app.logger.error(f"Error in multimodal_search: {e}")
        return jsonify({'error': 'Search failed', 'message': str(e)}, 500)
    finally:
        if 'conn' in locals() and conn:
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
    include_context = data.get('include_context', True)  # Inclure les moments avant/après
    context_window = data.get('context_window', 10.0)  # Fenêtre de contexte en secondes

    if not video_id or timestamp is None:
        return jsonify({'error': 'video_id and timestamp are required'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Requête principale pour les moments proches du timestamp
        cursor.execute("""
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds,
                   ABS(m.timestamp_seconds - %s) as time_diff,
                   CASE 
                       WHEN ABS(m.timestamp_seconds - %s) = 0 THEN 1.0
                       ELSE 1.0 - (ABS(m.timestamp_seconds - %s) / %s)
                   END as proximity_score
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.video_id = %s
            AND m.extraction_success = true
            AND ABS(m.timestamp_seconds - %s) <= %s
            ORDER BY time_diff
            LIMIT 10
        """, [timestamp, timestamp, timestamp, tolerance, video_id, timestamp, tolerance])

        main_results = cursor.fetchall()

        # Si demandé, inclure aussi les moments de contexte (avant/après)
        all_results = main_results
        if include_context and main_results:
            # Chercher les moments dans une fenêtre plus large pour le contexte
            cursor.execute("""
                SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds,
                       ABS(m.timestamp_seconds - %s) as time_diff,
                       0.5 as proximity_score  -- Score plus faible pour le contexte
                FROM video_moments m
                JOIN videos v ON m.video_id = v.video_id
                WHERE m.video_id = %s
                AND m.extraction_success = true
                AND m.timestamp_seconds BETWEEN %s AND %s
                AND ABS(m.timestamp_seconds - %s) > %s  -- Exclure les résultats déjà trouvés
                ORDER BY m.timestamp_seconds
                LIMIT 5
            """, [
                timestamp,
                video_id,
                timestamp - context_window,
                timestamp + context_window,
                timestamp,
                tolerance
            ])

            context_results = cursor.fetchall()
            all_results.extend(context_results)

        # Traiter et formater les résultats
        formatted_results = []
        for row in all_results:
            # Ajouter le score de proximité au row
            row['score'] = row.get('proximity_score', 0.0)
            formatted_result = transform_result(row)

            # Ajouter des métadonnées utiles
            formatted_result['time_difference'] = row.get('time_diff', 0.0)
            formatted_result['is_exact_match'] = row.get('time_diff', 0.0) < 1.0
            formatted_result['segment_type'] = 'exact' if row.get('time_diff', 0.0) <= tolerance else 'context'

            formatted_results.append(formatted_result)

        # Trier par score de proximité (plus proche = meilleur score)
        formatted_results.sort(key=lambda x: -x.get('score', 0.0))

        # Calculer quelques statistiques utiles
        exact_matches = sum(1 for r in formatted_results if r.get('is_exact_match', False))

        response_data = {
            'results': formatted_results,
            'count': len(formatted_results),
            'exact_matches': exact_matches,
            'search_params': {
                'video_id': video_id,
                'target_timestamp': timestamp,
                'tolerance': tolerance,
                'include_context': include_context
            },
            'best_match': formatted_results[0] if formatted_results else None
        }

        return jsonify(response_data)

    except Exception as e:
        app.logger.error(f"Error in search_video_segment: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


# Endpoint complémentaire pour obtenir une séquence vidéo continue
@app.route('/api/search/sequence', methods=['POST'])
def search_video_sequence():
    """Recherche une séquence continue de moments dans une vidéo."""
    data = request.get_json()
    video_id = data.get('video_id')
    start_time = data.get('start_time')
    end_time = data.get('end_time')

    if not video_id or start_time is None or end_time is None:
        return jsonify({'error': 'video_id, start_time and end_time are required'}), 400

    if start_time >= end_time:
        return jsonify({'error': 'start_time must be less than end_time'}), 400

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds,
                   (m.timestamp_seconds - %s) as relative_time
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.video_id = %s
            AND m.extraction_success = true
            AND m.timestamp_seconds BETWEEN %s AND %s
            ORDER BY m.timestamp_seconds
        """, [start_time, video_id, start_time, end_time])

        results = cursor.fetchall()

        # Ajouter des scores basés sur la position dans la séquence
        total_duration = end_time - start_time
        formatted_results = []

        for i, row in enumerate(results):
            # Score basé sur la position dans la séquence (début = score plus élevé)
            position_score = 1.0 - (i / len(results)) if results else 0.0
            row['score'] = position_score

            formatted_result = transform_result(row)
            formatted_result['sequence_position'] = i + 1
            formatted_result['relative_time'] = row.get('relative_time', 0.0)

            formatted_results.append(formatted_result)

        return jsonify({
            'results': formatted_results,
            'count': len(formatted_results),
            'sequence_info': {
                'video_id': video_id,
                'start_time': start_time,
                'end_time': end_time,
                'duration': total_duration,
                'moments_found': len(formatted_results)
            }
        })

    except Exception as e:
        app.logger.error(f"Error in search_video_sequence: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


# Endpoint pour trouver les "highlights" d'une vidéo (moments avec les meilleurs scores)
@app.route('/api/videos/<video_id>/highlights', methods=['GET'])
def get_video_highlights(video_id):
    """Obtient les moments les plus pertinents d'une vidéo."""
    limit = request.args.get('limit', 10, type=int)
    min_score = request.args.get('min_score', 0.5, type=float)

    conn = get_db_connection()
    if not conn:
        return jsonify({'error': 'Database connection failed'}), 500

    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cursor.execute("""
            SELECT m.*, v.original_filename, v.compressed_filename, v.duration_seconds,
                   COALESCE(m.overall_relevance_score, m.text_relevance_score, 0.0) as highlight_score
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.video_id = %s
            AND m.extraction_success = true
            AND COALESCE(m.overall_relevance_score, m.text_relevance_score, 0.0) >= %s
            ORDER BY highlight_score DESC, m.timestamp_seconds
            LIMIT %s
        """, [video_id, min_score, limit])

        results = cursor.fetchall()

        formatted_results = []
        for row in results:
            row['score'] = row.get('highlight_score', 0.0)
            formatted_result = transform_result(row)
            formatted_result['highlight_type'] = 'high_relevance'
            formatted_results.append(formatted_result)

        return jsonify({
            'results': formatted_results,
            'count': len(formatted_results),
            'video_id': video_id,
            'criteria': {
                'min_score': min_score,
                'limit': limit
            }
        })

    except Exception as e:
        app.logger.error(f"Error in get_video_highlights: {e}")
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
