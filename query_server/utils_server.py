import json
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import re
import string
from typing import List, Optional, Dict
from collections import Counter


def parse_json_field(field_value):
    """Safely parse JSON field that might be a string or already parsed."""
    if field_value is None:
        return []
    if isinstance(field_value, list):
        return field_value
    if isinstance(field_value, str):
        try:
            # Handle PostgreSQL array format like {item1,item2}
            if field_value.startswith('{') and field_value.endswith('}'):
                # Remove braces and split by comma
                content = field_value[1:-1]
                if not content:  # Empty array {}
                    return []
                items = content.split(',')
                return [item.strip().strip('"').strip("'") for item in items if item.strip()]
            else:
                import json
                return json.loads(field_value)
        except:
            return []
    return []


def color_distance(color1, color2):
    """Weighted Euclidean distance in RGB space based on human perception."""
    if not color1 or not color2 or len(color1) != 3 or len(color2) != 3:
        return float('inf')
    # Perceptual weighting for RGB (similar to luminance calculation)
    r_weight = 0.299
    g_weight = 0.587
    b_weight = 0.114

    r_diff = (color1[0] - color2[0]) * r_weight
    g_diff = (color1[1] - color2[1]) * g_weight
    b_diff = (color1[2] - color2[2]) * b_weight
    return (r_diff ** 2 + g_diff ** 2 + b_diff ** 2) ** 0.5


def cosine_similarity_score(embedding1, embedding2):
    """Compute cosine similarity between two embedding vectors."""
    if not embedding1 or not embedding2:
        return 0.0
    try:
        emb1 = np.array(embedding1).reshape(1, -1)
        emb2 = np.array(embedding2).reshape(1, -1)
        return float(np.dot(emb1, emb2.T) / (np.linalg.norm(emb1) * np.linalg.norm(emb2)))
    except Exception:
        return 0.0


def extract_keywords_from_sentence(sentence: str) -> List[str]:
    """
    Extract meaningful keywords from a sentence by:
    1. Converting to lowercase
    2. Removing punctuation
    3. Splitting into words
    4. Filtering out common stop words
    5. Removing very short words (less than 2 characters)

    Args:
        sentence (str): Input sentence from user

    Returns:
        List[str]: List of extracted keywords
    """
    # Common English stop words to filter out
    stop_words = {
        'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from', 'has', 'he',
        'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 'to', 'was', 'will',
        'with', 'the', 'this', 'but', 'they', 'have', 'had', 'what', 'said', 'each',
        'which', 'she', 'do', 'how', 'their', 'if', 'up', 'out', 'many', 'then',
        'them', 'these', 'so', 'some', 'her', 'would', 'make', 'like', 'into', 'him',
        'time', 'two', 'more', 'go', 'no', 'way', 'could', 'my', 'than', 'first',
        'been', 'call', 'who', 'its', 'now', 'find', 'long', 'down', 'day', 'did',
        'get', 'come', 'made', 'may', 'part', 'i', 'me', 'my', 'myself', 'we', 'our',
        'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he',
        'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
        'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom',
        'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been',
        'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'will',
        'would', 'should', 'could', 'can', 'may', 'might', 'must', 'shall', 'ought',
        'over', 'under', 'above', 'below', 'between', 'among', 'through', 'during',
        'before', 'after', 'since', 'until', 'while', 'where', 'when', 'why', 'how'
    }

    # Convert to lowercase and remove punctuation
    sentence = sentence.lower()
    sentence = re.sub(r'[^\w\s]', ' ', sentence)

    # Split into words and filter
    words = sentence.split()
    keywords = []

    for word in words:
        # Remove extra whitespace and check length
        word = word.strip()
        if len(word) >= 2 and word not in stop_words:
            keywords.append(word)

    return keywords


def calculate_keyword_match_score(search_keywords: List[str], extracted_words: List[str],
                                  detected_objects: List[str], filename: str = "") -> float:
    """
    Calculate how well the content matches specific search keywords.

    Args:
        search_keywords: Keywords from user search
        extracted_words: Words extracted from OCR
        detected_objects: Objects detected in the frame
        filename: Video filename

    Returns:
        float: Match score between 0.0 and 1.0
    """
    if not search_keywords:
        return 0.0

    # Combine all searchable content
    all_content = []
    if extracted_words:
        all_content.extend([word.lower() for word in extracted_words])
    if detected_objects:
        all_content.extend([obj.lower() for obj in detected_objects])
    if filename:
        all_content.extend(filename.lower().split())

    if not all_content:
        return 0.0

    # Calculate exact matches and partial matches
    exact_matches = 0
    partial_matches = 0
    search_content = ' '.join(all_content)

    for keyword in search_keywords:
        keyword_lower = keyword.lower()

        # Check for exact word match
        if keyword_lower in all_content:
            exact_matches += 1
        # Check for partial match (substring)
        elif keyword_lower in search_content:
            partial_matches += 1

    # Calculate weighted score
    exact_score = exact_matches / len(search_keywords)
    partial_score = (partial_matches / len(search_keywords)) * 0.5  # Partial matches worth less

    total_score = exact_score + partial_score
    return min(total_score, 1.0)


def calculate_text_relevance_score(extracted_words: List[str], detected_objects: List[str],
                                   filename: str, search_keywords: Optional[List[str]] = None) -> float:
    """
    Enhanced text relevance score with optional search keyword matching.

    Args:
        extracted_words: List of words from OCR
        detected_objects: List of detected objects
        filename: Video filename
        search_keywords: Optional keywords to match against

    Returns:
        float: Relevance score between 0.0 and 1.0
    """
    # Base relevance from content richness
    base_score = 0.0

    # Word richness score
    if extracted_words:
        unique_words = len(set(word.lower() for word in extracted_words))
        total_words = len(extracted_words)
        word_diversity = unique_words / max(total_words, 1)
        word_quantity = min(total_words / 10.0, 1.0)  # Normalize to 10 words max
        base_score += (word_diversity * 0.3 + word_quantity * 0.2) * 0.6

    # Object richness score
    if detected_objects:
        unique_objects = len(set(obj.lower() for obj in detected_objects))
        total_objects = len(detected_objects)
        object_diversity = unique_objects / max(total_objects, 1)
        object_quantity = min(total_objects / 5.0, 1.0)  # Normalize to 5 objects max
        base_score += (object_diversity * 0.3 + object_quantity * 0.2) * 0.3

    # Filename contribution
    if filename and len(filename) > 5:
        base_score += 0.1

    # If search keywords provided, boost score based on matches
    if search_keywords:
        keyword_match_score = calculate_keyword_match_score(
            search_keywords, extracted_words, detected_objects, filename
        )
        # Weighted combination: 40% base relevance, 60% keyword matching
        final_score = base_score * 0.4 + keyword_match_score * 0.6
    else:
        final_score = base_score

    return min(final_score, 1.0)


def calculate_object_relevance_score(detected_objects: List[str], target_objects: Optional[List[str]] = None) -> float:
    """
    Enhanced object relevance score with optional target object matching.

    Args:
        detected_objects: List of detected objects
        target_objects: Optional list of objects to specifically look for

    Returns:
        float: Relevance score between 0.0 and 1.0
    """
    if not detected_objects:
        return 0.0

    # Base score from object diversity and interaction potential
    unique_objects = len(set(obj.lower() for obj in detected_objects))
    total_objects = len(detected_objects)

    diversity_score = unique_objects / max(total_objects, 1)
    quantity_bonus = min(unique_objects * 0.15, 0.6)  # Bonus for multiple objects
    base_score = diversity_score * 0.4 + quantity_bonus

    # If target objects specified, boost score for matches
    if target_objects:
        detected_lower = [obj.lower() for obj in detected_objects]
        target_lower = [obj.lower() for obj in target_objects]

        exact_matches = sum(1 for target in target_lower if target in detected_lower)
        partial_matches = sum(1 for target in target_lower
                              if any(target in detected or detected in target
                                     for detected in detected_lower))

        match_score = (exact_matches + partial_matches * 0.5) / len(target_objects)
        # Weighted combination: 30% base relevance, 70% target matching
        final_score = base_score * 0.3 + match_score * 0.7
    else:
        final_score = base_score

    return min(final_score, 1.0)


def calculate_color_relevance_score(color_rgb: List[int], target_color: Optional[List[int]] = None) -> float:
    """
    Enhanced color relevance score with optional target color matching.

    Args:
        color_rgb: RGB color values [r, g, b]
        target_color: Optional target color to match against

    Returns:
        float: Relevance score between 0.0 and 1.0
    """
    if not color_rgb or len(color_rgb) != 3:
        return 0.0

    r, g, b = color_rgb

    # Base score from color characteristics
    # Calculate saturation and brightness
    max_val = max(r, g, b)
    min_val = min(r, g, b)

    if max_val == 0:
        return 0.0

    saturation = (max_val - min_val) / max_val
    brightness = (r + g + b) / (3 * 255)

    # Prefer medium brightness and reasonable saturation
    brightness_score = 1.0 - abs(brightness - 0.5) * 2  # Peak at 0.5 brightness
    saturation_score = min(saturation * 1.5, 1.0)  # Boost saturation slightly

    base_score = brightness_score * 0.3 + saturation_score * 0.7

    # If target color specified, calculate similarity
    if target_color and len(target_color) == 3:
        distance = color_distance(color_rgb, target_color)
        # Convert distance to similarity (lower distance = higher similarity)
        max_distance = 255 * np.sqrt(0.299 ** 2 + 0.587 ** 2 + 0.114 ** 2)  # Max possible distance
        similarity = 1.0 - (distance / max_distance)

        # Weighted combination: 20% base score, 80% color similarity
        final_score = base_score * 0.2 + similarity * 0.8
    else:
        final_score = base_score

    return min(final_score, 1.0)


def calculate_comprehensive_score(extracted_words: List[str], detected_objects: List[str],
                                  filename: str, color_rgb: List[int],
                                  search_keywords: Optional[List[str]] = None,
                                  target_objects: Optional[List[str]] = None,
                                  target_color: Optional[List[int]] = None,
                                  weights: Optional[Dict[str, float]] = None) -> Dict[str, float]:
    """
    Calculate comprehensive relevance scores with optional search targets.

    Args:
        extracted_words: Words from OCR
        detected_objects: Detected objects
        filename: Video filename
        color_rgb: RGB color values
        search_keywords: Optional search keywords
        target_objects: Optional target objects
        target_color: Optional target color
        weights: Optional custom weights for score components

    Returns:
        Dict containing individual and overall scores
    """
    # Default weights
    if weights is None:
        weights = {
            'text': 0.4,
            'object': 0.35,
            'color': 0.25
        }

    # Calculate individual scores
    text_score = calculate_text_relevance_score(
        extracted_words, detected_objects, filename, search_keywords
    )

    object_score = calculate_object_relevance_score(
        detected_objects, target_objects
    )

    color_score = calculate_color_relevance_score(
        color_rgb, target_color
    )

    # Calculate weighted overall score
    overall_score = (
            text_score * weights['text'] +
            object_score * weights['object'] +
            color_score * weights['color']
    )

    return {
        'text_relevance_score': text_score,
        'object_relevance_score': object_score,
        'color_relevance_score': color_score,
        'overall_relevance_score': min(overall_score, 1.0)
    }


def update_moment_scores(conn, moment_id: str, extracted_words: List[str],
                         detected_objects: List[str], filename: str, color_rgb: List[int],
                         search_context: Optional[Dict] = None):
    """
    Update relevance scores for a specific moment with optional search context.

    Args:
        conn: Database connection
        moment_id: Moment ID to update
        extracted_words: List of extracted words
        detected_objects: List of detected objects
        filename: Video filename
        color_rgb: RGB color values
        search_context: Optional context with search keywords, target objects, etc.
    """
    try:
        cursor = conn.cursor()

        # Extract search context if provided
        search_keywords = search_context.get('keywords') if search_context else None
        target_objects = search_context.get('objects') if search_context else None
        target_color = search_context.get('color') if search_context else None
        weights = search_context.get('weights') if search_context else None

        # Calculate comprehensive scores
        scores = calculate_comprehensive_score(
            extracted_words, detected_objects, filename, color_rgb,
            search_keywords, target_objects, target_color, weights
        )

        # Update database
        cursor.execute("""
            UPDATE video_moments 
            SET text_relevance_score = %s,
                object_relevance_score = %s,
                color_relevance_score = %s,
                overall_relevance_score = %s
            WHERE moment_id = %s
        """, (
            scores['text_relevance_score'],
            scores['object_relevance_score'],
            scores['color_relevance_score'],
            scores['overall_relevance_score'],
            moment_id
        ))

        conn.commit()

    except Exception as e:
        print(f"Error updating scores for moment {moment_id}: {e}")
        conn.rollback()


def update_all_moment_scores(conn, search_context: Optional[Dict] = None):
    """
    Update relevance scores for all moments in the database.

    Args:
        conn: Database connection
        search_context: Optional search context for targeted scoring
    """
    try:
        cursor = conn.cursor()

        # Get all moments
        cursor.execute("""
            SELECT moment_id, extracted_search_words, detected_object_names, 
                   v.original_filename, average_color_rgb
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
            WHERE m.extraction_success = true
        """)

        moments = cursor.fetchall()
        print(f"Updating scores for {len(moments)} moments...")

        updated_count = 0
        for moment in moments:
            moment_id = moment[0]
            extracted_words = parse_json_field(moment[1]) if moment[1] else []
            detected_objects = parse_json_field(moment[2]) if moment[2] else []
            filename = moment[3] or ""
            color_rgb = parse_json_field(moment[4]) if moment[4] else [0, 0, 0]

            update_moment_scores(conn, moment_id, extracted_words,
                                 detected_objects, filename, color_rgb, search_context)
            updated_count += 1

            if updated_count % 100 == 0:
                print(f"Updated {updated_count}/{len(moments)} moments...")

        print(f"Score update completed! Updated {updated_count} moments.")

    except Exception as e:
        print(f"Error updating all scores: {e}")
        conn.rollback()
