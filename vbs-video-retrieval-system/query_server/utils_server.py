import json
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import re
import string
from typing import List

def parse_json_field(field_value):
    """Parse a JSON string or return as-is if already parsed."""
    if not field_value:
        return []
    if isinstance(field_value, str):
        try:
            return json.loads(field_value)
        except:
            return []
    return field_value

def color_distance(color1, color2):
    """Weighted Euclidean distance in RGB space based on human perception."""
    if not color1 or not color2 or len(color1) != 3 or len(color2) != 3:
        return float('inf')
    r_diff = (color1[0] - color2[0]) * 0.3
    g_diff = (color1[1] - color2[1]) * 0.59
    b_diff = (color1[2] - color2[2]) * 0.11
    return (r_diff**2 + g_diff**2 + b_diff**2)**0.5

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

def calculate_text_relevance_score(extracted_words: List[str], detected_objects: List[str], filename: str) -> float:
    """
    Calculate text relevance score based on word frequency and importance.
    
    Args:
        extracted_words: List of words from OCR
        detected_objects: List of detected objects
        filename: Video filename
        
    Returns:
        float: Relevance score between 0.0 and 1.0
    """
    if not extracted_words and not detected_objects:
        return 0.0
    
    # Define importance weights
    word_weight = 0.6  # OCR words are most important
    object_weight = 0.3  # Detected objects are important
    filename_weight = 0.1  # Filename is least important
    
    # Calculate word diversity (more unique words = higher score)
    unique_words = len(set(extracted_words)) if extracted_words else 0
    total_words = len(extracted_words) if extracted_words else 0
    
    # Calculate object diversity
    unique_objects = len(set(detected_objects)) if detected_objects else 0
    total_objects = len(detected_objects) if detected_objects else 0
    
    # Calculate filename relevance (simple check for meaningful content)
    filename_relevance = 0.0
    if filename and len(filename) > 5:  # More than just numbers
        filename_relevance = 0.5
    
    # Calculate weighted score
    word_score = (unique_words / max(total_words, 1)) * word_weight if total_words > 0 else 0
    object_score = (unique_objects / max(total_objects, 1)) * object_weight if total_objects > 0 else 0
    
    total_score = word_score + object_score + (filename_relevance * filename_weight)
    
    return min(total_score, 1.0)  # Cap at 1.0

def calculate_object_relevance_score(detected_objects: List[str]) -> float:
    """
    Calculate object relevance score based on object diversity and confidence.
    
    Args:
        detected_objects: List of detected objects
        
    Returns:
        float: Relevance score between 0.0 and 1.0
    """
    if not detected_objects:
        return 0.0
    
    # More diverse objects = higher score
    unique_objects = len(set(detected_objects))
    total_objects = len(detected_objects)
    
    # Calculate diversity score
    diversity_score = unique_objects / max(total_objects, 1)
    
    # Bonus for having multiple objects (interaction potential)
    interaction_bonus = min(unique_objects * 0.1, 0.3)
    
    return min(diversity_score + interaction_bonus, 1.0)

def calculate_color_relevance_score(color_rgb: List[int]) -> float:
    """
    Calculate color relevance score based on color characteristics.
    
    Args:
        color_rgb: RGB color values [r, g, b]
        
    Returns:
        float: Relevance score between 0.0 and 1.0
    """
    if not color_rgb or len(color_rgb) != 3:
        return 0.0
    
    r, g, b = color_rgb
    
    # Calculate color saturation (more saturated = more interesting)
    max_val = max(r, g, b)
    min_val = min(r, g, b)
    
    if max_val == 0:
        return 0.0
    
    saturation = (max_val - min_val) / max_val
    
    # Calculate brightness (avoid too dark or too bright)
    brightness = (r + g + b) / (3 * 255)
    
    # Prefer medium brightness and high saturation
    brightness_score = 1.0 - abs(brightness - 0.5) * 2  # Peak at 0.5 brightness
    saturation_score = saturation
    
    # Combined score
    return (brightness_score * 0.4 + saturation_score * 0.6)

def update_moment_scores(conn, moment_id: str, extracted_words: List[str], 
                        detected_objects: List[str], filename: str, color_rgb: List[int]):
    """
    Update relevance scores for a specific moment in the database.
    
    Args:
        conn: Database connection
        moment_id: Moment ID to update
        extracted_words: List of extracted words
        detected_objects: List of detected objects
        filename: Video filename
        color_rgb: RGB color values
    """
    try:
        cursor = conn.cursor()
        
        # Calculate individual scores
        text_score = calculate_text_relevance_score(extracted_words, detected_objects, filename)
        object_score = calculate_object_relevance_score(detected_objects)
        color_score = calculate_color_relevance_score(color_rgb)
        
        # Calculate overall score (weighted average)
        overall_score = (text_score * 0.5 + object_score * 0.3 + color_score * 0.2)
        
        # Update database
        cursor.execute("""
            UPDATE video_moments 
            SET text_relevance_score = %s,
                object_relevance_score = %s,
                color_relevance_score = %s,
                overall_relevance_score = %s
            WHERE moment_id = %s
        """, (text_score, object_score, color_score, overall_score, moment_id))
        
        conn.commit()
        
    except Exception as e:
        print(f"Error updating scores for moment {moment_id}: {e}")
        conn.rollback()

def update_all_moment_scores(conn):
    """
    Update relevance scores for all moments in the database.
    
    Args:
        conn: Database connection
    """
    try:
        cursor = conn.cursor()
        
        # Get all moments
        cursor.execute("""
            SELECT moment_id, extracted_search_words, detected_object_names, 
                   v.original_filename, average_color_rgb
            FROM video_moments m
            JOIN videos v ON m.video_id = v.video_id
        """)
        
        moments = cursor.fetchall()
        print(f"Updating scores for {len(moments)} moments...")
        
        for moment in moments:
            moment_id = moment[0]
            extracted_words = moment[1] or []
            detected_objects = moment[2] or []
            filename = moment[3] or ""
            color_rgb = moment[4] or [0, 0, 0]
            
            update_moment_scores(conn, moment_id, extracted_words, 
                               detected_objects, filename, color_rgb)
        
        print("Score update completed!")
        
    except Exception as e:
        print(f"Error updating all scores: {e}")
        conn.rollback()
