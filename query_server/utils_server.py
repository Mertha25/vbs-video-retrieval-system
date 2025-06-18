import json
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

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
        return float(cosine_similarity(emb1, emb2)[0][0])
    except:
        return 0.0
