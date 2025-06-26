# --- START OF FILE vector_math.py ---

import numpy as np
from numpy.typing import ArrayLike # Type hints for arrays

def calculate_squared_l2_distance(vec_a: ArrayLike, vec_b: ArrayLike) -> float:
    """Calculates the squared Euclidean (L2) distance between two vectors."""
    np_a = np.array(vec_a)
    np_b = np.array(vec_b)
    # Sum of squared differences between corresponding elements
    return np.sum((np_a - np_b) ** 2)
    # Alternative using norm: return np.linalg.norm(np_a - np_b) ** 2


def calculate_cosine_similarity_distance(vec_a: ArrayLike, vec_b: ArrayLike) -> float:
    """
    Calculates the cosine distance between two vectors.
    Cosine distance is 1 minus cosine similarity. It measures the angle between vectors.
    """
    # Add a small value to the norm to prevent division by zero if a vector is all zeros
    NORM_EPS = 1e-8
    np_a = np.array(vec_a)
    np_b = np.array(vec_b)

    dot_product = np.dot(np_a, np_b)
    norm_a = np.linalg.norm(np_a)
    norm_b = np.linalg.norm(np_b)

    # Cosine similarity ranges from -1 (opposite) to 1 (identical direction). 0 is orthogonal.
    cosine_sim = dot_product / (norm_a * norm_b + NORM_EPS)

    # Cosine distance ranges from 0 (identical) to 2 (opposite). 1 is orthogonal.
    # For normalized vectors, L2 distance^2 = 2 * (1 - cosine similarity).
    # Cosine distance (1 - cosine similarity) is directly used by pgvector <=> operator.
    return 1 - cosine_sim

# --- END OF FILE vector_math.py ---