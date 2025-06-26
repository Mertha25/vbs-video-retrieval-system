# --- START OF FILE feature_extractors_gpu.py ---

import os
import torch
import clip
from ultralytics import YOLO
import easyocr # <-- Switched from paddleocr
import numpy as np
from PIL import Image
from typing import Union, List, Dict, Tuple, Any

# Import settings
from settings import MINIMUM_OBJECT_DETECTION_CONFIDENCE, MINIMUM_TEXT_EXTRACTION_CONFIDENCE, NUMBER_OF_DOMINANT_COLORS

# --- Model Loading ---
# Determine the device to use (GPU if available, otherwise CPU)
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {DEVICE}")

# Load CLIP model (for image/text embeddings)
print("Loading CLIP model...")
try:
    clip_model, clip_preprocess = clip.load("ViT-L/14", device=DEVICE)
    clip_model.eval() # Set model to evaluation mode
    print("CLIP model loaded.")
except Exception as e:
    print(f"Error loading CLIP model: {e}")
    print("Please ensure PyTorch is installed correctly and CUDA is available if using GPU.")
    clip_model = None
    clip_preprocess = None


# Load YOLO-OID model (for object detection with ~600 classes)
print("Loading YOLO-OID model...")
# You need the YOLOv7-OID model file. Place it in the same folder as your scripts, or provide a full path.
# We are using a standard YOLOv8 model here for compatibility, as decided earlier.
YOLO_OID_MODEL_PATH = 'yolov8l.pt' # <--- Using a standard YOLOv8 model for now
try:
    object_detection_model = YOLO(YOLO_OID_MODEL_PATH)
    object_detection_model.to(DEVICE) # Move model to the selected device
    print(f"YOLO model loaded.")
except Exception as e:
    print(f"Error loading YOLO model from {YOLO_OID_MODEL_PATH}: {e}")
    print("Please ensure the model file exists (if using a local file) and Ultralytics can load it.")
    object_detection_model = None


# Load EasyOCR reader (for text extraction) <-- Switched from PaddleOCR
print("Loading EasyOCR reader...")
# EasyOCR might download models on first run.
# ['en'] specifies English. gpu=True uses GPU if available.
try:
    text_recognition_reader = easyocr.Reader(['en'], gpu=(DEVICE == "cuda")) # EasyOCR handles the GPU check
    print("EasyOCR reader loaded.")
except Exception as e:
    print(f"Error loading EasyOCR reader: {e}")
    print("Please ensure EasyOCR is installed and models can be downloaded.")
    text_recognition_reader = None

# Helper function to convert numpy types to standard Python types for JSON serialization
def convert_numpy_types(obj):
    """
    Recursively converts numpy int/float/bool types within a dictionary or list
    to standard Python int/float/bool types, making them JSON serializable.
    """
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, dict):
        # Use a dictionary comprehension to process values recursively
        return {k: convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        # Use a list comprehension to process elements recursively
        return [convert_numpy_types(elem) for elem in obj]
    # Add handling for numpy arrays if they might appear
    elif isinstance(obj, np.ndarray):
        # Convert numpy arrays to lists (potentially nested)
        return obj.tolist()
    else:
        # Return the object unchanged if it's not a numpy type or container
        return obj


# --- Feature Extraction Functions ---

def get_image_clip_embedding(image: Union[Image.Image, os.PathLike]) -> Union[List[float], None]:
    """Gets the CLIP embedding vector for an image."""
    if clip_model is None or clip_preprocess is None:
        return None
    try:
        if isinstance(image, os.PathLike):
            image = Image.open(image).convert("RGB")
        else:
            image = image.convert("RGB")
        processed_image = clip_preprocess(image).unsqueeze(0).to(DEVICE)
        with torch.no_grad():
            image_features = clip_model.encode_image(processed_image)
        norm = image_features.norm(dim=-1, keepdim=True)
        if norm > 1e-6:
             image_features /= norm
        else:
             print("Warning: CLIP image embedding norm is near zero. Returning zero vector.")
             return [0.0] * 768
        return image_features.squeeze(0).tolist()
    except Exception as e:
        print(f"Error generating image CLIP embedding: {e}")
        return None


def get_text_clip_embedding(text: str) -> Union[List[float], None]:
    """Gets the CLIP embedding vector for text."""
    if clip_model is None:
        return None
    try:
        processed_text = clip.tokenize([text]).to(DEVICE)
        with torch.no_grad():
            text_features = clip_model.encode_text(processed_text)
        norm = text_features.norm(dim=-1, keepdim=True)
        if norm > 1e-6:
             text_features /= norm
        else:
            print("Warning: CLIP text embedding norm is near zero. Returning zero vector.")
            return [0.0] * 768
        return text_features.squeeze(0).tolist()
    except Exception as e:
        print(f"Error generating text CLIP embedding: {e}")
        return None


def detect_objects_with_details(image: Image.Image) -> List[Dict[str, Any]]:
    """
    Uses the YOLO model to find objects, their confidence scores, and bounding boxes.
    Returns a list of dictionaries. Filters results by MINIMUM_OBJECT_DETECTION_CONFIDENCE.
    Numbers in the output (confidence, box coords) will be converted to standard types by convert_numpy_types.
    """
    if object_detection_model is None:
        return []

    try:
        # Running on the correct device is handled by moving the model.
        results = object_detection_model(source=image, save=False, verbose=False)[0]

        detected_list = []
        if results.boxes is not None: # Check if any boxes were detected
             # Iterate through each detected box/object
             for box in results.boxes:
                 confidence = box.conf.item() # Confidence score (as a standard Python number)
                 class_id = int(box.cls.item()) # Class ID (as an integer)

                 # Check confidence threshold and if the class name is known by the model
                 if confidence >= MINIMUM_OBJECT_DETECTION_CONFIDENCE and class_id in results.names:
                     object_name = results.names[class_id]
                     # Get bounding box coordinates [x1, y1, x2, y2] as a list of standard Python numbers
                     bbox = box.xyxy[0].tolist()

                     detected_list.append({
                         'name': object_name,
                         'confidence': confidence,
                         'box': bbox
                     })

        return detected_list

    except Exception as e:
        print(f"Error during object detection: {e}")
        return []


def extract_text_with_details(image: Image.Image) -> List[Dict[str, Any]]:
    """
    Uses EasyOCR to extract text from the image, including confidence and bounding boxes.
    Returns a list of dictionaries. Filters results by MINIMUM_TEXT_EXTRACTION_CONFIDENCE.
    Numbers in the output (confidence, box coords) will be converted to standard types by convert_numpy_types.
    """
    if text_recognition_reader is None:
        return []

    try:
        # EasyOCR needs a NumPy array, not PIL Image directly, and expects BGR or RGB
        image_np = np.array(image.convert("RGB"))


        # EasyOCR readtext result format: [([box_points], 'text_string', confidence_float), ...]
        # box_points is a list of 4 [x, y] points
        raw_results = text_recognition_reader.readtext(image_np)

        extracted_list = []
        # raw_results is already the list of detected text blocks
        for result in raw_results:
             if len(result) == 3: # Ensure result has box, text, confidence
                 box_points_raw = result[0] # List of [x, y] points (often 4 points for a quad)
                 text_string = result[1]
                 confidence = float(result[2]) # Ensure confidence is a standard float

                 # Check confidence threshold
                 if confidence >= MINIMUM_TEXT_EXTRACTION_CONFIDENCE:
                     extracted_list.append({
                         'text': text_string,
                         'confidence': confidence,
                         'box_points': box_points_raw # Store the list of [x, y] points
                     })
             else:
                 print(f"Warning: Unexpected result format from EasyOCR: {result}")


        return extracted_list

    except Exception as e:
        print(f"Error during text extraction: {e}")
        return []

def get_image_dominant_and_average_colors(image: Image.Image) -> Tuple[List[Dict[str, Any]], List[int]]:
    """
    Finds the most frequent colors and the overall average color in the image.
    Returns a tuple: (list of dominant color dicts, list of average RGB [R, G, B]).
    Numbers in the output (count, percentage, average color values) will be converted to standard types by convert_numpy_types.
    """
    try:
        # Ensure image is RGB
        image_rgb = image.convert("RGB")

        # --- Calculate Average Color ---
        # Get image data as a numpy array
        image_np = np.array(image_rgb)
        # Calculate the mean of the R, G, B channels across all pixels
        average_color_np = np.mean(image_np, axis=(0, 1))
        # Convert to list of integers (round the float values) and clamp to 0-255
        average_color_rgb = [int(round(c)) for c in average_color_np]
        average_color_rgb = [max(0, min(255, c)) for c in average_color_rgb]


        # --- Find Dominant Colors ---
        # Get all unique colors and their counts.
        unique_colors_with_counts = image_rgb.getcolors(image_rgb.width * image_rgb.height)

        # Store original size for percentage calculation later if needed
        original_total_pixels = image_rgb.width * image_rgb.height
        sampled_total_pixels = None # Will store pixel count if we sample

        if unique_colors_with_counts is None:
            # If direct count failed, resize the image and try again
            small_image = image_rgb.copy()
            small_image.thumbnail((200, 200)) # Resize to a smaller size (e.g., 200x200)
            sampled_total_pixels = small_image.width * small_image.height
            unique_colors_with_counts = small_image.getcolors(sampled_total_pixels)

            if unique_colors_with_counts is None:
                # If it still fails after sampling, log a warning and return empty dominant colors
                print("Warning: Still too many unique colors after sampling, returning empty list for dominant colors.")
                return [], average_color_rgb # Return what we have


        # Sort unique colors by count (most frequent first) and take the top N
        # Use NUMBER_OF_DOMINANT_COLORS from settings
        sorted_colors = sorted(unique_colors_with_counts, key=lambda item: item[0], reverse=True)[:NUMBER_OF_DOMINANT_COLORS]

        # Use the pixel count from the source used for counting
        total_pixels_for_percentage = sampled_total_pixels if sampled_total_pixels is not None else original_total_pixels


        dominant_color_list = []
        if total_pixels_for_percentage > 0: # Avoid division by zero
             for count, color_rgb_tuple in sorted_colors:
                 dominant_color_list.append({
                     'color': list(color_rgb_tuple), # Convert tuple to list [R, G, B]
                     'count': count, # Will be converted by convert_numpy_types
                     'percentage': (count / total_pixels_for_percentage) * 100.0 # Will be converted by convert_numpy_types
                 })


        # Convert average color to list of standard ints (already mostly done, but safety)
        average_color_rgb = [int(c) for c in average_color_rgb] # Ensure average color is list of ints


        return dominant_color_list, average_color_rgb # Return the lists with converted types


    except Exception as e:
        print(f"Error getting dominant/average colors: {e}")
        return [], [0, 0, 0] # Return empty list and black as default on error


# --- END OF FILE feature_extractors_gpu.py ---