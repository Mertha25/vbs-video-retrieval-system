# --- START OF FILE settings.py ---

import os

# --- File and Directory Settings ---
# The main folder containing your video dataset (like V3C1-200)
# On Colab, this might be '/content/V3C1-200' or '/content/drive/MyDrive/V3C1-200'
# Using the path you provided:
DATASET_ROOT_DIR = "/content/drive/MyDrive/MyVideoProject/unique_search/V3C1-200"

# The standard filename for the original video inside each video ID folder
ORIGINAL_VIDEO_FILENAME = "{}.mp4" # Use {} as a placeholder for the video ID

# The filename for the compressed video created by the analyzer
ANALYZED_COMPRESSED_VIDEO_FILENAME = "compressed_for_web.mp4"

# The filename for the JSON file storing extracted features for a video
EXTRACTED_FEATURES_JSON_FILENAME = "video_analysis_report.json" # Updated filename from previous

# The folder inside each video folder where extracted frame images will be saved
# This folder will be created by the ingestor
KEYFRAME_IMAGES_SUBDIR = "extracted_frames"


# --- Video Processing Settings ---
# Threshold for FFMPEG shot change detection (lower = more sensitive, more shots)
SCENE_CHANGE_THRESHOLD = 1.0 # Use a float value (Keeping your lower threshold)

# Strategy for selecting keyframes from detected shots: 'middle', 'start', 'end', 'all', 'boundary'
# 'middle' selects the frame halfway between shots. 'start' gets the frame at the start of each shot.
# 'end' gets the frame at the end of each shot.
# 'all' gets middle, start, and end. 'boundary' gets just start and end.
KEYFRAME_SELECTION_STRATEGY = 'boundary' # (Keeping your change to 'boundary')

# If KEYFRAME_SELECTION_STRATEGY is 'boundary' or 'all', add a small offset to boundary frames
# to avoid potential issues right on the cut point.
KEYFRAME_BOUNDARY_OFFSET_SECONDS = 0.1

# --- NEW SETTING: Fixed Interval Keyframes ---
# If > 0, also adds keyframes at fixed intervals (e.g., 30 for every 30 seconds)
# These are added IN ADDITION to keyframes from the selection strategy.
KEYFRAME_INTERVAL_SECONDS = 30 # <--- ADDED THIS LINE: Add a keyframe every 30 seconds


# --- Feature Extraction Settings ---
# Confidence threshold for including a detected object (0.0 to 1.0)
MINIMUM_OBJECT_DETECTION_CONFIDENCE = 0.5

# Confidence threshold for including extracted text from PaddleOCR (0.0 to 1.0)
# PaddleOCR provides confidence per line/word; we'll process it.
MINIMUM_TEXT_EXTRACTION_CONFIDENCE = 0.3 # Example threshold

# Number of dominant colors to extract per frame
# Make sure this line is here and spelled correctly!
NUMBER_OF_DOMINANT_COLORS = 10 # (Keeping this line)


# --- Database Settings ---
# Connection details for your PostgreSQL database with the pgvector extension
# Expected to be running on Docker
DB_HOST = "127.0.0.1" # Or the IP address of your Docker container if needed
DB_PORT = "5432"    # Standard PostgreSQL port
DB_NAME = "videodb_creative_v2" # A new database name for this version (from our plan)
DB_USER = "admin"
DB_PASSWORD = "admin"

# The name of the table in the database where frame data will be stored
FRAME_DATA_TABLE_NAME = "video_moments" # (from our plan)


# --- Search Settings ---
# Default number of results per page for pagination
DEFAULT_ITEMS_PER_PAGE = 50

# Maximum allowed results per page
MAX_ITEMS_PER_PAGE = 200

# Default minimum CLIP vector similarity (0.0 to 1.0). Used to filter potential matches.
# Lower values mean less strict similarity.
DEFAULT_MIN_VECTOR_SIMILARITY = 0.75 # Example: only show results > 75% similar

# Default radius for color search matching (how close RGB values need to be considered a match)
DEFAULT_COLOR_MATCH_RADIUS = 25 # Max difference per R, G, B channel


# --- END OF FILE settings.py ---