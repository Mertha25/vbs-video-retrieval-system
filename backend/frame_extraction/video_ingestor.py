# --- START OF FILE video_ingestor.py ---

import os
import json
import time
from PIL import Image # Need this type
from datetime import datetime # Need this type
import shutil # Needed for deleting folders

# Import functions and settings from our modules
from settings import (
    DATASET_ROOT_DIR, ORIGINAL_VIDEO_FILENAME, EXTRACTED_FEATURES_JSON_FILENAME,
    KEYFRAME_IMAGES_SUBDIR, ANALYZED_COMPRESSED_VIDEO_FILENAME
)
from video_processors_io import (
    get_all_video_identifiers,
    run_ffmpeg_shot_detection,
    get_video_duration_and_fps,
    select_keyframes_from_shots,
    extract_single_frame_image,
    compress_video_for_storage,
    get_file_size_bytes,
    get_current_processing_time,
    # Assuming clean_previous_analysis_files is defined in video_processors_io.py
    # and imported from there.
    clean_previous_analysis_files # <--- Import the cleanup function
)
from feature_extractors_gpu import (
    get_image_clip_embedding,
    detect_objects_with_details,
    extract_text_with_details,
    get_image_dominant_and_average_colors,
    convert_numpy_types # <--- Import the numpy converter helper
)

def analyze_and_ingest_single_video(video_id: str):
    """
    Analyzes a single video file:
    1. Cleans up previous analysis files for this video.
    2. Gets basic info (duration, FPS).
    3. Detects shot changes.
    4. Selects keyframe timestamps.
    5. Compresses the video.
    6. Records video-level metadata (size, date).
    7. Saves frame images and extracts features (embeddings, objects, text, colors) for each keyframe.
    8. Saves all extracted data and video-level info to a JSON report.
    """
    print(f"\n--- Starting analysis for video: {video_id} ---")
    start_time = time.time()

    # --- Define File Paths ---
    video_dir_path = os.path.join(DATASET_ROOT_DIR, video_id)
    original_video_filename = ORIGINAL_VIDEO_FILENAME.format(video_id)
    original_video_path = os.path.join(video_dir_path, original_video_filename)

    ffmpeg_log_path = os.path.join(video_dir_path, f'{video_id}_ffmpeg_shot_log.txt')
    extracted_data_path = os.path.join(video_dir_path, EXTRACTED_FEATURES_JSON_FILENAME)
    # Full path for saving keyframe images directory
    keyframe_images_save_dir_full = os.path.join(video_dir_path, KEYFRAME_IMAGES_SUBDIR)
    compressed_video_filename = ANALYZED_COMPRESSED_VIDEO_FILENAME
    compressed_video_path = os.path.join(video_dir_path, compressed_video_filename)


    # --- Step 1: Clean up previous analysis files ---
    # Call the cleanup function at the very beginning
    print(f"  Cleaning up previous analysis files for {video_id}...")
    # Passing necessary paths to the cleanup function
    clean_previous_analysis_files(video_dir_path, extracted_data_path, compressed_video_path, keyframe_images_save_dir_full, ffmpeg_log_path)
    print("  Cleanup complete.")


    # --- Pre-checks ---
    # Re-check original video exists after potential cleanup (unlikely to delete original, but safe)
    if not os.path.exists(original_video_path):
        print(f"Error: Original video file not found at {original_video_path}. Skipping.")
        # Use the error reporting function
        create_error_report(video_id, original_video_filename, extracted_data_path, "Original video file not found.")
        return # Stop processing this video

    # Create the directory for saving keyframe images BEFORE attempting to save images
    try:
        os.makedirs(keyframe_images_save_dir_full, exist_ok=True)
    except Exception as e:
        print(f"Error creating keyframe images directory {keyframe_images_save_dir_full}: {e}. Analysis will continue, but images might not save.")


    # --- Step 2: Get Video Info and Shot Boundaries ---
    print(f"  Getting video duration and FPS...")
    video_duration, fps = get_video_duration_and_fps(original_video_path)
    if video_duration <= 0 or fps <= 0:
        print(f"Error: Could not get valid duration or FPS for {video_id}. Skipping analysis.")
        create_error_report(video_id, original_video_filename, extracted_data_path, f"Could not get duration/FPS. Duration={video_duration}, FPS={fps}")
        return


    print(f"  Running shot detection...")
    try:
        shot_boundary_timestamps = run_ffmpeg_shot_detection(original_video_path, ffmpeg_log_path)
        # Note: shot_boundary_timestamps includes 0.0
    except Exception as e:
        print(f"Error during shot detection for {video_id}: {e}. Skipping analysis.")
        create_error_report(video_id, original_video_filename, extracted_data_path, f"Shot detection failed: {e}")
        return


    # --- Step 3: Select Keyframe Timestamps ---
    print(f"  Selecting keyframe timestamps...")
    try:
        # We pass duration and fps to help select_keyframes_from_shots
        keyframe_timestamps_list = select_keyframes_from_shots(shot_boundary_timestamps, video_duration, fps)
    except Exception as e:
         print(f"Error selecting keyframes for {video_id}: {e}. Skipping analysis.")
         create_error_report(video_id, original_video_filename, extracted_data_path, f"Keyframe selection failed: {e}")
         return


    if not keyframe_timestamps_list:
        print(f"Warning: No keyframes selected for video {video_id}. Skipping feature extraction.")
        # Still create the JSON report with basic video info even if no keyframes
        video_analysis_summary = {
            'video_id': video_id,
            'original_filename': original_video_filename,
            'compressed_filename': compressed_video_filename, # This filename will be in the report
            'duration_seconds': video_duration,
            'fps': fps,
            'compressed_file_size_bytes': 0, # Will update after compression attempt
            'processing_date_utc': get_current_processing_time().isoformat(), # Store date/time
            'scene_change_timestamps': shot_boundary_timestamps,
            'keyframes_analyzed_count': 0,
            'analyzed_keyframes': [], # Empty list if no keyframes
            'analysis_status': 'completed_no_keyframes',
            'error_message': None
        }
        # Apply numpy conversion just in case (though likely not needed for this structure)
        cleaned_summary = convert_numpy_types(video_analysis_summary)
        try:
            with open(extracted_data_path, 'w', encoding='utf-8') as f:
                json.dump(cleaned_summary, f, indent=4)
            print(f"  Saved analysis report (no keyframes) to: {extracted_data_path}")
        except Exception as e:
            print(f"  Error saving analysis report JSON (no keyframes) for {video_id}: {e}")
            # If even this minimal report fails, try the error report function
            create_error_report(video_id, original_video_filename, extracted_data_path, f"Failed to save no-keyframe report: {e}")

        return # Stop processing this video


    # --- Step 4: Compress Video ---
    # Do compression before feature extraction as it might take time
    print(f"  Compressing video: {video_id}...")
    try:
        compress_video_for_storage(original_video_path, compressed_video_path)
    except Exception as e:
         print(f"  Error during video compression for {video_id}: {e}")
         # Compression error is not critical, continue processing but log it


    # --- Step 5: Get Compressed File Size ---
    compressed_file_size = get_file_size_bytes(compressed_video_path)
    print(f"  Compressed video size: {compressed_file_size} bytes.")


    # --- Step 6: Extract Features from Keyframes and Save Images ---
    print(f"  Extracting features from {len(keyframe_timestamps_list)} keyframes and saving images...")
    analyzed_keyframes_data = [] # List to store data for each processed keyframe

    # Process each selected timestamp
    for i, timestamp in enumerate(keyframe_timestamps_list):
        # Ensure timestamp is within bounds
        max_timestamp = video_duration - (1.0/fps if fps > 0 else 0)
        timestamp = max(0.0, min(timestamp, max_timestamp))

        print(f"    Processing keyframe {i+1}/{len(keyframe_timestamps_list)} at {timestamp:.2f}s...")

        # Create a unique identifier for this specific frame within the video
        # Using timestamp in milliseconds provides a very high chance of uniqueness
        frame_unique_id = f"frame_{int(timestamp * 1000):012d}" # e.g., frame_000000001234 (unique within video)

        # Define paths for the keyframe image
        frame_img_filename = f'{frame_unique_id}.jpeg'
        # Relative path from DATASET_ROOT_DIR to the image file
        keyframe_image_path_relative_to_dataset_root = os.path.join(video_id, KEYFRAME_IMAGES_SUBDIR, frame_img_filename)
        frame_image_path_full = os.path.join(DATASET_ROOT_DIR, keyframe_image_path_relative_to_dataset_root) # Full path to save the file


        # Extract the frame image using OpenCV
        frame_image = extract_single_frame_image(original_video_path, timestamp)

        # --- Save the Frame Image (If extracted successfully) ---
        image_save_success = False
        if frame_image is not None: # Only try to save if we got the image
             try:
                 # The directory for saving keyframe images was already created at the start
                 # Save the image using Pillow
                 frame_image.save(frame_image_path_full)
                 image_save_success = True
                 # print(f"    Saved frame image to {frame_image_path_full}") # Optional detailed print
             except Exception as e:
                 print(f"    Warning: Could not save frame image {frame_image_path_full}: {e}")
                 image_save_success = False


        # --- Extract Features (If image was extracted) ---
        # Initialize all feature variables to defaults in case extraction fails
        clip_embedding = None
        detected_objects_detailed = []
        detected_object_names_list = []
        extracted_text_detailed = []
        extracted_words_list = []
        dominant_colors_info = []
        average_color_rgb = [0, 0, 0] # Default to black


        if frame_image is not None: # Only try to extract features if we got the image
            try:
                # Get image embedding (CLIP)
                clip_embedding = get_image_clip_embedding(frame_image)
                # clip_embedding is None or list[float]

                # Detect objects (YOLO) - returns list of dicts with potential numpy types
                detected_objects_detailed = detect_objects_with_details(frame_image)
                # Create a simple list of just object names for easier searching
                detected_object_names_list = sorted(list(set([obj['name'].lower() for obj in detected_objects_detailed]))) # Get unique names, lowercase, sorted


                # Extract text (EasyOCR) - returns list of dicts with potential numpy types
                extracted_text_detailed = extract_text_with_details(frame_image)
                # Get a simple list of unique lowercase words for easier searching
                all_words = []
                for item in extracted_text_detailed:
                    if isinstance(item, dict) and 'text' in item and isinstance(item['text'], str):
                         # Basic split into words and clean punctuation
                        words = [word.strip('.,!?;:"\'()[]{}\n').lower() for word in item['text'].split()]
                        all_words.extend([word for word in words if word]) # Add non-empty words

                extracted_words_list = sorted(list(set(all_words))) # Get unique lowercase words, sorted


                # Get dominant and average colors - returns list of dicts and list[int], potentially with numpy types
                dominant_colors_info, average_color_rgb = get_image_dominant_and_average_colors(frame_image)


            except Exception as e:
                print(f"    An error occurred during feature extraction for frame {frame_unique_id}: {e}. This frame might have incomplete data.")
                # Continue processing, but acknowledge error for this frame


            # --- Compile Detailed Features Dictionary ---
            # This dictionary holds data that will go into the JSONB column in the DB
            # It contains potentially nested structures and numpy types
            detailed_features_dict = {
                 'detected_objects_detailed': detected_objects_detailed, # Full list from detector
                 'extracted_text_detailed': extracted_text_detailed,     # Full list from OCR
                 'dominant_colors_info': dominant_colors_info            # Full list of dominant colors
            }

            # --- NEW: Convert numpy types in the detailed features dictionary ---
            # Use the helper function to convert any numpy types to standard Python types
            # This needs to be done *before* saving to JSON later and before putting into moment_data_entry
            # as moment_data_entry also gets saved to JSON eventually (as part of the main report).
            # Call convert_numpy_types on the dictionary itself
            cleaned_detailed_features = convert_numpy_types(detailed_features_dict)


            # --- Store Data for this Keyframe (Moment) ---
            # This dictionary holds data for one row in the 'video_moments' table
            moment_data_entry = {
                # The primary key for the database entry
                # Using video_id + frame_unique_id ensures uniqueness across all videos
                'moment_id': f"{video_id}_{frame_unique_id}", # e.g., 00001_frame_000000001234

                'video_id': video_id,
                'timestamp_seconds': timestamp,

                # --- NEW: Add the frame_unique_id to the dictionary using the key 'frame_identifier' ---
                # This key is expected by db_uploader.py
                'frame_identifier': frame_unique_id, # <-- ADD THIS LINE

                # Store the relative path to the image from the DATASET_ROOT_DIR
                # This path will be used by the API server to serve the image file
                'keyframe_image_path': keyframe_image_path_relative_to_dataset_root if image_save_success else None, # Store path only if save was successful

                'clip_embedding': clip_embedding, # Can be None if extraction failed

                # Simple feature lists/values for search filtering and scoring
                'detected_object_names': detected_object_names_list,
                'extracted_search_words': extracted_words_list,
                'average_color_rgb': average_color_rgb, # This is already a list of ints from get_image_dominant_and_average_colors

                # The detailed features dictionary (with numpy types converted) for the JSONB column
                'detailed_features': cleaned_detailed_features # Use the cleaned dictionary here
            }

            analyzed_keyframes_data.append(moment_data_entry)

        else:
             # This case happens if extract_single_frame_image returns None
             print(f"    Skipped feature extraction, saving, and data compilation for frame at {timestamp:.2f}s as image extraction failed.")
             # Optionally, you could create a minimal entry here just with video/timestamp/status info


    # --- Step 7: Compile All Video-level and Keyframe Data for the Report ---
    # Get the current time *after* all processing for this video is done
    processing_completion_time = get_current_processing_time()

    print(f"  Finished frame processing for {video_id}. Compiling report.")
    video_analysis_report = {
        'video_id': video_id,
        'original_filename': original_video_filename,
        'compressed_filename': compressed_video_filename,
        'duration_seconds': video_duration,
        'fps': fps,
        'compressed_file_size_bytes': compressed_file_size, # Add compressed file size
        'processing_date_utc': processing_completion_time.isoformat(), # Store UTC date/time in ISO format (e.g., 2023-10-27T10:00:00.000000+00:00)
        'scene_change_timestamps': shot_boundary_timestamps,
        'keyframes_analyzed_count': len(analyzed_keyframes_data),
        'analyzed_keyframes': analyzed_keyframes_data, # List of all keyframe (moment) data entries
        'analysis_status': 'completed_with_keyframes' if analyzed_keyframes_data else 'completed_no_keyframes',
        'error_message': None # Add a field for overall video error if needed
    }

    # --- NEW: Apply numpy type conversion to the entire report dictionary before saving ---
    # This ensures ALL numbers in the final JSON are standard Python types
    # Apply convert_numpy_types to the top-level report dictionary
    cleaned_analysis_report = convert_numpy_types(video_analysis_report)


    # --- Step 8: Save the Analysis Report (JSON) ---
    try:
        # Save the compiled and cleaned data to the JSON report file
        # Use json.dump for writing dictionary directly, using the cleaned data
        with open(extracted_data_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_analysis_report, f, indent=4) # Use json.dump for writing dictionary directly
        print(f"  Successfully saved analysis report to: {extracted_data_path}")
    except Exception as e:
        print(f"  Error saving analysis report JSON for {video_id}: {e}")
        # Create a minimal error report if saving the main report fails unexpectedly
        create_error_report(video_id, original_video_filename, extracted_data_path, f"Failed to save main report JSON: {e}")


    end_time = time.time()
    print(f"--- Finished analysis for video: {video_id} in {end_time - start_time:.2f} seconds ---")


# Helper function to create a minimal error report if analysis fails early
# This function also needs to use the numpy type converter before saving
def create_error_report(video_id: str, original_video_filename: str, report_path: str, error_msg: str):
    """Creates a minimal JSON report indicating that analysis failed."""
    print(f"Creating error report for {video_id}: {error_msg}")
    # Ensure parent directory exists before writing report
    report_dir = os.path.dirname(report_path)
    if report_dir and not os.path.exists(report_dir):
        try:
            os.makedirs(report_dir, exist_ok=True) # Use exist_ok=True for safety
        except Exception as e:
            print(f"Error creating directory for error report {report_dir}: {e}")

    error_report_data = {
        'video_id': video_id,
        'original_filename': original_video_filename,
        'compressed_filename': ANALYZED_COMPRESSED_VIDEO_FILENAME,
        'duration_seconds': 0, # Use default values as analysis failed early
        'fps': 0,
        'compressed_file_size_bytes': 0,
        'processing_date_utc': get_current_processing_time().isoformat(),
        'scene_change_timestamps': [0.0], # Indicate start time
        'keyframes_analyzed_count': 0,
        'analyzed_keyframes': [], # Empty list
        'analysis_status': 'failed', # Status indicates failure
        'error_message': error_msg # Store the error message
    }
    try:
        # Apply numpy type conversion just in case the error message or other data contains numpy types
        cleaned_error_report = convert_numpy_types(error_report_data)
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(cleaned_error_report, f, indent=4)
        print(f"  Saved error report to: {report_path}")
    except Exception as e:
        # If even saving the error report fails... print and give up
        print(f"  CRITICAL ERROR: Could not save error report JSON for {video_id}: {e}")


# --- Add Cleanup Function (Included here for completeness of the video_ingestor logic block) ---
# NOTE: In a real project, this function should ideally be in video_processors_io.py
# and imported from there. But for providing the full code in one response,
# I'll include the definition here. If you saved the previous video_processors_io.py,
# you might have this function twice if you copy this block.
# Ensure you only have *one* definition of clean_previous_analysis_files in your project!
# If it's already in video_processors_io.py and imported above, KEEP that import and DELETE this function definition here.
# If it's NOT in video_processors_io.py, KEEP this definition here for now.
# **Important**: Based on the traceback, clean_previous_analysis_files IS expected to be imported from video_processors_io.py
# So, you should DELETE this definition from video_ingestor.py and ensure it's only in video_processors_io.py
# I will include it here commented out, for reference, but the import should be the source.
#
# def clean_previous_analysis_files(video_dir_path: str, report_path: str, compressed_video_path: str, keyframe_images_dir_full: str, ffmpeg_log_path: str):
#     """
#     Deletes previously created analysis output files and directories for a given video.
#     Takes full paths as input.
#     """
#     # print(f"  Attempting to clean old analysis files in {video_dir_path}...") # Optional detailed print
#
#     files_to_delete = [report_path, compressed_video_path, ffmpeg_log_path]
#     dirs_to_delete = [keyframe_images_dir_full]
#
#     for file_path in files_to_delete:
#         if os.path.exists(file_path):
#             try: os.remove(file_path)
#             except Exception as e: print(f"    Warning: Could not delete file {file_path}: {e}")
#
#     for dir_path in dirs_to_delete:
#         if os.path.exists(dir_path):
#             try: shutil.rmtree(dir_path)
#             except Exception as e: print(f"    Warning: Could not delete directory {dir_path}: {e}")
#
#     # print("  Old analysis file cleanup finished.") # Optional detailed print


# --- Main execution block ---
if __name__ == '__main__':
    print("Starting video analysis batch processing...")
    print(f"Scanning for videos in: {DATASET_ROOT_DIR}")

    # Get the list of videos to process based on folders in the dataset directory
    video_identifiers = get_all_video_identifiers(DATASET_ROOT_DIR)

    if not video_identifiers:
        print("No valid video directories found in the dataset root directory.")
        print(f"Please ensure your video folders (e.g., '00001') are inside '{DATASET_ROOT_DIR}' and contain '{ORIGINAL_VIDEO_FILENAME.format('video_id')}' file.")
    else:
        print(f"Found {len(video_identifiers)} videos to analyze.")

        # Loop through each video identifier and start the analysis process
        total_videos = len(video_identifiers)
        for index, vid in enumerate(video_identifiers):
            # print separators for clarity
            print("\n" + "="*60)
            print(f"Processing video {index + 1}/{total_videos}: {vid}")
            print("="*60)
            # Wrap analysis in a try/except to catch errors per video and continue with the next
            try:
                analyze_and_ingest_single_video(vid)
            except Exception as e:
                print(f"\n" + "="*60)
                print(f"FATAL ERROR processing video {vid}: {e}")
                print(f"Skipping video {vid}.")
                print("="*60 + "\n")
                # Attempt to create an error report for this video if it wasn't created already
                report_path_for_error = os.path.join(DATASET_ROOT_DIR, vid, EXTRACTED_FEATURES_JSON_FILENAME)
                original_video_filename_for_error = ORIGINAL_VIDEO_FILENAME.format(vid)
                # Only create a new error report if the main report doesn't exist (meaning it failed early)
                # If the main report exists, the error might have happened late, and the error is logged there.
                # Pass full path to create_error_report
                create_error_report(vid, original_video_filename_for_error, report_path_for_error, f"Fatal error during analysis: {e}") # Create error report on fatal exception


        print("\n" + "="*60)
        print("Video analysis batch processing completed.")
        print("="*60)

# --- END OF FILE video_ingestor.py ---