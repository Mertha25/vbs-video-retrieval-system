# --- START OF FILE video_processors_io.py ---

import os
import subprocess
import cv2 as cv # Using OpenCV for efficient frame extraction
import numpy as np # For image processing
from typing import List, Tuple, Union
from PIL import Image # For image format conversion
from datetime import datetime # To get the current date/time
import time # To add delays if needed
import shutil # Needed for deleting folders (for cleanup function)


# Import settings
from settings import (
    DATASET_ROOT_DIR, ORIGINAL_VIDEO_FILENAME,
    SCENE_CHANGE_THRESHOLD, KEYFRAME_SELECTION_STRATEGY,
    KEYFRAME_BOUNDARY_OFFSET_SECONDS, ANALYZED_COMPRESSED_VIDEO_FILENAME,
    KEYFRAME_INTERVAL_SECONDS # <--- Import the new setting for interval keyframes
)

def get_all_video_identifiers(base_dir: str) -> List[str]:
    """
    Finds the unique identifiers (folder names) for all videos
    within the dataset base directory by checking for expected video files.
    """
    video_ids = []
    if not os.path.exists(base_dir):
        print(f"Error: Dataset root directory not found at {base_dir}")
        return []

    # List all items in the base directory
    all_items = os.listdir(base_dir)
    # Filter for directories and sort them
    all_items.sort() # Sort alphabetically/numerically

    print(f"Scanning directory: {base_dir}")
    for item_name in all_items:
        item_path = os.path.join(base_dir, item_name)
        # Check if the item is a directory and is not a hidden folder
        if os.path.isdir(item_path) and not item_name.startswith('.'):
            # Assume the folder name is the video identifier
            video_id = item_name
            # Check if the actual video file exists inside using the configured naming pattern
            expected_video_file = os.path.join(item_path, ORIGINAL_VIDEO_FILENAME.format(video_id))
            if os.path.exists(expected_video_file):
                 video_ids.append(video_id)
            else:
                 print(f"Warning: Directory '{video_id}' found in {base_dir}, but expected video file '{os.path.basename(expected_video_file)}' not found inside. Skipping.")


    return video_ids

def execute_ffmpeg_command(command: str) -> Tuple[int, str, str]:
    """Runs an FFMPEG command and returns its exit code, stdout, and stderr."""
    # print(f"Executing FFMPEG: {command}") # Optional: print every command
    try:
        # Use subprocess.run to get more control and capture output reliably
        # We use a short timeout in case FFMPEG hangs (e.g., corrupted file)
        process = subprocess.run(
            command,
            shell=True, # Set to True to run the command as a shell command (handles pipes etc.)
            capture_output=True, # Capture stdout and stderr
            text=True, # Decode stdout/stderr as text (Python 3.7+)
            check=False, # Don't raise CalledProcessError for non-zero exit codes
            timeout=600 # Add a timeout (e.g., 10 minutes) for safety
        )
        if process.returncode != 0:
             print(f"FFMPEG command finished with non-zero exit code {process.returncode}")
             print(f"  Stderr: {process.stderr}")
        return process.returncode, process.stdout, process.stderr
    except subprocess.TimeoutExpired:
        print(f"FFMPEG command timed out after {600} seconds.")
        return -2, "", "FFMPEG command timed out."
    except Exception as e:
        print(f"Error running FFMPEG command: {e}")
        return -1, "", str(e) # Return error code -1 and error message

def run_ffmpeg_shot_detection(video_full_path: str, output_log_path: str) -> List[float]:
    """
    Runs FFMPEG's scdet filter on a video to find shot change timestamps.
    Saves the raw output to a log file and returns the list of timestamps.
    """
    # FFMPEG command: input -> scdet filter -> output to null, capture logs
    # We redirect stderr (2) to stdout (1) to capture logs like 'lavfi.scd.time'
    # Using -nostats -loglevel 0 to reduce FFMPEG's usual verbose output, only scdet logs should appear in stdout/stderr
    ffmpeg_cmd = f'ffmpeg -i "{video_full_path}" -vf "scdet=s=0:t={SCENE_CHANGE_THRESHOLD},showinfo" -f null - -nostats -loglevel 0 2>&1'

    # print(f"Running FFMPEG scdet: {ffmpeg_cmd}") # Optional detailed print

    returncode, stdout, stderr = execute_ffmpeg_command(ffmpeg_cmd)

    # Save the full FFMPEG output (stdout + stderr) to a log file
    full_output = stdout + stderr
    try:
        with open(output_log_path, 'w', encoding='utf-8') as f: # Use utf-8 encoding
            f.write(full_output)
    except Exception as e:
         print(f"Warning: Could not save FFMPEG log to {output_log_path}: {e}")


    # Parse the output to find the timestamps from scdet
    shot_change_timestamps = []
    import re
    # Look for lines containing '[scdet @ ...] lavfi.scd.time: X.Y'
    # The regex looks for [scdet followed by anything up to ] space lavfi.scd.time: and then captures the number
    scdet_matches = re.findall(r"\[scdet.*?\] lavfi\.scd\.time: (\d+\.\d+)", full_output)
    shot_change_timestamps = [float(ts) for ts in scdet_matches]

    # Add the start of the video (time 0)
    if 0.0 not in shot_change_timestamps:
         shot_change_timestamps.insert(0, 0.0)

    # Sort the timestamps
    shot_change_timestamps.sort()

    print(f"Detected {len(shot_change_timestamps)} shot change points.")

    # If FFMPEG returned an error code and we found very few timestamps (only 0.0 and maybe video_end if added),
    # it's likely shot detection failed or the video is very short/static.
    # We still return the timestamps found (at least [0.0]) to attempt processing.
    if returncode != 0 and len(shot_change_timestamps) <= 1:
         print("Warning: FFMPEG shot detection command failed or found no shots.")


    return shot_change_timestamps

def get_video_duration_and_fps(video_full_path: str) -> Tuple[float, float]:
    """
    Uses OpenCV to get the total duration and frames per second (FPS) of a video.
    Returns duration (seconds) and fps (float).
    Returns (0.0, 0.0) on failure.
    """
    cap = cv.VideoCapture(video_full_path)
    duration_sec = 0.0
    fps = 0.0

    if cap.isOpened():
        # CAP_PROP_FRAME_COUNT gives the total number of frames
        frame_count = cap.get(cv.CAP_PROP_FRAME_COUNT)
        # CAP_PROP_FPS gives the frames per second
        fps = cap.get(cv.CAP_PROP_FPS)

        if frame_count > 0 and fps > 0:
            duration_sec = frame_count / fps
        else:
            print(f"Warning: Could not get valid frame count ({frame_count}) or FPS ({fps}) for {video_full_path} via OpenCV.")
            # Try getting duration via CAP_PROP_POS_MSEC trick if frame count is zero
            # This seeks to the end and gets the time position
            cap.set(cv.CAP_PROP_POS_ANY, cap.get(cv.CAP_PROP_FRAME_COUNT));
            duration_sec = cap.get(cv.CAP_PROP_POS_MSEC) / 1000.0
            if duration_sec <= 0:
                 print(f"Warning: Duration still zero or less ({duration_sec:.2f}s) for {video_full_path} via OpenCV.")
                 duration_sec = 0.0 # Ensure it's not negative or garbage value

        cap.release()
    else:
        print(f"Error: Could not open video file {video_full_path} with OpenCV to get duration/FPS. Returning 0.0, 0.0.")


    print(f"Video Duration: {duration_sec:.2f} seconds, FPS: {fps:.2f}")
    return duration_sec, fps

def select_keyframes_from_shots(shot_timestamps: List[float], video_duration: float, fps: float) -> List[float]:
    """
    Selects keyframe timestamps based on the detected shot changes, strategy,
    and fixed intervals. Ensures selected timestamps are within video duration and unique.
    """
    # Ensure 0.0 and video_duration are in the boundaries list if not already present
    # Add a small tolerance when checking if video_duration is already present
    boundaries = sorted(list(set(shot_timestamps + [0.0])))
    if video_duration > 0 and not any(abs(ts - video_duration) < 0.1 for ts in boundaries):
         boundaries.append(video_duration)
    boundaries.sort()

    keyframes = []
    if fps <= 0:
        print("Warning: FPS is invalid or zero, cannot accurately select keyframe timestamps.")
        # If FPS is bad, just return the start and end if video_duration is > 0
        if video_duration > 0:
             return [0.0, video_duration]
        return [] # Return empty list if no valid duration


    time_per_frame = 1.0 / fps
    min_time_diff = time_per_frame / 2.0 # Consider timestamps closer than half a frame duration as duplicates

    # --- Implement Keyframe Selection Strategy (Shot-based) ---
    if KEYFRAME_SELECTION_STRATEGY == 'middle':
        for i in range(len(boundaries) - 1):
            start_time = boundaries[i]
            end_time = boundaries[i+1]
            # Only calculate middle if segment is longer than a frame
            if end_time > start_time + time_per_frame: # Use full frame time for robustness
                middle_time = (start_time + end_time) / 2.0
                keyframes.append(middle_time)

    elif KEYFRAME_SELECTION_STRATEGY == 'start':
         # Select the boundary points themselves (with offset for non-zero start)
         for boundary_time in boundaries:
              if boundary_time == 0.0:
                   keyframes.append(0.0)
              # Add a small offset *after* the boundary, but ensure it's not past video end
              elif boundary_time < video_duration:
                  ts = boundary_time + KEYFRAME_BOUNDARY_OFFSET_SECONDS
                  keyframes.append(ts)
         # Add the very last frame's time explicitly if it's not already close
         if video_duration > 0 and (not keyframes or abs(keyframes[-1] - video_duration) > min_time_diff * 2):
              keyframes.append(video_duration)


    elif KEYFRAME_SELECTION_STRATEGY == 'end':
         # Select the boundary points (with offset before for non-end)
         for i in range(1, len(boundaries)): # Start from the second boundary
             boundary_time = boundaries[i]
             if boundary_time == video_duration:
                  keyframes.append(video_duration)
             elif boundary_time > 0.0: # Don't subtract offset from time 0
                 # Subtract a small offset before the boundary
                 ts = boundary_time - KEYFRAME_BOUNDARY_OFFSET_SECONDS
                 # Ensure we don't go before the previous boundary
                 if ts < boundaries[i-1]:
                     ts = boundaries[i-1] # Use the previous boundary time as minimum
                 keyframes.append(ts)
         # Add the first frame's time explicitly if it's not already close to 0
         if 0.0 not in keyframes and (not keyframes or keyframes[0] > min_time_diff * 2):
             keyframes.insert(0, 0.0) # Insert at beginning


    elif KEYFRAME_SELECTION_STRATEGY == 'boundary':
         # Select just the boundary points themselves (0.0 and detected shots + video_duration)
         keyframes = list(boundaries)


    elif KEYFRAME_SELECTION_STRATEGY == 'all':
         # Combine middle frames and all boundary points
         middle_frames = []
         for i in range(len(boundaries) - 1):
            start_time = boundaries[i]
            end_time = boundaries[i+1]
            if end_time > start_time + time_per_frame:
                middle_frames.append((start_time + end_time) / 2.0)

         # Combine middle frames and all boundary timestamps
         keyframes = list(set(middle_frames + list(boundaries)))


    else:
        print(f"Warning: Unknown keyframe selection strategy '{KEYFRAME_SELECTION_STRATEGY}'. Defaulting to 'middle'.")
        # Revert to middle strategy if input is invalid
        for i in range(len(boundaries) - 1):
            start_time = boundaries[i]
            end_time = boundaries[i+1]
            if end_time > start_time + time_per_frame:
                middle_time = (start_time + end_time) / 2.0
                keyframes.append(middle_time)


    # --- NEW: Add Keyframes at Fixed Intervals (if interval > 0) ---
    # This adds keyframes regardless of shot detection, ensuring coverage.
    # Make sure KEYFRAME_INTERVAL_SECONDS is not None and greater than 0
    if KEYFRAME_INTERVAL_SECONDS is not None and KEYFRAME_INTERVAL_SECONDS > 0 and video_duration > 0:
        interval_keyframes = []
        # Start times from the first interval (KEYFRAME_INTERVAL_SECONDS)
        # up to video_duration
        # We use <= video_duration + small_tolerance to include the last interval
        # if it lands exactly at or very near the end
        for i in range(1, int(video_duration / KEYFRAME_INTERVAL_SECONDS) + 2): # +2 to ensure we get the last interval
            interval_time = float(i * KEYFRAME_INTERVAL_SECONDS)
            # Break the loop if the interval time goes significantly past the video duration
            if interval_time > video_duration + time_per_frame/2.0:
                 break

            # Add the interval time if it's within the video duration (with small tolerance)
            # Check against 0.0 explicitly just in case
            if 0.0 < interval_time <= video_duration + time_per_frame/2.0:
                 interval_keyframes.append(interval_time)

        # Add the interval keyframes to the main list
        keyframes.extend(interval_keyframes)
        print(f"  Added {len(interval_keyframes)} keyframes at {KEYFRAME_INTERVAL_SECONDS}s intervals.")


    # --- Filter timestamps to be within video duration and remove near-duplicates ---
    # Ensure timestamps are valid (0.0 to video_duration, with a small tolerance at the end)
    valid_keyframes = [ts for ts in keyframes if ts >= 0.0 and ts <= video_duration + time_per_frame/2.0] # Add tolerance for end frame

    # Remove near-duplicate timestamps that might occur
    unique_keyframes = []
    if valid_keyframes:
        valid_keyframes.sort()
        unique_keyframes.append(valid_keyframes[0])
        for ts in valid_keyframes[1:]:
            if abs(ts - unique_keyframes[-1]) > min_time_diff:
                 unique_keyframes.append(ts)

    # Ensure 0.0 is always included if valid keyframes exist and it's not close to the first one
    if valid_keyframes and unique_keyframes[0] > min_time_diff * 2:
         if 0.0 not in unique_keyframes: # Redundant check, but safe
              unique_keyframes.insert(0, 0.0)

    # Ensure the end of the video is included if valid keyframes exist and it's not close to the last one
    if video_duration > 0 and valid_keyframes and abs(unique_keyframes[-1] - video_duration) > min_time_diff * 2:
        # Check if a timestamp is already very close to video_duration
        if not any(abs(ts - video_duration) < min_time_diff * 2 for ts in unique_keyframes):
             unique_keyframes.append(video_duration)


    unique_keyframes.sort() # Final sort

    print(f"Selected {len(unique_keyframes)} final keyframe timestamps.")

    return unique_keyframes


def extract_single_frame_image(video_full_path: str, timestamp_seconds: float) -> Union[Image.Image, None]:
    """Uses OpenCV to extract a single frame."""
    cap = cv.VideoCapture(video_full_path)
    if not cap.isOpened():
        # print(f"Error: Could not open video file {video_full_path} for frame extraction.") # Avoid excessive prints
        return None

    try:
        # Set the video position to the desired timestamp in milliseconds
        cap.set(cv.CAP_PROP_POS_MSEC, timestamp_seconds * 1000)

        # Read the frame at the current position.
        # OpenCV reads the frame *after* seeking.
        success, frame = cap.read()

        if success and frame is not None:
            # Convert color format from BGR (OpenCV default) to RGB (PIL default)
            frame_rgb = cv.cvtColor(frame, cv.COLOR_BGR2RGB)
            # Convert the numpy array (OpenCV image) to a PIL Image object
            pil_image = Image.fromarray(frame_rgb)
            # print(f"  Successfully extracted frame at {timestamp_seconds:.2f}s") # Optional detailed print
            return pil_image
        else:
            # print(f"Warning: Failed to read frame from {video_full_path} at {timestamp_seconds:.2f}s.") # Optional print
            return None

    except Exception as e:
        print(f"Error extracting frame from {video_full_path} at {timestamp_seconds:.2f}s: {e}")
        return None
    finally:
        if cap and cap.isOpened():
            cap.release()


def compress_video_for_storage(video_full_path: str, output_compressed_path: str):
    """Compresses the video using FFMPEG."""
    ffmpeg_cmd = f'ffmpeg -i "{video_full_path}" -vcodec libx264 -acodec aac -ac 1 -crf 35 -y "{output_compressed_path}" -nostats -loglevel 0'
    print(f"Compressing video: {os.path.basename(video_full_path)}")
    returncode, stdout, stderr = execute_ffmpeg_command(ffmpeg_cmd)
    if returncode == 0: print("  Compression successful.")
    else: print("  Compression failed.\n  FFMPEG Stderr:\n{stderr}")

def get_file_size_bytes(file_path: str) -> int:
    """Gets the size of a file in bytes."""
    if os.path.exists(file_path):
        try: return os.path.getsize(file_path)
        except Exception as e: print(f"Warning: Could not get file size for {file_path}: {e}"); return 0
    return 0

def get_current_processing_time() -> datetime:
    """Gets the current UTC time."""
    return datetime.utcnow()

# --- Add Cleanup Function ---
# This function deletes previous output files for a video.
def clean_previous_analysis_files(video_dir_path: str, report_path: str, compressed_video_path: str, keyframe_images_dir_full: str, ffmpeg_log_path: str):
    """
    Deletes previously created analysis output files and directories for a given video.
    Takes full paths as input.
    """
    # print(f"  Attempting to clean old analysis files in {video_dir_path}...") # Optional print

    files_to_delete = [report_path, compressed_video_path, ffmpeg_log_path]
    dirs_to_delete = [keyframe_images_dir_full]

    for file_path in files_to_delete:
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception as e: print(f"    Warning: Could not delete file {file_path}: {e}")

    for dir_path in dirs_to_delete:
        if os.path.exists(dir_path):
            try: shutil.rmtree(dir_path)
            except Exception as e: print(f"    Warning: Could not delete directory {dir_path}: {e}")

    # print("  Old analysis file cleanup finished.") # Optional print


# --- END OF FILE video_processors_io.py ---