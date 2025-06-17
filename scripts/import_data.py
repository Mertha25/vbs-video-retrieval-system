#!/usr/bin/env python3
"""
Data Importer for Video Retrieval System

"""

import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime
import logging

# Database configuration (same as your working setup)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'admin123',
    'database': 'videodb_creative_v2'
}

# Your dataset path - UPDATE THIS to match your actual path
DATASET_PATH = r"C:\Users\Hp\Desktop\video-retrieval-system\Dataset\V3C1-200"


def setup_logging():
    """Setup logging"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)


def get_db_connection():
    """Get database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None


def find_video_folders(dataset_path):
    """Find all video folders in your dataset"""
    dataset = Path(dataset_path)
    if not dataset.exists():
        print(f"Dataset path not found: {dataset_path}")
        return []

    video_folders = []
    for item in dataset.iterdir():
        if item.is_dir() and item.name.isdigit():  # Video folders like 00001, 00002, etc.
            analysis_report = item / "video_analysis_report.json"
            if analysis_report.exists():
                video_folders.append(item)
            else:
                print(f"Warning: No analysis report found in {item}")

    print(f"Found {len(video_folders)} video folders with analysis reports")
    return sorted(video_folders)


def import_single_video(video_folder, logger):
    """Import data from a single video folder"""
    video_id = video_folder.name
    analysis_file = video_folder / "video_analysis_report.json"

    try:
        # Load the analysis report
        with open(analysis_file, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        logger.info(f"Processing video {video_id}...")

        conn = get_db_connection()
        if not conn:
            return False, "Database connection failed"

        cursor = conn.cursor()

        try:
            # Start transaction
            conn.autocommit = False

            # 1. Import video record
            video_sql = """
            INSERT INTO videos (
                video_id, original_filename, compressed_filename, 
                duration_seconds, fps, compressed_file_size_bytes,
                processing_date_utc, scene_change_timestamps,
                keyframes_analyzed_count, analysis_status, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE SET
                original_filename = EXCLUDED.original_filename,
                compressed_filename = EXCLUDED.compressed_filename,
                duration_seconds = EXCLUDED.duration_seconds,
                fps = EXCLUDED.fps,
                compressed_file_size_bytes = EXCLUDED.compressed_file_size_bytes,
                processing_date_utc = EXCLUDED.processing_date_utc,
                scene_change_timestamps = EXCLUDED.scene_change_timestamps,
                keyframes_analyzed_count = EXCLUDED.keyframes_analyzed_count,
                analysis_status = EXCLUDED.analysis_status,
                error_message = EXCLUDED.error_message,
                updated_at = CURRENT_TIMESTAMP
            """

            # Parse processing date
            processing_date = None
            if report_data.get('processing_date_utc'):
                try:
                    date_str = report_data['processing_date_utc']
                    if date_str.endswith('Z'):
                        date_str = date_str.replace('Z', '+00:00')
                    processing_date = datetime.fromisoformat(date_str)
                except Exception as e:
                    logger.warning(f"Could not parse date for {video_id}: {e}")

            # Convert scene_change_timestamps to JSON string
            scene_timestamps = json.dumps(report_data.get('scene_change_timestamps', []))

            cursor.execute(video_sql, (
                report_data.get('video_id', video_id),
                report_data.get('original_filename', f'{video_id}.mp4'),
                report_data.get('compressed_filename', 'compressed_for_web.mp4'),
                report_data.get('duration_seconds', 0),
                report_data.get('fps', 25.0),
                report_data.get('compressed_file_size_bytes', 0),
                processing_date,
                scene_timestamps,
                report_data.get('keyframes_analyzed_count', 0),
                report_data.get('analysis_status', 'completed'),
                report_data.get('error_message')
            ))

            # 2. Delete existing moments for this video (clean import)
            cursor.execute("DELETE FROM video_moments WHERE video_id = %s", (video_id,))

            # 3. Import moments (keyframes)
            moments_imported = 0
            analyzed_keyframes = report_data.get('analyzed_keyframes', [])

            for moment_data in analyzed_keyframes:
                moment_sql = """
                INSERT INTO video_moments (
                    moment_id, video_id, frame_identifier, timestamp_seconds,
                    keyframe_image_path, clip_embedding, detected_object_names,
                    extracted_search_words, average_color_rgb, detailed_features
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                # Convert arrays to JSON strings for storage in TEXT fields
                clip_embedding = json.dumps(moment_data.get('clip_embedding')) if moment_data.get(
                    'clip_embedding') else None
                object_names = json.dumps(moment_data.get('detected_object_names', []))
                search_words = json.dumps(moment_data.get('extracted_search_words', []))
                avg_color = json.dumps(moment_data.get('average_color_rgb', [0, 0, 0]))
                detailed_features = json.dumps(moment_data.get('detailed_features', {}))

                cursor.execute(moment_sql, (
                    moment_data.get('moment_id', f"{video_id}_frame_{moments_imported}"),
                    video_id,
                    moment_data.get('frame_identifier', f'frame_{moments_imported:012d}'),
                    moment_data.get('timestamp_seconds', 0.0),
                    moment_data.get('keyframe_image_path'),
                    clip_embedding,
                    object_names,
                    search_words,
                    avg_color,
                    detailed_features
                ))

                moments_imported += 1

            # Commit transaction
            conn.commit()

            logger.info(f" Video {video_id}: {moments_imported} moments imported")
            return True, f"Imported {moments_imported} moments"

        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        logger.error(f" Error importing video {video_id}: {e}")
        return False, str(e)


def main():
    """Main import function"""
    logger = setup_logging()

    print("🎬 Real Data Importer for Video Retrieval System")
    print("=" * 50)

    # Check dataset path
    if not os.path.exists(DATASET_PATH):
        print(f" Dataset path not found: {DATASET_PATH}")
        print("Please update DATASET_PATH in this script to your actual V3C1-200 location")
        return

    print(f" Dataset path: {DATASET_PATH}")

    # Find video folders
    video_folders = find_video_folders(DATASET_PATH)
    if not video_folders:
        print(" No video folders with analysis reports found")
        return

    print(f" Found {len(video_folders)} videos to import")

    # Show some examples
    if len(video_folders) > 0:
        print(f" Examples: {', '.join([f.name for f in video_folders[:5]])}")
        if len(video_folders) > 5:
            print(f"   ... and {len(video_folders) - 5} more")

    # Ask for confirmation
    response = input(f"\n Import {len(video_folders)} videos? (y/N): ")
    if response.lower() != 'y':
        print(" Import cancelled")
        return

    # Import videos
    successful_imports = 0
    failed_imports = 0
    total_moments = 0

    for i, video_folder in enumerate(video_folders, 1):
        print(f"\n [{i}/{len(video_folders)}] Processing {video_folder.name}...")

        success, message = import_single_video(video_folder, logger)
        if success:
            successful_imports += 1
            # Extract moment count from message
            if "moments" in message:
                try:
                    moment_count = int(message.split()[1])
                    total_moments += moment_count
                except:
                    pass
        else:
            failed_imports += 1
            print(f" Failed: {message}")

    # Final report
    print("\n" + "=" * 50)
    print(" IMPORT SUMMARY")
    print("=" * 50)
    print(f" Successful imports: {successful_imports}")
    print(f" Failed imports: {failed_imports}")
    print(f" Total moments imported: {total_moments}")

    if successful_imports > 0:
        print(f"\n SUCCESS! Your real video data is now in the database!")
        print(f" Test search with your API:")
        print(f"   PowerShell: $body = @{{ query = 'person' }} | ConvertTo-Json")
        print(
            f"   PowerShell: Invoke-RestMethod -Uri 'http://localhost:5000/api/search/text' -Method POST -Body $body -ContentType 'application/json'")
        print(f" Check stats: http://localhost:5000/api/stats")


if __name__ == "__main__":
    main()