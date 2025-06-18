import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime
import logging

from config import DB_CONFIG

DATASET_PATH = r"C:\Users\Hp\Desktop\video-retrieval-system\Dataset\V3C1-200"

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    return logging.getLogger(__name__)

def get_db_connection():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        return None

def find_video_folders(dataset_path):
    dataset = Path(dataset_path)
    if not dataset.exists():
        return []

    video_folders = []
    for item in dataset.iterdir():
        if item.is_dir() and item.name.isdigit():
            analysis_report = item / "video_analysis_report.json"
            if analysis_report.exists():
                video_folders.append(item)
    return sorted(video_folders)

def import_single_video(video_folder, logger):
    video_id = video_folder.name
    analysis_file = video_folder / "video_analysis_report.json"

    try:
        with open(analysis_file, 'r', encoding='utf-8') as f:
            report_data = json.load(f)

        logger.info(f"Processing video {video_id}...")
        conn = get_db_connection()
        if not conn:
            return False, "Database connection failed"

        cursor = conn.cursor()
        try:
            conn.autocommit = False

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

            processing_date = None
            if report_data.get('processing_date_utc'):
                try:
                    date_str = report_data['processing_date_utc']
                    if date_str.endswith('Z'):
                        date_str = date_str.replace('Z', '+00:00')
                    processing_date = datetime.fromisoformat(date_str)
                except Exception as e:
                    logger.warning(f"Could not parse date for {video_id}: {e}")

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

            cursor.execute("DELETE FROM video_moments WHERE video_id = %s", (video_id,))

            analyzed_keyframes = report_data.get('analyzed_keyframes', [])
            for idx, moment_data in enumerate(analyzed_keyframes):
                moment_sql = """
                INSERT INTO video_moments (
                    moment_id, video_id, frame_identifier, timestamp_seconds,
                    keyframe_image_path, clip_embedding, detected_object_names,
                    extracted_search_words, average_color_rgb, detailed_features
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                cursor.execute(moment_sql, (
                    moment_data.get('moment_id', f"{video_id}_frame_{idx}"),
                    video_id,
                    moment_data.get('frame_identifier', f'frame_{idx:012d}'),
                    moment_data.get('timestamp_seconds', 0.0),
                    moment_data.get('keyframe_image_path'),
                    json.dumps(moment_data.get('clip_embedding')) if moment_data.get('clip_embedding') else None,
                    json.dumps(moment_data.get('detected_object_names', [])),
                    json.dumps(moment_data.get('extracted_search_words', [])),
                    json.dumps(moment_data.get('average_color_rgb', [0, 0, 0])),
                    json.dumps(moment_data.get('detailed_features', {}))
                ))

            conn.commit()
            logger.info(f" Video {video_id}: {len(analyzed_keyframes)} moments imported")
            return True, f"Imported {len(analyzed_keyframes)} moments"

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
    logger = setup_logging()

    if not os.path.exists(DATASET_PATH):
        logger.error(f"Dataset path not found: {DATASET_PATH}")
        return

    video_folders = find_video_folders(DATASET_PATH)
    if not video_folders:
        logger.warning("No video folders with analysis reports found")
        return

    logger.info(f"Found {len(video_folders)} video folders to import")
    response = input(f"\nImport {len(video_folders)} videos? (y/N): ")
    if response.lower() != 'y':
        logger.info("Import cancelled")
        return

    successful, failed, total_moments = 0, 0, 0
    for i, folder in enumerate(video_folders, 1):
        logger.info(f"[{i}/{len(video_folders)}] Processing {folder.name}...")
        success, message = import_single_video(folder, logger)
        if success:
            successful += 1
            try:
                total_moments += int(message.split()[1])
            except:
                pass
        else:
            failed += 1
            logger.error(f"Failed: {message}")

    logger.info("\n=== IMPORT SUMMARY ===")
    logger.info(f"Successful imports: {successful}")
    logger.info(f"Failed imports: {failed}")
    logger.info(f"Total moments imported: {total_moments}")

if __name__ == "__main__":
    main()
