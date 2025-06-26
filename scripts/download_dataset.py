# /vbs-video-retrieval-system/scripts/download_dataset.py
import os
import zipfile
import shutil
from pathlib import Path

# --- Configuration ---
# Update this path to point to your local ZIP file
LOCAL_ZIP_PATH = r"E:\image and video deep learning\V3C1_200.zip"  # Update this path to location of your downladed zip file
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET_DIR = PROJECT_ROOT / "Dataset" / "V3C1-200"
TEMP_EXTRACT_DIR = PROJECT_ROOT / "temp_v3c1_extracted"

def check_zip_file():
    """Check if the local ZIP file exists."""
    if not os.path.exists(LOCAL_ZIP_PATH):
        print(f"❌ Error: ZIP file not found at {LOCAL_ZIP_PATH}")
        print("Please update the LOCAL_ZIP_PATH variable in the script to point to your ZIP file.")
        return False
    return True

def extract_and_organize():
    """Extracts the ZIP file and organizes videos into correct folders."""
    print(f"Extracting ZIP file from {LOCAL_ZIP_PATH}...")
    
    try:
        with zipfile.ZipFile(LOCAL_ZIP_PATH, 'r') as zip_ref:
            # List contents for verification
            file_list = zip_ref.namelist()
            print(f"Found {len(file_list)} files in ZIP")
            
            # Extract all files
            zip_ref.extractall(TEMP_EXTRACT_DIR)
        
        print("Organizing videos into folders...")
        
        # Check if videos are in a V3C1_200 subfolder
        video_source_dir = TEMP_EXTRACT_DIR / "V3C1_200"
        if video_source_dir.exists():
            print(f"Found V3C1_200 subfolder, looking for videos in: {video_source_dir}")
            search_dir = video_source_dir
        else:
            print("No V3C1_200 subfolder found, looking for videos in root of extracted files")
            search_dir = TEMP_EXTRACT_DIR
        
        # Move each video to its corresponding folder
        video_count = 0
        for video_file in search_dir.glob("*.mp4"):
            video_id = video_file.stem  # e.g., "00001" from "00001.mp4"
            target_folder = TARGET_DIR / video_id
            target_folder.mkdir(parents=True, exist_ok=True)
            
            # Move the video file
            shutil.move(str(video_file), str(target_folder / video_file.name))
            print(f"Moved {video_file.name} to {target_folder}")
            video_count += 1
        
        if video_count == 0:
            print("❌ No .mp4 files found!")
            print("Available files in extracted directory:")
            for item in search_dir.iterdir():
                print(f"  - {item.name}")
            return False
        
        print(f"✅ Successfully organized {video_count} videos")
        
    except zipfile.BadZipFile:
        print("❌ Error: Invalid ZIP file or file is corrupted")
        return False
    except Exception as e:
        print(f"❌ Error during extraction: {e}")
        return False
    
    return True

def cleanup():
    """Removes temporary files."""
    print("Cleaning up temporary files...")
    if TEMP_EXTRACT_DIR.exists():
        shutil.rmtree(TEMP_EXTRACT_DIR)
    print("Cleanup completed")

def main():
    """Main function to extract and organize the dataset."""
    print("🚀 VBS Video Dataset Organizer")
    print("=" * 40)
    print(f"Source ZIP: {LOCAL_ZIP_PATH}")
    print(f"Target Directory: {TARGET_DIR}")
    print("=" * 40)
    
    # Check if ZIP file exists
    if not check_zip_file():
        return
    
    # Create target directory
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    
    try:
        # Extract and organize
        if extract_and_organize():
            # Cleanup
            cleanup()
            
            print("\n✅ Dataset organization completed!")
            print(f"📁 Videos are now available in: {TARGET_DIR}")
            print("\nNext steps:")
            print("1. Run: docker-compose up --build")
            print("2. Initialize database schema")
            print("3. Run: python -m scripts.import_data")
        else:
            print("❌ Failed to organize dataset")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        cleanup()

if __name__ == "__main__":
    main() 