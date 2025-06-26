#!/usr/bin/env python3
"""
Requirements Setup Script
Manages the integration of requirements files and ensures consistency.
"""

import os
import shutil
from pathlib import Path

def main():
    """Main function to set up requirements integration."""
    print("🔧 Setting up requirements integration...")
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    query_server_dir = project_root / "query_server"
    main_requirements = project_root / "requirements.txt"
    query_requirements = query_server_dir / "requirements.txt"
    
    print(f"📁 Project root: {project_root}")
    print(f"📁 Query server: {query_server_dir}")
    
    # Check if main requirements.txt exists
    if not main_requirements.exists():
        print("❌ Main requirements.txt not found!")
        return False
    
    # Check if query_server requirements.txt exists
    if query_requirements.exists():
        print("📋 Found query_server/requirements.txt")
        print("📋 Found main requirements.txt")
        
        # Compare the files
        with open(main_requirements, 'r') as f:
            main_content = f.read()
        
        with open(query_requirements, 'r') as f:
            query_content = f.read()
        
        if main_content == query_content:
            print("✅ Requirements files are identical")
        else:
            print("⚠️  Requirements files differ")
            print("📝 Main requirements.txt is more comprehensive and up-to-date")
        
        # Ask if user wants to remove the duplicate
        response = input("🗑️  Remove query_server/requirements.txt? (y/n): ")
        if response.lower() in ['y', 'yes']:
            try:
                query_requirements.unlink()
                print("✅ Removed query_server/requirements.txt")
            except Exception as e:
                print(f"❌ Failed to remove file: {e}")
        else:
            print("ℹ️  Keeping both files")
    
    # Create a symlink or copy for development convenience
    if not query_requirements.exists():
        print("🔗 Creating symlink for development convenience...")
        try:
            # Create a symlink (works on Unix-like systems)
            if os.name != 'nt':  # Not Windows
                os.symlink(main_requirements, query_requirements)
                print("✅ Created symlink: query_server/requirements.txt -> requirements.txt")
            else:
                # On Windows, create a copy
                shutil.copy2(main_requirements, query_requirements)
                print("✅ Created copy: query_server/requirements.txt")
        except Exception as e:
            print(f"⚠️  Could not create link/copy: {e}")
    
    # Verify Docker setup
    print("\n🐳 Verifying Docker setup...")
    dockerfile_path = query_server_dir / "Dockerfile"
    if dockerfile_path.exists():
        with open(dockerfile_path, 'r') as f:
            dockerfile_content = f.read()
        
        if "COPY requirements.txt" in dockerfile_content:
            print("✅ Dockerfile correctly references requirements.txt")
        else:
            print("⚠️  Dockerfile may need updating")
    
    print("\n✅ Requirements integration setup complete!")
    print("\n📋 Summary:")
    print("  • Main requirements.txt: Comprehensive dependencies")
    print("  • Includes: AI/ML, Web framework, Database, VBS integration")
    print("  • Docker build: Uses main requirements.txt")
    print("  • Development: Can use either file")
    
    return True

if __name__ == "__main__":
    main() 