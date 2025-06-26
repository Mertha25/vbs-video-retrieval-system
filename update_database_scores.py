#!/usr/bin/env python3
"""
Database migration script to add relevance score columns and populate them.
Run this script to update your existing database with the new scoring system.
"""

import psycopg2
import psycopg2.extras
import sys
import os

# Add the query_server directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'query_server'))

from utils_server import update_all_moment_scores
from db_utils import get_db_connection

def add_score_columns():
    """Add the new score columns to the video_moments table."""
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        print("🔧 Adding relevance score columns to database...")
        
        # Add new columns if they don't exist
        columns_to_add = [
            "text_relevance_score FLOAT DEFAULT 0.0",
            "object_relevance_score FLOAT DEFAULT 0.0", 
            "color_relevance_score FLOAT DEFAULT 0.0",
            "overall_relevance_score FLOAT DEFAULT 0.0"
        ]
        
        for column_def in columns_to_add:
            column_name = column_def.split()[0]
            try:
                cursor.execute(f"ALTER TABLE video_moments ADD COLUMN {column_def}")
                print(f"✅ Added column: {column_name}")
            except psycopg2.errors.DuplicateColumn:
                print(f"⚠️  Column {column_name} already exists, skipping...")
        
        conn.commit()
        print("✅ Database schema updated successfully!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating database schema: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def populate_scores():
    """Populate the score columns with calculated values."""
    
    conn = get_db_connection()
    try:
        print("📊 Calculating and populating relevance scores...")
        update_all_moment_scores(conn)
        print("✅ Score population completed!")
        return True
        
    except Exception as e:
        print(f"❌ Error populating scores: {e}")
        return False
    finally:
        conn.close()

def verify_scores():
    """Verify that scores have been populated correctly."""
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check if scores exist
        cursor.execute("""
            SELECT COUNT(*) as total_moments,
                   COUNT(text_relevance_score) as text_scores,
                   COUNT(object_relevance_score) as object_scores,
                   COUNT(color_relevance_score) as color_scores,
                   COUNT(overall_relevance_score) as overall_scores,
                   AVG(overall_relevance_score) as avg_score
            FROM video_moments
        """)
        
        result = cursor.fetchone()
        
        print("\n📊 Score Verification Results:")
        print(f"   Total moments: {result[0]}")
        print(f"   Text scores: {result[1]}")
        print(f"   Object scores: {result[2]}")
        print(f"   Color scores: {result[3]}")
        print(f"   Overall scores: {result[4]}")
        print(f"   Average score: {result[5]:.3f}")
        
        # Show some sample scores
        cursor.execute("""
            SELECT moment_id, text_relevance_score, object_relevance_score, 
                   color_relevance_score, overall_relevance_score
            FROM video_moments 
            WHERE overall_relevance_score > 0
            ORDER BY overall_relevance_score DESC 
            LIMIT 5
        """)
        
        samples = cursor.fetchall()
        print(f"\n🏆 Top 5 highest scoring moments:")
        for sample in samples:
            print(f"   {sample[0]}: {sample[4]:.3f} (text: {sample[1]:.3f}, object: {sample[2]:.3f}, color: {sample[3]:.3f})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying scores: {e}")
        return False
    finally:
        conn.close()

def main():
    """Main migration function."""
    
    print("🚀 Starting Database Score Migration")
    print("=" * 50)
    
    # Step 1: Add columns
    if not add_score_columns():
        print("❌ Failed to add score columns. Exiting.")
        return False
    
    # Step 2: Populate scores
    if not populate_scores():
        print("❌ Failed to populate scores. Exiting.")
        return False
    
    # Step 3: Verify results
    if not verify_scores():
        print("❌ Failed to verify scores. Exiting.")
        return False
    
    print("\n🎉 Database migration completed successfully!")
    print("You can now use the new scoring system in your searches.")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 