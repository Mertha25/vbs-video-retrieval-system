#!/usr/bin/env python3
"""
Test script for VBS Video Retrieval System
This script tests the database connection, API endpoints, and basic functionality.
"""

import requests
import psycopg2
import json
from query_server.config import DB_CONFIG

def test_database_connection():
    """Test database connection and basic queries."""
    print("🔍 Testing database connection...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Test basic queries
        cursor.execute("SELECT COUNT(*) FROM videos")
        video_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM video_moments")
        moment_count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✅ Database connection successful")
        print(f"   Videos: {video_count}")
        print(f"   Moments: {moment_count}")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_backend_health():
    """Test backend health endpoint."""
    print("\n🔍 Testing backend health...")
    try:
        response = requests.get("http://localhost:5000/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Backend is healthy")
            print(f"   Status: {data.get('status')}")
            print(f"   Service: {data.get('service')}")
            return True
        else:
            print(f"❌ Backend health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Backend health check failed: {e}")
        return False

def test_system_stats():
    """Test system stats endpoint."""
    print("\n🔍 Testing system stats...")
    try:
        response = requests.get("http://localhost:5000/api/stats", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ System stats retrieved")
            print(f"   Videos: {data.get('videos', 0)}")
            print(f"   Moments: {data.get('moments', 0)}")
            print(f"   Moments with color: {data.get('moments_with_color', 0)}")
            print(f"   Moments with embedding: {data.get('moments_with_embedding', 0)}")
            return True
        else:
            print(f"❌ System stats failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ System stats failed: {e}")
        return False

def test_text_search():
    """Test basic text search."""
    print("\n🔍 Testing text search...")
    try:
        payload = {
            "text": "test",
            "limit": 5
        }
        response = requests.post(
            "http://localhost:5000/api/search/multimodal",
            json=payload,
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Text search successful")
            print(f"   Results: {data.get('count', 0)}")
            return True
        else:
            print(f"❌ Text search failed: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Text search failed: {e}")
        return False

def test_frontend_access():
    """Test frontend accessibility."""
    print("\n🔍 Testing frontend access...")
    try:
        response = requests.get("http://localhost", timeout=5)
        if response.status_code == 200:
            print(f"✅ Frontend is accessible")
            return True
        else:
            print(f"❌ Frontend access failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Frontend access failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 VBS Video Retrieval System - System Test")
    print("=" * 50)
    
    tests = [
        test_database_connection,
        test_backend_health,
        test_system_stats,
        test_text_search,
        test_frontend_access
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 50)
    print("📊 Test Results Summary")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"🎉 All {total} tests passed! System is working correctly.")
    else:
        print(f"⚠️  {passed}/{total} tests passed. Some issues detected.")
        print("\n💡 Troubleshooting tips:")
        print("   1. Ensure all containers are running: docker-compose ps")
        print("   2. Check container logs: docker-compose logs")
        print("   3. Restart services: docker-compose restart")
        print("   4. Rebuild if needed: docker-compose up --build")

if __name__ == "__main__":
    main() 