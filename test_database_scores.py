#!/usr/bin/env python3
"""
Test script to compare old calculation method vs new database approach
"""

import requests
import json
import time

def test_search_comparison():
    """Compare old vs new search approaches"""
    
    test_queries = [
        "person walking with dog",
        "red car driving",
        "buildings in city"
    ]
    
    print("🧪 Testing Database vs Calculation Approach")
    print("=" * 60)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: '{query}'")
        
        try:
            # Test the new database approach
            start_time = time.time()
            response = requests.post(
                "http://localhost:5000/api/search/text",
                json={"query": query, "limit": 5},
                timeout=10
            )
            db_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                print(f"  ✅ Database Approach:")
                print(f"     Response time: {db_time:.3f}s")
                print(f"     Results: {data.get('count', 0)}")
                print(f"     Score type: {data.get('score_type', 'unknown')}")
                
                # Show sample scores
                results = data.get('results', [])
                if results:
                    print(f"     Sample scores:")
                    for j, result in enumerate(results[:3]):
                        score = result.get('score', 'N/A')
                        filename = result.get('original_filename', 'Unknown')
                        print(f"       {j+1}. {filename}: {score}")
                        
            else:
                print(f"  ❌ Database approach failed: {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request failed: {e}")
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print("🎯 Test completed!")

def test_performance():
    """Test performance of the new approach"""
    
    print("\n🚀 Performance Test")
    print("=" * 40)
    
    query = "person car building"
    iterations = 10
    
    times = []
    
    for i in range(iterations):
        try:
            start_time = time.time()
            response = requests.post(
                "http://localhost:5000/api/search/text",
                json={"query": query, "limit": 10},
                timeout=10
            )
            end_time = time.time()
            
            if response.status_code == 200:
                times.append(end_time - start_time)
                print(f"  Run {i+1}: {times[-1]:.3f}s")
            else:
                print(f"  Run {i+1}: Failed")
                
        except Exception as e:
            print(f"  Run {i+1}: Error - {e}")
    
    if times:
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"\n📊 Performance Results:")
        print(f"   Average: {avg_time:.3f}s")
        print(f"   Minimum: {min_time:.3f}s")
        print(f"   Maximum: {max_time:.3f}s")
        print(f"   Total runs: {len(times)}")

if __name__ == "__main__":
    test_search_comparison()
    test_performance() 