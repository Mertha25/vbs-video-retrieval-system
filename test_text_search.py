#!/usr/bin/env python3
"""
Test script for text search with scoring functionality
"""

import requests
import json

def test_text_search():
    """Test the text search endpoint with scoring"""
    
    # Test cases
    test_queries = [
        "person walking with dog",
        "red car driving",
        "buildings in city",
        "people playing sports"
    ]
    
    print("🧪 Testing Text Search with Scoring")
    print("=" * 50)
    
    for i, query in enumerate(test_queries, 1):
        print(f"\nTest {i}: '{query}'")
        
        try:
            # Test the text search endpoint
            response = requests.post(
                "http://localhost:5000/api/search/text",
                json={"query": query, "limit": 5},
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"  ✅ Status: {response.status_code}")
                print(f"  📊 Results: {data.get('count', 0)}")
                print(f"  🔑 Extracted keywords: {data.get('extracted_keywords', [])}")
                
                # Check if results have scores
                results = data.get('results', [])
                if results:
                    print(f"  📈 Sample scores:")
                    for j, result in enumerate(results[:3]):  # Show first 3 results
                        score = result.get('score', 'N/A')
                        filename = result.get('original_filename', 'Unknown')
                        print(f"    {j+1}. {filename}: {score:.3f}")
                else:
                    print("  ⚠️  No results found")
                    
            else:
                print(f"  ❌ Status: {response.status_code}")
                print(f"  Error: {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request failed: {e}")
        except Exception as e:
            print(f"  ❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("🎯 Test completed! Check the results above.")

if __name__ == "__main__":
    test_text_search() 