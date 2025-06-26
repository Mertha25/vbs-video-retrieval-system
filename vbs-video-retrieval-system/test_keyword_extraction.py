#!/usr/bin/env python3
"""
Test script for keyword extraction functionality
"""

import sys
import os

# Add the query_server directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'query_server'))

from utils_server import extract_keywords_from_sentence

def test_keyword_extraction():
    """Test the keyword extraction function with various inputs"""
    
    test_cases = [
        {
            "input": "I want to find videos with people playing sports",
            "expected_keywords": ["want", "videos", "people", "playing", "sports"]
        },
        {
            "input": "Show me cars and buildings in the city",
            "expected_keywords": ["show", "cars", "buildings", "city"]
        },
        {
            "input": "A person walking with a dog in the park",
            "expected_keywords": ["person", "walking", "dog", "park"]
        },
        {
            "input": "Red car driving on the highway",
            "expected_keywords": ["red", "car", "driving", "highway"]
        },
        {
            "input": "The quick brown fox jumps over the lazy dog",
            "expected_keywords": ["quick", "brown", "fox", "jumps", "lazy", "dog"]
        },
        {
            "input": "Hello world! This is a test.",
            "expected_keywords": ["hello", "world", "test"]
        }
    ]
    
    print("🧪 Testing Keyword Extraction Function")
    print("=" * 50)
    
    all_passed = True
    
    for i, test_case in enumerate(test_cases, 1):
        input_text = test_case["input"]
        expected = set(test_case["expected_keywords"])
        
        # Extract keywords
        extracted = extract_keywords_from_sentence(input_text)
        extracted_set = set(extracted)
        
        # Check if extraction matches expected
        passed = extracted_set == expected
        
        print(f"\nTest {i}:")
        print(f"  Input: '{input_text}'")
        print(f"  Expected: {sorted(expected)}")
        print(f"  Extracted: {sorted(extracted_set)}")
        print(f"  Status: {'✅ PASS' if passed else '❌ FAIL'}")
        
        if not passed:
            all_passed = False
            print(f"  Missing: {expected - extracted_set}")
            print(f"  Extra: {extracted_set - expected}")
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All tests passed!")
    else:
        print("❌ Some tests failed!")
    
    return all_passed

if __name__ == "__main__":
    test_keyword_extraction() 