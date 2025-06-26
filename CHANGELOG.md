# Changelog

All notable changes to the VBS Video Retrieval System will be documented in this file.

## [1.1.0] - 2025-06-22

### 🆕 Added
- **Enhanced Text Search with Keyword Extraction**
  - Intelligent keyword extraction from natural language queries
  - Automatic stop word filtering to focus on meaningful terms
  - Relevance scoring for search results (0-1 scale)
  - Multi-field search across extracted words, object names, and filenames

### 🔧 Enhanced
- **Backend API Endpoints**
  - `/api/search/text` - Now includes keyword extraction and scoring
  - `/api/search/multimodal` - Enhanced with keyword-based text search
  - Added `extract_keywords_from_sentence()` utility function
  - Improved response format with extracted keywords and scores

- **Frontend Components**
  - Updated `useSearchVideos` hook to handle extracted keywords
  - Enhanced `Home` component to display search analysis
  - Added keyword display in search results
  - Improved score visualization

### 🧪 Testing
- Added `test_keyword_extraction.py` for keyword extraction validation
- Added `test_text_search.py` for end-to-end text search testing
- Comprehensive test cases for various query types

### 📚 Documentation
- Updated README.md with new features and API documentation
- Added detailed API endpoint examples
- Documented scoring algorithm and keyword extraction process

### 🔍 Technical Details
- **Keyword Extraction Algorithm:**
  - Converts input to lowercase
  - Removes punctuation and special characters
  - Filters out common English stop words
  - Removes words shorter than 2 characters
  - Returns unique, meaningful keywords

- **Scoring Algorithm:**
  - Counts matched keywords across database fields
  - Normalizes by total number of extracted keywords
  - Returns score between 0.0 and 1.0
  - Higher scores indicate more relevant results

- **Database Fields Searched:**
  - `extracted_search_words` - OCR text from video frames
  - `detected_object_names` - YOLO object detection results
  - `original_filename` - Video filenames

### 🐛 Bug Fixes
- Fixed missing score display in frontend for text-only searches
- Improved error handling for empty keyword extraction
- Enhanced stop word list with additional common words

### 📦 Dependencies
- No new dependencies added
- All changes use existing Python standard library modules

---

## [1.0.0] - 2025-06-20

### 🎉 Initial Release
- Complete VBS Video Retrieval System
- Multi-modal search capabilities (text, color, object, vector)
- VBS competition integration with DRES
- Docker-based deployment
- React frontend with modern UI
- PostgreSQL backend with pgvector support

### 🎯 VBS Features
- DRES client integration
- Known-Item Search (KIS) support
- Competition mode interface
- Real-time status monitoring
- Batch submission capabilities

### 🔧 Core Features
- Video streaming and playback
- AI-powered content analysis (YOLO, CLIP, EasyOCR)
- Vector similarity search
- Temporal search capabilities
- Responsive web interface

---

## Version History

- **1.1.0** - Enhanced text search with keyword extraction and scoring
- **1.0.0** - Initial release with full VBS competition support

---

## Contributing

When making changes, please:
1. Update this changelog with a new version entry
2. Follow semantic versioning (MAJOR.MINOR.PATCH)
3. Include detailed descriptions of changes
4. Categorize changes (Added, Changed, Deprecated, Removed, Fixed, Security) 