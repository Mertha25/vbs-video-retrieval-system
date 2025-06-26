# VBS Video Retrieval System

A full-stack, multimodal video retrieval system for content-based search, built for research and competition use with **Video Browser Showdown (VBS)** integration.

---

## 🆕 Recent Updates (Latest Version)

### **Enhanced Text Search with Keyword Extraction & Scoring**

**New Features:**
- **🔍 Intelligent Keyword Extraction** - Automatically extracts meaningful keywords from user sentences
- **📊 Relevance Scoring** - Each search result now includes a relevance score (0-1)
- **🎯 Smart Stop Word Filtering** - Removes common words to focus on meaningful terms
- **📈 Score Display** - Frontend shows extracted keywords and relevance scores
- **🔄 Improved Search Logic** - Searches across multiple database fields for better results

**How It Works:**
1. User enters a natural language query (e.g., "A person walking with a dog in the park")
2. System extracts keywords: `['person', 'walking', 'dog', 'park']`
3. Searches database for any of these keywords in:
   - `extracted_search_words` (OCR text from video frames)
   - `detected_object_names` (YOLO object detection)
   - `original_filename` (video filenames)
4. Computes relevance score based on keyword matches
5. Returns ranked results with scores and extracted keywords

**Example Query:**
```
Input: "Show me videos with red cars driving on the highway"
Extracted Keywords: ['show', 'videos', 'red', 'cars', 'driving', 'highway']
Results: Videos ranked by relevance score (0.0 - 1.0)
```

**Technical Implementation:**
- **Backend**: Enhanced `/api/search/text` and `/api/search/multimodal` endpoints
- **Frontend**: Updated search interface with keyword display and score visualization
- **Database**: Optimized queries for keyword-based search across multiple fields
- **Scoring**: Fraction-based scoring (matched keywords / total keywords)

---

## 🔌 API Endpoints

### **Enhanced Search Endpoints**

#### **Text Search with Keyword Extraction**
```http
POST /api/search/text
Content-Type: application/json

{
  "query": "A person walking with a dog in the park",
  "limit": 50
}
```

**Response:**
```json
{
  "results": [
    {
      "moment_id": "00001_frame_001",
      "video_id": "00001",
      "timestamp_seconds": 5.2,
      "original_filename": "00001.mp4",
      "score": 0.75,
      "extracted_search_words": ["person", "walking", "dog"],
      "detected_object_names": ["person", "dog"]
    }
  ],
  "count": 1,
  "extracted_keywords": ["person", "walking", "dog", "park"],
  "query": "A person walking with a dog in the park"
}
```

#### **Multimodal Search (Enhanced)**
```http
POST /api/search/multimodal
Content-Type: application/json

{
  "text": "red car driving on highway",
  "color": [255, 0, 0],
  "objects": ["car", "person"],
  "limit": 50
}
```

**Features:**
- **Keyword Extraction**: Automatically extracts meaningful keywords from text input
- **Relevance Scoring**: Each result includes a score (0-1) based on keyword matches
- **Multi-field Search**: Searches across `extracted_search_words`, `detected_object_names`, and `original_filename`
- **Combined Modalities**: Can combine text, color, object, and time-based searches

### **Core Search Endpoints**

#### **Color Search**
```http
POST /api/search/color
{
  "color": [255, 0, 0],
  "threshold": 50,
  "limit": 50
}
```

#### **Object Search**
```http
POST /api/search/objects
{
  "objects": ["car", "person"],
  "match_all": false,
  "limit": 50
}
```

#### **Vector Search**
```http
POST /api/search/vector
{
  "embedding": [0.1, 0.2, ...],
  "threshold": 0.7,
  "limit": 50
}
```

#### **Temporal Search**
```http
POST /api/search/temporal
{
  "start_time": 0,
  "end_time": 60,
  "video_id": "00001",
  "limit": 50
}
```

### **Video Serving**
```http
GET /api/videos/{video_id}/{filename}
```

### **System Endpoints**
```http
GET /health                    # Health check
GET /api/stats                 # System statistics
```

### **🎯 VBS Competition Endpoints**
```http
GET /api/dres/status           # DRES connection status
GET /api/dres/queries          # Available VBS queries
POST /api/dres/submit          # Submit KIS result
POST /api/dres/submit-batch    # Batch submission
```

---

## Project Overview

This system allows users to search a large video dataset using text, color, object, and time-based queries. It features:

- **Backend:** Python Flask API, PostgreSQL with pgvector for fast vector search, Dockerized for easy deployment.
- **Frontend:** React + Vite, modern UI for building and running complex queries.
- **Deployment:** All components run via Docker Compose for reproducibility and ease of use.
- **Video Playback:** Full video streaming and playback capabilities with proper CORS support.
- **🎯 VBS Integration:** Complete Video Browser Showdown competition support with DRES integration.
- **🔍 Enhanced Text Search:** Intelligent keyword extraction and relevance scoring.

---

## 🎯 VBS Competition Features

### **Known-Item Search (KIS) Support**
- **DRES Integration** - Direct communication with Distributed Retrieval Evaluation Server
- **Competition Mode** - Dedicated GUI for VBS query management
- **Real-time Status** - Monitor DRES connection and active queries
- **Batch Submissions** - Submit multiple results efficiently

### **Quick VBS Setup**
```bash
# Enable VBS mode
export ENABLE_VBS=true
export DRES_BASE_URL=http://your-dres-server:8080
export DRES_USERNAME=your_username
export DRES_PASSWORD=your_password

# Deploy with VBS support
./deploy.sh
```

### **VBS Workflow**
1. **Connect to DRES** - Authenticate with the evaluation server
2. **Select Query** - Choose from active VBS queries
3. **Search Videos** - Use multimodal search to find candidates
4. **Submit Results** - Send KIS results directly to DRES

📖 **Full VBS Guide:** [VBS Integration Guide](VBS_INTEGRATION_GUIDE.md)

---

## 🎨 Frontend Architecture

### **React + Vite Frontend**

The frontend is built with modern React and Vite, providing a responsive and interactive user interface for video search and playback.

#### **Technology Stack**
- **React 18** - Modern React with hooks and functional components
- **TypeScript** - Type-safe development
- **Vite** - Fast build tool and development server
- **Tailwind CSS** - Utility-first CSS framework
- **Lucide React** - Modern icon library

#### **Key Features**
- **Responsive Design** - Works on desktop, tablet, and mobile devices
- **Dark/Light Mode** - Automatic theme switching
- **Real-time Search** - Instant search results with loading states
- **Video Playback** - Full video player with timestamp seeking
- **Advanced Filtering** - Multiple search modalities in one interface
- **🎯 VBS Competition Mode** - Integrated DRES submission workflow

### **Component Architecture**

```
src/
├── App.tsx                    # Root application component
├── main.tsx                   # Application entry point
├── pages/
│   └── Home.tsx              # Main search interface
├── components/
│   ├── Header/               # Navigation and system status
│   ├── FilterPanel/          # Search filters and controls
│   ├── VideoGrid/            # Video results display
│   ├── VideoCard/            # Individual video display
│   ├── TimePicker/           # Time-based search controls
│   ├── ConnectionStatus/     # Backend connection indicator
│   ├── DRESStatus/           # 🎯 DRES connection status
│   ├── VBSCompetitionMode/   # 🎯 VBS competition interface
│   └── SearchSettingsPopup/  # Advanced search options
├── hooks/
│   ├── useSearchVideos.ts    # Search API integration
│   ├── useConnectionStatus.ts # Backend health monitoring
│   └── useDRES.ts            # 🎯 DRES integration hook
├── context/
│   └── QueryProvider.tsx     # Global state management
└── api/
    └── axios.ts              # HTTP client configuration
```

### **Core Components**

#### **1. FilterPanel Component**
The main search interface that provides multiple search modalities:

- **Text Search** - Natural language queries with semantic understanding
- **Color Search** - Color picker for visual similarity search
- **Object Search** - Search for specific objects detected in videos
- **Time-based Search** - Filter videos by duration or timestamp ranges
- **Media Upload** - Upload images/videos for similarity search

**Features:**
- Toggle-based interface for different search types
- Real-time validation and error handling
- Drag-and-drop file upload
- Color picker with RGB preview
- Time interval selection with validation

#### **2. VideoCard Component**
Displays individual video results with rich metadata:

- **Video Player** - HTML5 video player with controls
- **Metadata Display** - Score, timestamp, objects, text, colors
- **Interactive Overlays** - Full-screen player and submission modes
- **Timestamp Seeking** - Jump to specific moments in videos
- **Color Visualization** - Dominant color swatches
- **🎯 DRES Submission** - Direct submission to VBS evaluation server

**Features:**
- Automatic timestamp seeking on play
- Full-screen video overlay
- **🎯 VBS submission mode** for competition use
- Responsive design for different screen sizes
- Error handling for missing videos

#### **3. VideoGrid Component**
Responsive grid layout for displaying search results:

- **Responsive Layout** - Adapts to different screen sizes
- **Loading States** - Skeleton loading animations
- **Empty States** - Helpful messages when no results
- **Pagination** - Load more results as needed
- **🎯 VBS Integration** - Competition mode support

#### **4. 🎯 VBS Competition Mode Component**
Dedicated interface for VBS competition participation:

- **DRES Status Monitoring** - Real-time connection status
- **Active Queries List** - Browse and select available queries
- **Query Details** - View query information and instructions
- **Submission Interface** - Integrated submission workflow

### **State Management**

#### **React Hooks**
- `useState` for local component state
- `useEffect` for side effects and API calls
- `useRef` for DOM element references
- Custom hooks for reusable logic
- **🎯 `useDRES`** for VBS competition integration

#### **Context API**
- Global query state management
- Theme switching
- Connection status
- Search history
- **🎯 VBS competition state**

### **API Integration**

#### **HTTP Client**
- Axios for API requests
- Request/response interceptors
- Error handling and retry logic
- CORS configuration
- **🎯 DRES API integration**

#### **Real-time Updates**
- Connection status monitoring
- Automatic retry on connection loss
- Loading states for better UX
- Optimistic updates for search results
- **🎯 DRES status monitoring**

---

## 🔧 Backend Architecture

### **Flask API Server**

The backend is built with Python Flask, providing a robust API for video search and serving with AI-powered analysis capabilities.

#### **Technology Stack**
- **Flask** - Lightweight web framework for API development
- **PostgreSQL** - Primary database with pgvector extension
- **psycopg2** - PostgreSQL adapter for Python
- **pgvector** - Vector similarity search extension
- **Docker** - Containerized deployment
- **CORS** - Cross-origin resource sharing support
- **🎯 DRES Client** - VBS competition integration

#### **Core Features**
- **RESTful API** - Standard HTTP endpoints for all operations
- **Vector Search** - Fast similarity search using pgvector
- **Video Serving** - Direct file serving with proper headers
- **AI Integration** - YOLO, CLIP, and EasyOCR model integration
- **Error Handling** - Comprehensive error handling and logging
- **Performance** - Optimized database queries and caching
- **🎯 VBS Integration** - Complete DRES API support

### **API Architecture**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │    Backend      │    │   Database      │
│   (React)       │◄──►│  (Flask API)    │◄──►│  (PostgreSQL)   │
│                 │    │                 │    │                 │
│ • Search UI     │    │ • Search Logic  │    │ • Video Metadata│
│ • Video Player  │    │ • AI Processing │    │ • CLIP Embeddings│
│ • Filter Panel  │    │ • Video Serving │    │ • Object Data   │
│ • Responsive    │    │ • CORS Support  │    │ • Vector Search │
│ 🎯 VBS Mode     │    │ 🎯 DRES Client  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Video Files   │    │   AI Models     │    │   Analysis Data │
│                 │    │                 │    │                 │
│ • MP4 Videos    │    │ • YOLOv8 Large  │    │ • JSON Reports  │
│ • Organized     │    │ • CLIP ViT-B/32 │    │ • Frame Images  │
│ • Web-Ready     │    │ • EasyOCR       │    │ • Embeddings    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │
         │                       │
         ▼                       ▼
┌─────────────────┐    ┌─────────────────┐
│   🎯 DRES       │    │   🎯 VBS        │
│   Server        │    │   Competition   │
│                 │    │                 │
│ • Authentication│    │ • Query Mgmt    │
│ • Submissions   │    │ • Results       │
│ • Evaluation    │    │ • Scoring       │
└─────────────────┘    └─────────────────┘
```

### **Database Schema**

#### **Core Tables**

```sql
-- Videos table - stores video metadata
CREATE TABLE videos (
    video_id VARCHAR(50) PRIMARY KEY,
    original_filename VARCHAR(255),
    compressed_filename VARCHAR(255),
    duration_seconds FLOAT,
    fps FLOAT,
    compressed_file_size_bytes BIGINT,
    processing_date_utc TIMESTAMP,
    scene_change_timestamps INTEGER[],
    keyframes_analyzed_count INTEGER,
    analysis_status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Video moments table - stores analysis results for each keyframe
CREATE TABLE video_moments (
    moment_id VARCHAR(100) PRIMARY KEY,
    video_id VARCHAR(50) REFERENCES videos(video_id),
    frame_identifier VARCHAR(50),
    timestamp_seconds FLOAT,
    keyframe_image_path TEXT,
    clip_embedding vector(512),  -- pgvector extension for similarity search
    detected_object_names TEXT[],
    extracted_search_words TEXT[],
    average_color_rgb INTEGER[],
    detailed_features JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### **API Endpoints**

#### **Core Search Endpoints**
- `POST /api/search/text` - Text-based search
- `POST /api/search/color` - Color-based search
- `POST /api/search/objects` - Object-based search
- `POST /api/search/vector` - Vector similarity search
- `POST /api/search/multimodal` - Combined search
- `POST /api/search/temporal` - Time-based search

#### **Video Serving**
- `GET /api/videos/{video_id}/{filename}` - Serve video files
- `GET /api/stats` - System statistics

#### **Health & Status**
- `GET /health` - Backend health check (includes DRES status)
- `GET /` - Welcome message

#### **🎯 VBS Competition Endpoints**
- `GET /api/dres/status` - DRES connection status
- `POST /api/dres/submit` - Submit KIS result to DRES
- `GET /api/dres/queries` - Get active VBS queries
- `GET /api/dres/query/{query_id}` - Get query details
- `POST /api/dres/submit-batch` - Submit multiple results
- `GET /api/dres/history` - Get submission history

### **Video Serving**

#### **Video File Endpoint**
```
GET /api/videos/{video_id}/{filename}
```