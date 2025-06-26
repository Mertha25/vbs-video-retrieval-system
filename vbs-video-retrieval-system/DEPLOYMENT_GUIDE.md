# 🚀 VBS Video Retrieval System - Complete Deployment Guide

This comprehensive guide will walk you through deploying the VBS Video Retrieval System on your computer, including all necessary path configurations, system requirements, and troubleshooting steps.

---

## 📋 Table of Contents

1. [Prerequisites](#-prerequisites)
2. [System Architecture Overview](#-system-architecture-overview)
3. [Step-by-Step Deployment](#-step-by-step-deployment)
4. [Configuration and Path Setup](#-configuration-and-path-setup)
5. [System Verification](#-system-verification)
6. [Troubleshooting](#-troubleshooting)
7. [System Management](#-system-management)
8. [Performance Optimization](#-performance-optimization)
9. [Security Considerations](#-security-considerations)
10. [Backup and Recovery](#-backup-and-recovery)

---

## 🔧 Prerequisites

### **Required Software**

Before starting, ensure you have the following installed on your computer:

#### **1. Docker Desktop**
- **Download:** https://www.docker.com/products/docker-desktop/
- **Version:** 20.10+ (with Docker Compose v2)
- **Platforms:** Windows, macOS, Linux
- **Verify Installation:**
  ```bash
  docker --version
  docker-compose --version
  ```

#### **2. Python 3.8+**
- **Download:** https://www.python.org/downloads/
- **Verify Installation:**
  ```bash
  python --version
  pip --version
  ```

#### **3. Git**
- **Download:** https://git-scm.com/downloads
- **Verify Installation:**
  ```bash
  git --version
  ```

### **System Requirements**

- **RAM:** Minimum 8GB (16GB recommended)
- **Storage:** At least 50GB free space (for dataset and database)
- **CPU:** Multi-core processor (4+ cores recommended)
- **Network:** Internet connection for initial setup

---

## 🏗️ System Architecture Overview

The VBS Video Retrieval System consists of three main components:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │    Backend      │    │   Database      │
│   (React)       │◄──►│   (Flask API)   │◄──►│  (PostgreSQL)   │
│   Port: 80      │    │   Port: 5000    │    │   Port: 5432    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
   Web Browser              Video Files              Analysis Data
   Interface                (Dataset/)              (Embeddings, etc.)
```

### **Component Details**

1. **Frontend (React + Vite)**
   - Modern web interface for video search
   - Video playback functionality with proper CORS support
   - Real-time search and filtering

2. **Backend (Flask API)**
   - RESTful API for video search
   - Video file serving with proper headers
   - Database interaction
   - AI model integration (CLIP, YOLO)

3. **Database (PostgreSQL + pgvector)**
   - Video metadata storage
   - Vector embeddings for similarity search
   - Object detection results
   - Color analysis data

---

## 🚀 Step-by-Step Deployment

### **Step 1: Clone and Setup the Project**

```bash
# 1. Clone the repository
git clone <your-repository-url>
cd vbs-video-retrieval-system

# 2. Verify the project structure
ls -la
# Should show: docker-compose.yml, scripts/, query_server/, etc.
```

**Expected Project Structure:**
```
vbs-video-retrieval-system/
├── docker-compose.yml
├── deploy.sh (Linux/Mac)
├── deploy.bat (Windows)
├── scripts/
│   ├── download_dataset.py
│   └── import_data.py
├── query_server/
│   ├── app.py
│   ├── config.py
│   └── requirements.txt
├── database/
│   └── schema.sql
├── backend/
│   └── config/
│       └── settings.py
└── Dataset/
    └── V3C1-200/
```

### **Step 2: Prepare Your Video Dataset**

#### **Option A: Using the Automated Script (Recommended)**

1. **Place your ZIP file** in a known location
   ```bash
   # Example locations:
   # Windows: C:\V3C1_200.zip
   # Linux/Mac: /home/user/V3C1_200.zip
   ```

2. **Update the script path** in `scripts/download_dataset.py`:
   ```python
   # Windows Example:
   LOCAL_ZIP_PATH = r"C:\V3C1_200.zip"
   
   # Linux/Mac Example:
   LOCAL_ZIP_PATH = r"/home/user/V3C1_200.zip"
   ```

3. **Run the organization script**:
   ```bash
   python scripts/download_dataset.py
   ```

4. **Verify the output**:
   ```bash
   ls Dataset/V3C1-200/00001/
   # Should show: 00001.mp4, video_analysis_report.json
   ```

#### **Option B: Manual Organization**

1. **Extract your ZIP file** manually
2. **Navigate to the `V3C1_200` subfolder**
3. **Copy each video** to its corresponding folder:
   ```
   Dataset/V3C1-200/
   ├── 00001/
   │   └── 00001.mp4
   ├── 00002/
   │   └── 00002.mp4
   └── ...
   ```

### **Step 3: Start the System**

#### **A. Build and Start All Services**

```bash
# Build and start all services in detached mode
docker-compose up --build -d

# Check service status
docker-compose ps
```

**Expected Output:**
```
NAME                       IMAGE                                 COMMAND                  SERVICE    CREATED          STATUS                    PORTS
video_retrieval_backend    vbs-video-retrieval-system-backend    "python app.py"          backend    2 minutes ago    Up 2 minutes             0.0.0.0:5000->5000/tcp
video_retrieval_frontend   vbs-video-retrieval-system-frontend   "/docker-entrypoint.…"   frontend   2 minutes ago    Up 2 minutes             0.0.0.0:80->80/tcp
video_retrieval_postgres   pgvector/pgvector:pg15                "docker-entrypoint.s…"   postgres   2 minutes ago    Up 2 minutes (healthy)   0.0.0.0:5432->5432/tcp
```

#### **B. Initialize Database Schema**

```bash
# Wait for PostgreSQL to be ready (check health status)
docker-compose ps postgres

# Copy schema to container
docker cp database/schema.sql video_retrieval_postgres:/schema.sql

# Load the schema
docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -f /schema.sql
```

### **Step 4: Import Your Data**

#### **A. Use the Fixed Import Script**

The system now includes a fixed import script that handles database connections properly:

```bash
# Run the fixed import script
python import_data_fixed.py
```

**Expected Output:**
```
2025-06-21 03:03:49,328 - INFO - Found 200 video folders to import
2025-06-21 03:03:49,986 - INFO -  Video 00001: 11 moments imported
...
2025-06-21 03:06:39,263 - INFO - === IMPORT SUMMARY ===
2025-06-21 03:06:39,264 - INFO - Successful imports: 200
2025-06-21 03:06:39,264 - INFO - Failed imports: 0
2025-06-21 03:06:39,264 - INFO - Total moments imported: 3407
```

#### **B. Verify Import Success**

```bash
# Check database statistics
curl http://localhost:5000/api/stats
```

**Expected Response:**
```json
{
  "videos": 200,
  "moments": 3407,
  "moments_with_color": 3407,
  "moments_with_embedding": 3407,
  "total_duration_seconds": 123456.78,
  "average_duration_seconds": 617.28,
  "last_updated": "2025-06-21T01:16:28.123456"
}
```

### **Step 5: Access the Application**

- **Frontend:** [http://localhost](http://localhost)
- **Backend API:** [http://localhost:5000/api](http://localhost:5000/api)
- **Health Check:** [http://localhost:5000/health](http://localhost:5000/health)

---

## ✅ System Verification

### **1. Health Checks**

```bash
# Backend health
curl http://localhost:5000/health

# Expected response:
{
  "status": "ok",
  "timestamp": "2025-06-21T01:16:28.123456",
  "service": "IR Video Retrieval API"
}
```

### **2. Video Serving Test**

```bash
# Test video serving
curl -I http://localhost:5000/api/videos/00001/00001.mp4

# Expected response:
HTTP/1.1 200 OK
Content-Type: video/mp4
Content-Length: 22089971
Accept-Ranges: bytes
```

### **3. Search API Test**

```bash
# Test text search
curl -X POST http://localhost:5000/api/search/text \
  -H "Content-Type: application/json" \
  -d '{"query": "test", "limit": 3}'

# Expected response:
{
  "count": 2,
  "results": [
    {
      "id": "00105_frame_000000270000",
      "title": "Video 00105 at 270.0s",
      "video_path": "http://localhost:5000/api/videos/00105/00105.mp4",
      "duration": 2826.56,
      "score": 0.0,
      "timestamp": 270.0,
      "objects": ["person"],
      "text": ["test", "formation", "innovation"],
      "dominant_colors": [[102, 118, 126]]
    }
  ]
}
```

### **4. Frontend Verification**

1. **Open** [http://localhost](http://localhost) in your browser
2. **Verify** the interface loads without errors
3. **Test** a search query (e.g., "person", "test")
4. **Check** that videos display and can be played
5. **Verify** video playback works correctly

---

## 🔧 Troubleshooting

### **Common Issues and Solutions**

#### **1. Videos Not Playing (Black Displays)**

**Problem:** Videos show as black rectangles instead of actual content.

**Solution:**
```bash
# Check if video files exist
ls Dataset/V3C1-200/00001/
# Should show: 00001.mp4, video_analysis_report.json

# Verify video serving endpoint
curl -I http://localhost:5000/api/videos/00001/00001.mp4

# Check backend logs
docker-compose logs backend --tail=20
```

**Root Cause:** Usually incorrect video filenames in database or missing video files.

#### **2. Database Connection Issues**

**Problem:** Import script fails with database connection errors.

**Solution:**
```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# Restart the entire stack
docker-compose down
docker-compose up -d

# Use the fixed import script
python import_data_fixed.py
```

#### **3. Import Failures**

**Problem:** Data import fails or shows errors.

**Solution:**
```bash
# Use the fixed import script
python import_data_fixed.py

# Check for missing analysis reports
ls Dataset/V3C1-200/00001/video_analysis_report.json

# Verify database schema is loaded
docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -c "\dt"
```

#### **4. Docker Build Issues**

**Problem:** Docker containers fail to build or start.

**Solution:**
```bash
# Clean up Docker resources
docker system prune -a

# Rebuild without cache
docker-compose build --no-cache

# Start fresh
docker-compose down
docker-compose up --build -d
```

#### **5. Port Conflicts**

**Problem:** Services fail to start due to port conflicts.

**Solution:**
```bash
# Check what's using the ports
netstat -tulpn | grep :5000
netstat -tulpn | grep :80

# Stop conflicting services or change ports in docker-compose.yml
```

### **Useful Debugging Commands**

```bash
# Check service status
docker-compose ps

# View logs for all services
docker-compose logs

# View logs for specific service
docker-compose logs backend
docker-compose logs frontend
docker-compose logs postgres

# Access container shell
docker exec -it video_retrieval_backend bash
docker exec -it video_retrieval_postgres psql -U postgres -d videodb_creative_v2

# Check resource usage
docker stats

# Restart specific service
docker-compose restart backend
```

---

## 🛠️ System Management

### **Daily Operations**

#### **Starting the System**
```bash
cd vbs-video-retrieval-system
docker-compose up -d
```

#### **Stopping the System**
```bash
docker-compose down
```

#### **Restarting Services**
```bash
# Restart all services
docker-compose restart

# Restart specific service
docker-compose restart backend
```

#### **Viewing Logs**
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f backend
```

### **Data Management**

#### **Backup Database**
```bash
# Create backup
docker exec video_retrieval_postgres pg_dump -U postgres videodb_creative_v2 > backup.sql

# Restore from backup
docker exec -i video_retrieval_postgres psql -U postgres videodb_creative_v2 < backup.sql
```

#### **Update Dataset**
```bash
# Stop services
docker-compose down

# Update video files in Dataset/V3C1-200/

# Restart services
docker-compose up -d

# Re-import data
python import_data_fixed.py
```

---

## ⚡ Performance Optimization

### **System Tuning**

#### **Docker Resource Limits**
Edit `docker-compose.yml` to add resource limits:
```yaml
services:
  backend:
    # ... existing config ...
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2.0'
```

#### **Database Optimization**
```sql
-- Increase shared buffers
ALTER SYSTEM SET shared_buffers = '256MB';

-- Optimize for vector operations
ALTER SYSTEM SET work_mem = '64MB';
```

#### **Backend Optimization**
- Enable gunicorn for production
- Add Redis caching for frequent queries
- Implement connection pooling

### **Monitoring**

#### **System Metrics**
```bash
# Monitor resource usage
docker stats

# Check disk space
df -h

# Monitor memory usage
free -h
```

#### **Application Metrics**
- API response times
- Database query performance
- Video serving throughput

---

## 🔒 Security Considerations

### **Production Deployment**

#### **Network Security**
- Use reverse proxy (nginx) for SSL termination
- Implement rate limiting
- Restrict database access to internal network

#### **Data Security**
- Encrypt sensitive data at rest
- Implement proper authentication
- Regular security updates

#### **Container Security**
- Use non-root users in containers
- Scan images for vulnerabilities
- Keep base images updated

---

## 💾 Backup and Recovery

### **Regular Backups**

#### **Database Backup**
```bash
# Create automated backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
docker exec video_retrieval_postgres pg_dump -U postgres videodb_creative_v2 > backup_$DATE.sql
```

#### **Configuration Backup**
```bash
# Backup configuration files
tar -czf config_backup_$(date +%Y%m%d).tar.gz docker-compose.yml scripts/ database/
```

### **Disaster Recovery**

#### **Full System Restore**
```bash
# Stop services
docker-compose down

# Restore database
docker exec -i video_retrieval_postgres psql -U postgres videodb_creative_v2 < backup.sql

# Restart services
docker-compose up -d
```

---

## 📞 Support and Maintenance

### **Regular Maintenance Tasks**

1. **Weekly:**
   - Check system logs for errors
   - Monitor disk space usage
   - Verify backup completion

2. **Monthly:**
   - Update Docker images
   - Review and rotate logs
   - Performance analysis

3. **Quarterly:**
   - Security updates
   - Full system backup
   - Performance optimization review

### **Getting Help**

- Check the troubleshooting section above
- Review system logs for error messages
- Verify all prerequisites are met
- Ensure proper file permissions

---

## 🎯 Success Metrics

### **System Performance Indicators**

- **API Response Time:** < 500ms for search queries
- **Video Loading Time:** < 3 seconds for first frame
- **System Uptime:** > 99% availability
- **Database Performance:** < 100ms for vector similarity queries

### **User Experience Metrics**

- **Search Accuracy:** Relevant results in top 10
- **Video Playback:** Smooth playback without buffering
- **Interface Responsiveness:** < 200ms for UI interactions

---

## 🎨 Frontend Architecture & Deployment

### **React + Vite Frontend Overview**

The frontend is a modern React application built with Vite, providing an intuitive interface for video search and playback.

#### **Technology Stack**
- **React 18** - Modern React with hooks and functional components
- **TypeScript** - Type-safe development with full type checking
- **Vite** - Fast build tool and development server
- **Tailwind CSS** - Utility-first CSS framework for responsive design
- **Lucide React** - Modern, customizable icon library
- **Axios** - HTTP client for API communication

#### **Frontend Features**
- **Responsive Design** - Works seamlessly on desktop, tablet, and mobile
- **Dark/Light Mode** - Automatic theme switching with system preference detection
- **Real-time Search** - Instant search results with loading states and error handling
- **Video Playback** - Full HTML5 video player with timestamp seeking
- **Advanced Filtering** - Multiple search modalities in one unified interface
- **Connection Monitoring** - Real-time backend health status
- **Error Recovery** - Graceful fallback to mock data when backend is unavailable

### **Frontend Component Architecture**

```
frontend/src/
├── App.tsx                    # Root application component
├── main.tsx                   # Application entry point with React 18
├── pages/
│   └── Home.tsx              # Main search interface with layout
├── components/
│   ├── Header/               # Navigation and system status
│   │   ├── Header.tsx        # Main header with title and status
│   │   └── Header.module.css # Header styling
│   ├── FilterPanel/          # Search filters and controls
│   │   ├── FilterPanel.tsx   # Multi-modal search interface
│   │   └── FilterPanel.module.css
│   ├── VideoGrid/            # Video results display
│   │   ├── VideoGrid.tsx     # Responsive grid layout
│   │   └── VideoGrid.module.css
│   ├── VideoCard/            # Individual video display
│   │   ├── VideoCard.tsx     # Video player and metadata
│   │   └── VideoCard.module.css
│   ├── TimePicker/           # Time-based search controls
│   ├── SkeletonGrid/         # Loading state animations
│   ├── SearchSettingsPopup/  # Advanced search options
│   ├── Layout/               # Page layout components
│   └── ConnectionStatus/     # Backend connection indicator
├── hooks/
│   ├── useSearchVideos.ts    # Search API integration
│   └── useConnectionStatus.ts # Backend health monitoring
├── context/
│   └── QueryProvider.tsx     # Global state management
├── api/
│   └── axios.ts              # HTTP client configuration
├── data/
│   └── mockVideos.ts         # Fallback data for development
└── styles/
    └── index.css             # Global styles and Tailwind imports
```

### **Core Frontend Components**

#### **1. FilterPanel Component**
The main search interface providing multiple search modalities:

**Search Types:**
- **Text Search** - Natural language queries with semantic understanding
- **Color Search** - Interactive color picker for visual similarity
- **Object Search** - Multi-object selection with YOLO detection
- **Time-based Search** - Duration and timestamp range filtering
- **Media Upload** - Drag-and-drop image/video upload for similarity search

**Features:**
- Toggle-based interface for different search types
- Real-time validation and error handling
- Drag-and-drop file upload with visual feedback
- Color picker with RGB preview and hex input
- Time interval selection with validation
- Object tag management with add/remove functionality

#### **2. VideoCard Component**
Displays individual video results with rich metadata and playback:

**Video Player Features:**
- HTML5 video player with full controls
- Automatic timestamp seeking to relevant moments
- Full-screen overlay mode for better viewing
- Submission mode for competition use with timestamp editing

**Metadata Display:**
- Search relevance score with precision formatting
- Timestamp information with second precision
- Detected objects list with confidence indicators
- Extracted text with bounding box information
- Dominant color visualization with RGB swatches

**Interactive Features:**
- Click to play with automatic timestamp seeking
- Full-screen video overlay
- Timestamp submission for competition use
- Responsive design for different screen sizes

#### **3. VideoGrid Component**
Responsive grid layout for displaying search results:

**Layout Features:**
- CSS Grid with responsive breakpoints
- Automatic column adjustment based on screen size
- Consistent card spacing and alignment
- Smooth loading transitions

**State Management:**
- Loading states with skeleton animations
- Empty state handling with helpful messages
- Error state with fallback to mock data
- Pagination support for large result sets

#### **4. Header Component**
Navigation and system status information:

**Features:**
- Application branding and title
- Real-time backend connection status
- Theme toggle for dark/light mode
- Responsive design for mobile devices

### **Frontend State Management**

#### **React Hooks Architecture**
- **useState** - Local component state management
- **useEffect** - Side effects and API integration
- **useRef** - DOM element references and video controls
- **Custom Hooks** - Reusable logic for search and connection monitoring

#### **Context API for Global State**
- **QueryProvider** - Global search state and history
- **Theme Context** - Dark/light mode switching
- **Connection Context** - Backend health monitoring
- **Search History** - Recent queries and results

### **API Integration**

#### **HTTP Client Configuration**
```typescript
// api/axios.ts
import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:5000/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor for authentication
api.interceptors.request.use((config) => {
  // Add any auth headers if needed
  return config;
});

// Response interceptor for error handling
api.interceptors.response.use(
  (response) => response,
  (error) => {
    console.error('API Error:', error);
    return Promise.reject(error);
  }
);
```

#### **Search API Integration**
```typescript
// hooks/useSearchVideos.ts
export const useSearchVideos = () => {
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const searchWithCriteria = async (criteria) => {
    setLoading(true);
    setError(null);
    
    try {
      const response = await api.post('/search/multimodal', criteria);
      setResults(response.data.results);
    } catch (err) {
      setError(err.message);
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  return { results, loading, error, searchWithCriteria };
};
```

### **Frontend Deployment Process**

#### **1. Docker Frontend Configuration**
```dockerfile
# frontend/Dockerfile
FROM node:18-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy source code
COPY . .

# Build the application
RUN npm run build

# Serve with nginx
FROM nginx:alpine
COPY --from=0 /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

#### **2. Nginx Configuration**
```nginx
# nginx.conf
server {
    listen 80;
    server_name localhost;
    root /usr/share/nginx/html;
    index index.html;

    # Handle React Router
    location / {
        try_files $uri $uri/ /index.html;
    }

    # API proxy to backend
    location /api/ {
        proxy_pass http://backend:5000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # Video serving proxy
    location /videos/ {
        proxy_pass http://backend:5000/videos/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

#### **3. Environment Configuration**
```typescript
// vite.config.ts
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/api': {
        target: 'http://localhost:5000',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
});
```

### **Frontend Performance Optimization**

#### **Build Optimization**
- **Code Splitting** - Automatic route-based code splitting
- **Tree Shaking** - Unused code elimination
- **Minification** - JavaScript and CSS minification
- **Asset Optimization** - Image and font optimization

#### **Runtime Optimization**
- **Lazy Loading** - Video components loaded on demand
- **Memoization** - React.memo for expensive components
- **Debouncing** - Search input debouncing for API calls
- **Caching** - Browser caching for static assets

#### **Bundle Analysis**
```bash
# Analyze bundle size
npm run build
npx vite-bundle-analyzer dist
```

### **Frontend Testing Strategy**

#### **Unit Testing**
```bash
# Run unit tests
npm test

# Run tests with coverage
npm run test:coverage
```

#### **Integration Testing**
```bash
# Run integration tests
npm run test:integration
```

#### **E2E Testing**
```bash
# Run end-to-end tests
npm run test:e2e
```

### **Frontend Development Workflow**

#### **Development Server**
```bash
# Start development server
cd frontend
npm install
npm run dev
```

#### **Hot Reload**
- Automatic reload on file changes
- Fast refresh for React components
- TypeScript compilation on save

#### **Debugging**
- React Developer Tools integration
- Source maps for debugging
- Console logging and error tracking

### **Frontend Security Considerations**

#### **Content Security Policy**
```html
<!-- index.html -->
<meta http-equiv="Content-Security-Policy" 
      content="default-src 'self'; 
               script-src 'self' 'unsafe-inline'; 
               style-src 'self' 'unsafe-inline'; 
               img-src 'self' data: blob:; 
               media-src 'self' blob:;">
```

#### **CORS Configuration**
- Backend configured to allow frontend origin
- Proper headers for video streaming
- Secure cookie handling

#### **Input Validation**
- Client-side validation for all user inputs
- Sanitization of search queries
- File upload validation and size limits

---

## 🚀 Complete Deployment Process

### **Prerequisites**

1. **System Requirements**
   - Docker & Docker Compose installed
   - 8GB+ RAM available
   - 50GB+ free disk space
   - Modern web browser (Chrome, Firefox, Safari, Edge)

2. **Network Requirements**
   - Port 80 available for frontend
   - Port 5000 available for backend API
   - Port 5432 available for PostgreSQL

### **Step 1: Clone and Setup**

```bash
# Navigate to project directory
cd vbs-video-retrieval-system

# Verify Docker is running
docker --version
docker-compose --version
```

### **Step 2: Dataset Preparation**

```bash
# Download and organize video dataset
python scripts/download_dataset.py

# Verify dataset structure
ls -la Dataset/V3C1-200/00001/
# Should show: 00001.mp4, video_analysis_report.json, extracted_frames/
```

### **Step 3: Build and Start Services**

```bash
# Build and start all services
docker-compose up --build -d

# Check service status
docker-compose ps

# View logs if needed
docker-compose logs -f
```

### **Step 4: Database Initialization**

```bash
# Wait for PostgreSQL to be ready (30-60 seconds)
sleep 30

# Initialize database schema
docker cp database/schema.sql video_retrieval_postgres:/schema.sql
docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -f /schema.sql
```

### **Step 5: Data Import**

```bash
# Import video data and analysis
python import_data_fixed.py

# Verify import success
curl http://localhost:5000/api/stats
```

### **Step 6: System Verification**

```bash
# Test backend health
curl http://localhost:5000/health

# Test frontend accessibility
curl http://localhost

# Test video serving
curl -I http://localhost:5000/api/videos/00001/00001.mp4
```

### **Step 7: Access the Application**

- **Frontend Interface:** [http://localhost](http://localhost)
- **Backend API:** [http://localhost:5000/api](http://localhost:5000/api)
- **Health Check:** [http://localhost:5000/health](http://localhost:5000/health)

---

## 🔧 Configuration Options

### **Environment Variables**

#### **Backend Configuration**
```bash
# .env file
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=videodb_creative_v2
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

VIDEO_DATASET_PATH=/app/dataset
MODEL_PATH=/app/models
```

#### **Frontend Configuration**
```bash
# frontend/.env
VITE_API_BASE_URL=http://localhost:5000/api
VITE_VIDEO_BASE_URL=http://localhost:5000/api/videos
VITE_APP_TITLE=VBS Video Retrieval System
```

### **Docker Compose Configuration**

```yaml
# docker-compose.yml
version: '3.8'

services:
  frontend:
    build: ./frontend
    ports:
      - "80:80"
    depends_on:
      - backend
    environment:
      - NODE_ENV=production

  backend:
    build: ./backend
    ports:
      - "5000:5000"
    volumes:
      - ./Dataset:/app/dataset:ro
      - ./backend/object_detection:/app/models:ro
    environment:
      - VIDEO_DATASET_PATH=/app/dataset
      - MODEL_PATH=/app/models

  postgres:
    image: pgvector/pgvector:pg15
    environment:
      - POSTGRES_DB=videodb_creative_v2
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=your_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./database/schema.sql:/docker-entrypoint-initdb.d/schema.sql
    ports:
      - "5432:5432"

volumes:
  postgres_data:
```

---

## 🧪 Testing and Validation

### **Automated Testing**

```bash
# Run comprehensive system test
python test_system.py

# Test individual components
curl http://localhost:5000/health
curl http://localhost:5000/api/stats
curl http://localhost
```

### **Manual Testing Checklist**

#### **Frontend Testing**
- [ ] Page loads without errors
- [ ] Search interface is responsive
- [ ] Video playback works correctly
- [ ] Dark/light mode switching
- [ ] Error handling displays properly
- [ ] Connection status indicator works

#### **Backend Testing**
- [ ] API endpoints respond correctly
- [ ] Video files are served properly
- [ ] Search results are returned
- [ ] Database queries execute
- [ ] Error handling works

#### **Integration Testing**
- [ ] Search queries work end-to-end
- [ ] Video playback from search results
- [ ] Multiple search modalities work
- [ ] System handles errors gracefully

---

## 🔍 Troubleshooting Guide

### **Common Issues and Solutions**

#### **1. Frontend Not Loading**
```bash
# Check frontend container
docker-compose logs frontend

# Verify nginx configuration
docker exec video_retrieval_frontend nginx -t

# Check port availability
netstat -tulpn | grep :80
```

#### **2. Backend API Errors**
```bash
# Check backend logs
docker-compose logs backend

# Test API directly
curl http://localhost:5000/health

# Check database connection
docker exec video_retrieval_postgres pg_isready
```

#### **3. Video Playback Issues**
```bash
# Verify video files exist
ls -la Dataset/V3C1-200/00001/

# Check video serving endpoint
curl -I http://localhost:5000/api/videos/00001/00001.mp4

# Verify CORS headers
curl -H "Origin: http://localhost" -I http://localhost:5000/api/videos/00001/00001.mp4
```

#### **4. Database Connection Issues**
```bash
# Check PostgreSQL status
docker-compose ps postgres

# Test database connection
docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -c "SELECT COUNT(*) FROM videos;"

# Check schema
docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -c "\dt"
```

#### **5. Import Script Failures**
```bash
# Check import script logs
python import_data_fixed.py

# Verify analysis files exist
find Dataset/V3C1-200 -name "video_analysis_report.json" | wc -l

# Check database connectivity
python -c "import psycopg2; print('Database connection OK')"
```

### **Performance Optimization**

#### **Frontend Performance**
```bash
# Analyze bundle size
cd frontend
npm run build
npx vite-bundle-analyzer dist

# Optimize images
npm install -g imagemin-cli
imagemin src/assets/* --out-dir=dist/assets
```

#### **Backend Performance**
```bash
# Monitor resource usage
docker stats

# Check API response times
curl -w "@curl-format.txt" http://localhost:5000/api/stats

# Optimize database queries
docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -c "ANALYZE;"
```

---

## 📊 Monitoring and Maintenance

### **System Monitoring**

#### **Health Checks**
```bash
# Automated health check script
#!/bin/bash
curl -f http://localhost:5000/health || echo "Backend down"
curl -f http://localhost || echo "Frontend down"
docker-compose ps | grep -v "Up" || echo "All containers running"
```

#### **Log Monitoring**
```bash
# View real-time logs
docker-compose logs -f

# Check specific service logs
docker-compose logs backend
docker-compose logs frontend
docker-compose logs postgres
```

### **Backup and Recovery**

#### **Database Backup**
```bash
# Create database backup
docker exec video_retrieval_postgres pg_dump -U postgres videodb_creative_v2 > backup.sql

# Restore from backup
docker exec -i video_retrieval_postgres psql -U postgres videodb_creative_v2 < backup.sql
```

#### **Configuration Backup**
```bash
# Backup configuration files
tar -czf config_backup.tar.gz docker-compose.yml .env database/ backend/config/
```

### **Updates and Maintenance**

#### **System Updates**
```bash
# Update all services
docker-compose pull
docker-compose up --build -d

# Update specific service
docker-compose build backend
docker-compose up -d backend
```

#### **Data Updates**
```bash
# Re-import data after updates
python import_data_fixed.py

# Verify data integrity
curl http://localhost:5000/api/stats
```

---

## 🎯 Production Deployment

### **Production Considerations**

#### **Security Hardening**
```bash
# Use HTTPS in production
# Configure SSL certificates
# Set up proper firewall rules
# Use environment variables for secrets
```

#### **Performance Tuning**
```bash
# Optimize Docker resource limits
# Configure database connection pooling
# Set up CDN for static assets
# Implement caching strategies
```

#### **Monitoring Setup**
```bash
# Set up application monitoring
# Configure log aggregation
# Set up alerting for failures
# Monitor resource usage
```

---

## 📚 Additional Resources

### **Documentation**
- [API Documentation](API_DOCUMENTATION.md)
- [System Summary](COMPLETE_SYSTEM_SUMMARY.md)
- [Database Schema](database/schema.sql)

### **Development Tools**
- [React Developer Tools](https://chrome.google.com/webstore/detail/react-developer-tools)
- [Postman](https://www.postman.com/) for API testing
- [pgAdmin](https://www.pgadmin.org/) for database management

### **Support**
- Check logs for detailed error messages
- Verify all prerequisites are met
- Ensure proper file permissions
- Test with minimal dataset first

---

This deployment guide provides everything needed to successfully deploy and maintain the VBS Video Retrieval System with full video playback functionality. 