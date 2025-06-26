# 🎯 VBS Competition Integration Guide

This guide explains how to use the Video Browser Showdown (VBS) integration features in your video retrieval system.

## 📋 Overview

The VBS integration provides:
- **DRES Client** - Communication with the Distributed Retrieval Evaluation Server
- **Competition Mode** - GUI for managing VBS queries and submissions
- **Known-Item Search (KIS)** - Submit video moments to DRES
- **Real-time Status** - Monitor DRES connection and active queries

## 🚀 Quick Start

### 1. Enable VBS Mode

Set the environment variable to enable VBS features:

```bash
export ENABLE_VBS=true
# or in Windows
set ENABLE_VBS=true
```

### 2. Configure DRES Connection

Set your DRES server details:

```bash
export DRES_BASE_URL=http://your-dres-server:8080
export DRES_USERNAME=your_username
export DRES_PASSWORD=your_password
```

### 3. Start the System

```bash
# Deploy with VBS support
./deploy.sh

# Or manually
docker-compose up -d
```

## 🔧 Backend DRES Integration

### DRES Client

The backend includes a comprehensive DRES client (`dres_client.py`):

```python
from dres_client import get_dres_client, submit_to_dres

# Get client instance
client = get_dres_client()

# Submit a KIS result
success = submit_to_dres(
    query_id="query_123",
    video_id="00001", 
    timestamp=45.2,
    confidence=0.95
)
```

### API Endpoints

The backend provides these DRES-related endpoints:

#### `GET /api/dres/status`
Get DRES connection status and competition information.

**Response:**
```json
{
  "connected": true,
  "timestamp": "2024-01-15T10:30:00Z",
  "competition": {
    "name": "VBS 2024",
    "status": "active"
  },
  "active_queries_count": 5
}
```

#### `POST /api/dres/submit`
Submit a Known-Item Search result to DRES.

**Request:**
```json
{
  "query_id": "query_123",
  "video_id": "00001",
  "timestamp": 45.2,
  "confidence": 0.95,
  "segment_start": 44.0,
  "segment_end": 46.0
}
```

**Response:**
```json
{
  "success": true,
  "message": "Successfully submitted result for query query_123",
  "submission": {
    "query_id": "query_123",
    "video_id": "00001",
    "timestamp": 45.2,
    "confidence": 0.95
  }
}
```

#### `GET /api/dres/queries`
Get list of active queries from DRES.

**Response:**
```json
{
  "queries": [
    {
      "id": "query_123",
      "title": "Find person walking",
      "description": "Locate a person walking in the video",
      "type": "KIS"
    }
  ],
  "count": 1
}
```

#### `GET /api/dres/query/{query_id}`
Get detailed information about a specific query.

#### `POST /api/dres/submit-batch`
Submit multiple results at once.

**Request:**
```json
[
  {
    "query_id": "query_123",
    "video_id": "00001",
    "timestamp": 45.2,
    "confidence": 0.95
  },
  {
    "query_id": "query_124", 
    "video_id": "00002",
    "timestamp": 120.5,
    "confidence": 0.87
  }
]
```

## 🎨 Frontend VBS Integration

### VBS Competition Mode Component

The frontend includes a dedicated VBS competition mode component that provides:

- **DRES Status Monitoring** - Real-time connection status
- **Active Queries List** - Browse and select available queries
- **Query Details** - View query information and instructions
- **Submission Interface** - Integrated submission workflow

### Usage in Components

#### VideoCard with DRES Support

```tsx
import { VideoCard } from './components/VideoCard/VideoCard';

<VideoCard
  video={videoData}
  currentQueryId="query_123"
  enableDRES={true}
  onSubmit={(id, timestamp) => {
    // Custom submission logic (optional)
  }}
/>
```

#### VBS Competition Mode

```tsx
import { VBSCompetitionMode } from './components/VBSCompetitionMode/VBSCompetitionMode';

<VBSCompetitionMode
  enableDRES={true}
  currentQueryId={selectedQueryId}
  onQuerySelect={(queryId) => {
    setSelectedQueryId(queryId);
  }}
/>
```

### DRES Hook

Use the `useDRES` hook for DRES operations:

```tsx
import { useDRES } from './hooks/useDRES';

const {
  status,
  loading,
  error,
  submitResult,
  getActiveQueries,
  isConnected
} = useDRES();

// Submit a result
const handleSubmit = async () => {
  const result = await submitResult({
    query_id: "query_123",
    video_id: "00001",
    timestamp: 45.2,
    confidence: 0.95
  });
  
  if (result.success) {
    console.log("Submission successful!");
  }
};
```

## 🔄 Workflow

### 1. Competition Setup

1. **Connect to DRES** - Ensure your system can reach the DRES server
2. **Authenticate** - Provide valid credentials
3. **Check Status** - Verify connection and competition status

### 2. Query Selection

1. **Load Active Queries** - Fetch available queries from DRES
2. **Select Query** - Choose the query you want to work on
3. **Review Details** - Read query description and requirements

### 3. Video Search

1. **Search Interface** - Use the existing search functionality
2. **Find Candidates** - Locate potential video moments
3. **Review Results** - Examine video content and metadata

### 4. Submission

1. **Select Video** - Choose the best matching video
2. **Adjust Timestamp** - Fine-tune the exact moment
3. **Submit to DRES** - Send result to the evaluation server
4. **Confirm Success** - Verify submission was accepted

## 🛠️ Configuration

### Environment Variables

```bash
# DRES Configuration
DRES_BASE_URL=http://localhost:8080
DRES_USERNAME=vbs_user
DRES_PASSWORD=vbs_password

# VBS Mode
ENABLE_VBS=true

# Backend Configuration
API_URL_BASE=http://localhost:5000
VIDEO_DATASET_PATH=/app/dataset
```

### Docker Configuration

Add to your `docker-compose.yml`:

```yaml
services:
  backend:
    environment:
      - DRES_BASE_URL=${DRES_BASE_URL:-http://localhost:8080}
      - DRES_USERNAME=${DRES_USERNAME:-vbs_user}
      - DRES_PASSWORD=${DRES_PASSWORD:-vbs_password}
      - ENABLE_VBS=${ENABLE_VBS:-false}
```

## 🧪 Testing

### Test DRES Connection

```bash
# Test backend DRES connection
curl http://localhost:5000/api/dres/status

# Test submission
curl -X POST http://localhost:5000/api/dres/submit \
  -H "Content-Type: application/json" \
  -d '{
    "query_id": "test_query",
    "video_id": "00001",
    "timestamp": 45.2,
    "confidence": 0.95
  }'
```

### Frontend Testing

1. **Enable VBS Mode** - Set `enableDRES={true}` in components
2. **Check Status** - Verify DRES connection indicator
3. **Test Submission** - Submit a test result through the UI

## 📊 Monitoring

### Backend Health Check

The health endpoint includes DRES status:

```bash
curl http://localhost:5000/health
```

**Response:**
```json
{
  "status": "ok",
  "timestamp": "2024-01-15T10:30:00Z",
  "service": "IR Video Retrieval API",
  "dres_available": true
}
```

### Frontend Status Indicators

- **DRES Status Component** - Shows connection status in header
- **Competition Mode** - Displays active queries and selected query
- **Submission Feedback** - Real-time submission result notifications

## 🔍 Troubleshooting

### Common Issues

#### DRES Connection Failed

**Symptoms:** Status shows "DRES Disconnected"

**Solutions:**
1. Check DRES server is running
2. Verify network connectivity
3. Confirm credentials are correct
4. Check firewall settings

#### Authentication Errors

**Symptoms:** "DRES authentication failed" errors

**Solutions:**
1. Verify username/password
2. Check DRES server authentication settings
3. Ensure proper API endpoints

#### Submission Failures

**Symptoms:** Submissions return error responses

**Solutions:**
1. Verify query ID is valid
2. Check video ID format
3. Ensure timestamp is within video duration
4. Confirm confidence score is 0.0-1.0

### Debug Mode

Enable debug logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Logs

Check backend logs for DRES-related messages:

```bash
docker-compose logs backend | grep -i dres
```

## 📚 API Reference

### DRES Client Methods

```python
class DRESClient:
    def authenticate(self) -> bool
    def submit_result(self, query_id: str, video_id: str, timestamp: float, confidence: float = 1.0) -> bool
    def get_active_queries(self) -> List[Dict[str, Any]]
    def get_query_info(self, query_id: str) -> Optional[Dict[str, Any]]
    def get_competition_status(self) -> Optional[Dict[str, Any]]
    def submit_multiple_results(self, results: List[Dict[str, Any]]) -> Dict[str, bool]
    def test_connection(self) -> bool
    def get_submission_history(self, query_id: str = None) -> List[Dict[str, Any]]
```

### Frontend Hook Methods

```typescript
const {
  // State
  status: DRESStatus,
  loading: boolean,
  error: string | null,
  
  // Actions
  getStatus: () => Promise<DRESStatus | null>,
  submitResult: (submission: DRESSubmission) => Promise<DRESSubmissionResult>,
  submitBatch: (submissions: DRESSubmission[]) => Promise<DRESBatchResult>,
  getActiveQueries: () => Promise<DRESQuery[]>,
  getQueryInfo: (queryId: string) => Promise<any>,
  getSubmissionHistory: (queryId?: string) => Promise<any[]>,
  testConnection: () => Promise<boolean>,
  clearError: () => void,
  
  // Computed
  isConnected: boolean,
  hasActiveQueries: boolean
} = useDRES();
```

## 🎯 Competition Best Practices

### 1. Query Understanding
- Read query descriptions carefully
- Understand the search requirements
- Note any specific constraints or preferences

### 2. Efficient Search
- Use multiple search modalities (text, color, objects)
- Leverage the multimodal search capabilities
- Refine searches based on initial results

### 3. Precise Timestamps
- Use the video player to find exact moments
- Consider context before and after the target moment
- Verify the timestamp accuracy

### 4. Confidence Scoring
- Use confidence scores to indicate result quality
- Higher confidence for clear matches
- Lower confidence for ambiguous results

### 5. Multiple Submissions
- Submit multiple candidates if allowed
- Use batch submission for efficiency
- Track submission history

## 🔗 Resources

- [VBS Official Website](https://www.videobrowsershowdown.org)
- [DRES Documentation](https://github.com/lucaro/vbs2024_dres)
- [VBS Competition Rules](https://www.videobrowsershowdown.org/rules)
- [Known-Item Search Guide](https://www.videobrowsershowdown.org/kis)

---

This integration guide provides everything needed to use your video retrieval system in VBS competitions with full DRES support. 