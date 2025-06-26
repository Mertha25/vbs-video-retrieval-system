"""
DRES (Distributed Retrieval Evaluation Server) Client
For Video Browser Showdown (VBS) competition integration

This module provides a client for communicating with the DRES server
to submit Known-Item Search (KIS) results and handle competition queries.
"""

import requests
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DRESClient:
    """
    Client for communicating with the DRES (Distributed Retrieval Evaluation Server)
    used in the Video Browser Showdown competition.
    """
    
    def __init__(self, base_url: str = None, username: str = None, password: str = None):
        """
        Initialize the DRES client.
        
        Args:
            base_url: DRES server base URL (defaults to environment variable)
            username: DRES username (defaults to environment variable)
            password: DRES password (defaults to environment variable)
        """
        self.base_url = base_url or os.environ.get('DRES_BASE_URL', 'http://localhost:8080')
        self.username = username or os.environ.get('DRES_USERNAME', 'vbs_user')
        self.password = password or os.environ.get('DRES_PASSWORD', 'vbs_password')
        
        # Session for maintaining authentication
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        # Authentication token
        self.auth_token = None
        
        logger.info(f"DRES Client initialized for {self.base_url}")
    
    def authenticate(self) -> bool:
        """
        Authenticate with the DRES server.
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        try:
            auth_url = f"{self.base_url}/api/auth/login"
            auth_data = {
                "username": self.username,
                "password": self.password
            }
            
            response = self.session.post(auth_url, json=auth_data)
            
            if response.status_code == 200:
                auth_response = response.json()
                self.auth_token = auth_response.get('token')
                
                if self.auth_token:
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.auth_token}'
                    })
                    logger.info("DRES authentication successful")
                    return True
                else:
                    logger.error("No token received from DRES")
                    return False
            else:
                logger.error(f"DRES authentication failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"DRES authentication error: {e}")
            return False
    
    def submit_result(self, 
                     query_id: str, 
                     video_id: str, 
                     timestamp: float,
                     confidence: float = 1.0,
                     segment_start: float = None,
                     segment_end: float = None) -> bool:
        """
        Submit a Known-Item Search (KIS) result to DRES.
        
        Args:
            query_id: The query identifier from DRES
            video_id: The video identifier (e.g., "00001")
            timestamp: The timestamp in seconds where the target moment occurs
            confidence: Confidence score (0.0 to 1.0)
            segment_start: Start time of the segment (optional)
            segment_end: End time of the segment (optional)
            
        Returns:
            bool: True if submission successful, False otherwise
        """
        try:
            # Ensure we're authenticated
            if not self.auth_token and not self.authenticate():
                logger.error("Cannot submit result: not authenticated with DRES")
                return False
            
            submit_url = f"{self.base_url}/api/submission/submit"
            
            # Prepare submission data
            submission_data = {
                "queryId": query_id,
                "videoId": video_id,
                "timestamp": timestamp,
                "confidence": confidence
            }
            
            # Add segment information if provided
            if segment_start is not None and segment_end is not None:
                submission_data.update({
                    "segmentStart": segment_start,
                    "segmentEnd": segment_end
                })
            
            response = self.session.post(submit_url, json=submission_data)
            
            if response.status_code == 200:
                logger.info(f"Successfully submitted result for query {query_id}: video {video_id} at {timestamp}s")
                return True
            else:
                logger.error(f"DRES submission failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"DRES submission error: {e}")
            return False
    
    def get_query_info(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a specific query from DRES.
        
        Args:
            query_id: The query identifier
            
        Returns:
            Dict containing query information or None if failed
        """
        try:
            if not self.auth_token and not self.authenticate():
                return None
            
            query_url = f"{self.base_url}/api/query/{query_id}"
            response = self.session.get(query_url)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get query info: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting query info: {e}")
            return None
    
    def get_active_queries(self) -> List[Dict[str, Any]]:
        """
        Get list of active queries from DRES.
        
        Returns:
            List of active queries
        """
        try:
            if not self.auth_token and not self.authenticate():
                return []
            
            queries_url = f"{self.base_url}/api/query/active"
            response = self.session.get(queries_url)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get active queries: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting active queries: {e}")
            return []
    
    def get_competition_status(self) -> Optional[Dict[str, Any]]:
        """
        Get current competition status from DRES.
        
        Returns:
            Dict containing competition status or None if failed
        """
        try:
            if not self.auth_token and not self.authenticate():
                return None
            
            status_url = f"{self.base_url}/api/competition/status"
            response = self.session.get(status_url)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get competition status: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting competition status: {e}")
            return None
    
    def submit_multiple_results(self, results: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Submit multiple results at once.
        
        Args:
            results: List of result dictionaries with keys:
                    - query_id: str
                    - video_id: str
                    - timestamp: float
                    - confidence: float (optional)
                    - segment_start: float (optional)
                    - segment_end: float (optional)
        
        Returns:
            Dict mapping query_id to success status
        """
        submission_results = {}
        
        for result in results:
            query_id = result.get('query_id')
            if not query_id:
                logger.warning("Skipping result without query_id")
                continue
                
            success = self.submit_result(
                query_id=query_id,
                video_id=result.get('video_id'),
                timestamp=result.get('timestamp'),
                confidence=result.get('confidence', 1.0),
                segment_start=result.get('segment_start'),
                segment_end=result.get('segment_end')
            )
            
            submission_results[query_id] = success
        
        return submission_results
    
    def test_connection(self) -> bool:
        """
        Test the connection to the DRES server.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            # Try to authenticate
            if self.authenticate():
                logger.info("DRES connection test successful")
                return True
            else:
                logger.error("DRES connection test failed: authentication failed")
                return False
                
        except Exception as e:
            logger.error(f"DRES connection test failed: {e}")
            return False
    
    def get_submission_history(self, query_id: str = None) -> List[Dict[str, Any]]:
        """
        Get submission history from DRES.
        
        Args:
            query_id: Optional query ID to filter results
            
        Returns:
            List of submission history entries
        """
        try:
            if not self.auth_token and not self.authenticate():
                return []
            
            history_url = f"{self.base_url}/api/submission/history"
            if query_id:
                history_url += f"?queryId={query_id}"
            
            response = self.session.get(history_url)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get submission history: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting submission history: {e}")
            return []

# Global DRES client instance
dres_client = None

def get_dres_client() -> DRESClient:
    """
    Get or create the global DRES client instance.
    
    Returns:
        DRESClient instance
    """
    global dres_client
    if dres_client is None:
        dres_client = DRESClient()
    return dres_client

def submit_to_dres(query_id: str, video_id: str, timestamp: float, confidence: float = 1.0) -> bool:
    """
    Convenience function to submit a result to DRES.
    
    Args:
        query_id: The query identifier
        video_id: The video identifier
        timestamp: The timestamp in seconds
        confidence: Confidence score (0.0 to 1.0)
        
    Returns:
        bool: True if submission successful, False otherwise
    """
    client = get_dres_client()
    return client.submit_result(query_id, video_id, timestamp, confidence)

def test_dres_connection() -> bool:
    """
    Convenience function to test DRES connection.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    client = get_dres_client()
    return client.test_connection() 