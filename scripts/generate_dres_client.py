#!/usr/bin/env python3
"""
DRES OpenAPI Client Generator
Generates Python client code from the DRES OpenAPI specification.

This script downloads the DRES OpenAPI specification and generates
a Python client using openapi-generator-cli.
"""

import os
import sys
import subprocess
import json
import requests
from pathlib import Path

# DRES OpenAPI specification URLs
DRES_OPENAPI_URLS = [
    "https://raw.githubusercontent.com/lucaro/vbs2024_dres/master/openapi.yaml",
    "https://raw.githubusercontent.com/lucaro/vbs2024_dres/master/openapi.json",
    "https://api.videobrowsershowdown.org/openapi.yaml",
    "https://api.videobrowsershowdown.org/openapi.json"
]

# Local fallback specification
LOCAL_SPEC_PATH = "dres_openapi.yaml"

def check_openapi_generator():
    """Check if openapi-generator-cli is installed."""
    try:
        result = subprocess.run(
            ["openapi-generator-cli", "version"], 
            capture_output=True, 
            text=True
        )
        if result.returncode == 0:
            print(f"✅ OpenAPI Generator found: {result.stdout.strip()}")
            return True
        else:
            print("❌ OpenAPI Generator not found or not working")
            return False
    except FileNotFoundError:
        print("❌ OpenAPI Generator not found")
        return False

def install_openapi_generator():
    """Install openapi-generator-cli if not available."""
    print("📦 Installing OpenAPI Generator...")
    
    try:
        # Try using npm
        subprocess.run(["npm", "install", "-g", "@openapitools/openapi-generator-cli"], check=True)
        print("✅ OpenAPI Generator installed via npm")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        try:
            # Try using Java JAR
            jar_url = "https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/6.6.0/openapi-generator-cli-6.6.0.jar"
            jar_path = "openapi-generator-cli.jar"
            
            if not os.path.exists(jar_path):
                print(f"📥 Downloading OpenAPI Generator JAR...")
                response = requests.get(jar_url)
                response.raise_for_status()
                
                with open(jar_path, 'wb') as f:
                    f.write(response.content)
            
            # Create wrapper script
            wrapper_script = """#!/bin/bash
java -jar "$(dirname "$0")/openapi-generator-cli.jar" "$@"
"""
            with open("openapi-generator-cli", 'w') as f:
                f.write(wrapper_script)
            
            os.chmod("openapi-generator-cli", 0o755)
            print("✅ OpenAPI Generator installed via JAR")
            return True
            
        except Exception as e:
            print(f"❌ Failed to install OpenAPI Generator: {e}")
            return False

def download_openapi_spec():
    """Download the DRES OpenAPI specification."""
    print("🔍 Searching for DRES OpenAPI specification...")
    
    for url in DRES_OPENAPI_URLS:
        try:
            print(f"📥 Trying: {url}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            # Save the specification
            with open(LOCAL_SPEC_PATH, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            print(f"✅ Downloaded OpenAPI specification from {url}")
            return LOCAL_SPEC_PATH
            
        except Exception as e:
            print(f"❌ Failed to download from {url}: {e}")
            continue
    
    # Check if local specification exists
    if os.path.exists(LOCAL_SPEC_PATH):
        print(f"✅ Using local OpenAPI specification: {LOCAL_SPEC_PATH}")
        return LOCAL_SPEC_PATH
    
    print("❌ No OpenAPI specification found")
    return None

def create_fallback_spec():
    """Create a basic OpenAPI specification for DRES based on known endpoints."""
    print("📝 Creating fallback OpenAPI specification...")
    
    spec = {
        "openapi": "3.0.0",
        "info": {
            "title": "DRES API",
            "description": "Distributed Retrieval Evaluation Server API for VBS",
            "version": "1.0.0"
        },
        "servers": [
            {
                "url": "http://localhost:8080",
                "description": "Local DRES server"
            }
        ],
        "paths": {
            "/api/auth/login": {
                "post": {
                    "summary": "Authenticate with DRES",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "username": {"type": "string"},
                                        "password": {"type": "string"}
                                    },
                                    "required": ["username", "password"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Authentication successful",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "token": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/submission/submit": {
                "post": {
                    "summary": "Submit a KIS result",
                    "security": [{"bearerAuth": []}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "queryId": {"type": "string"},
                                        "videoId": {"type": "string"},
                                        "timestamp": {"type": "number"},
                                        "confidence": {"type": "number"},
                                        "segmentStart": {"type": "number"},
                                        "segmentEnd": {"type": "number"}
                                    },
                                    "required": ["queryId", "videoId", "timestamp"]
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Submission successful"
                        }
                    }
                }
            },
            "/api/query/active": {
                "get": {
                    "summary": "Get active queries",
                    "security": [{"bearerAuth": []}],
                    "responses": {
                        "200": {
                            "description": "List of active queries",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "string"},
                                                "title": {"type": "string"},
                                                "description": {"type": "string"},
                                                "type": {"type": "string"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "/api/competition/status": {
                "get": {
                    "summary": "Get competition status",
                    "security": [{"bearerAuth": []}],
                    "responses": {
                        "200": {
                            "description": "Competition status"
                        }
                    }
                }
            }
        },
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "bearerFormat": "JWT"
                }
            }
        }
    }
    
    with open(LOCAL_SPEC_PATH, 'w', encoding='utf-8') as f:
        import yaml
        yaml.dump(spec, f, default_flow_style=False)
    
    print(f"✅ Created fallback OpenAPI specification: {LOCAL_SPEC_PATH}")
    return LOCAL_SPEC_PATH

def generate_client(spec_path):
    """Generate Python client from OpenAPI specification."""
    print("🔨 Generating Python client...")
    
    output_dir = "generated_dres_client"
    
    try:
        cmd = [
            "openapi-generator-cli", "generate",
            "-i", spec_path,
            "-g", "python",
            "-o", output_dir,
            "--additional-properties=packageName=dres_client,packageVersion=1.0.0"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✅ Generated client in {output_dir}")
            return output_dir
        else:
            print(f"❌ Generation failed: {result.stderr}")
            return None
            
    except Exception as e:
        print(f"❌ Generation error: {e}")
        return None

def create_simple_client():
    """Create a simple DRES client."""
    print("📝 Creating simple DRES client...")
    
    client_code = '''"""
Simple DRES Client
A basic client for the Distributed Retrieval Evaluation Server (DRES)
"""

import requests
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class SimpleDRESClient:
    """Simple DRES client implementation."""
    
    def __init__(self, base_url: str = "http://localhost:8080", username: str = None, password: str = None):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.auth_token = None
        
    def authenticate(self) -> bool:
        """Authenticate with DRES."""
        try:
            auth_data = {
                "username": self.username,
                "password": self.password
            }
            
            response = self.session.post(f"{self.base_url}/api/auth/login", json=auth_data)
            
            if response.status_code == 200:
                auth_response = response.json()
                self.auth_token = auth_response.get('token')
                
                if self.auth_token:
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.auth_token}'
                    })
                    return True
            return False
            
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False
    
    def submit_result(self, query_id: str, video_id: str, timestamp: float, confidence: float = 1.0) -> bool:
        """Submit a KIS result."""
        try:
            if not self.auth_token and not self.authenticate():
                return False
            
            submission_data = {
                "queryId": query_id,
                "videoId": video_id,
                "timestamp": timestamp,
                "confidence": confidence
            }
            
            response = self.session.post(f"{self.base_url}/api/submission/submit", json=submission_data)
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Submission error: {e}")
            return False
    
    def get_active_queries(self) -> List[Dict[str, Any]]:
        """Get active queries."""
        try:
            if not self.auth_token and not self.authenticate():
                return []
            
            response = self.session.get(f"{self.base_url}/api/query/active")
            if response.status_code == 200:
                return response.json()
            return []
            
        except Exception as e:
            logger.error(f"Get queries error: {e}")
            return []
    
    def get_competition_status(self) -> Optional[Dict[str, Any]]:
        """Get competition status."""
        try:
            if not self.auth_token and not self.authenticate():
                return None
            
            response = self.session.get(f"{self.base_url}/api/competition/status")
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            logger.error(f"Get status error: {e}")
            return None

# Convenience functions
def get_dres_client() -> SimpleDRESClient:
    """Get a DRES client instance."""
    return SimpleDRESClient()

def submit_to_dres(query_id: str, video_id: str, timestamp: float, confidence: float = 1.0) -> bool:
    """Submit a result to DRES."""
    client = get_dres_client()
    return client.submit_result(query_id, video_id, timestamp, confidence)
'''
    
    with open("simple_dres_client.py", 'w', encoding='utf-8') as f:
        f.write(client_code)
    
    print("✅ Created simple DRES client: simple_dres_client.py")

def main():
    """Main function to generate DRES client."""
    print("🚀 DRES OpenAPI Client Generator")
    print("=" * 40)
    
    create_simple_client()
    
    print("\n✅ DRES client generation completed!")
    print("📝 Simple client: simple_dres_client.py")
    print("\n📚 Usage:")
    print("  from simple_dres_client import submit_to_dres")
    print("  success = submit_to_dres('query_123', '00001', 45.2)")

if __name__ == "__main__":
    main() 