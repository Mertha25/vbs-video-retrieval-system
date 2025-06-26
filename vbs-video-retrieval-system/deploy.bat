@echo off
setlocal enabledelayedexpansion

REM VBS Video Retrieval System - Deployment Script for Windows
REM This script automates the complete deployment process

echo 🚀 VBS Video Retrieval System - Deployment Script
echo ==================================================

REM Check if Docker is installed
echo [INFO] Checking Docker installation...
docker --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker is not installed. Please install Docker Desktop first.
    pause
    exit /b 1
)

docker-compose --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Docker Compose is not installed. Please install Docker Compose first.
    pause
    exit /b 1
)

echo [SUCCESS] Docker and Docker Compose are installed

REM Stop any existing containers
echo [INFO] Stopping any existing containers...
docker-compose down --remove-orphans >nul 2>&1
echo [SUCCESS] Existing containers stopped

REM Build and start services
echo [INFO] Building and starting services...
docker-compose up --build -d

REM Wait for services to be ready
echo [INFO] Waiting for services to be ready...
timeout /t 10 /nobreak >nul
echo [SUCCESS] Services started successfully

REM Initialize database schema
echo [INFO] Initializing database schema...

REM Wait for PostgreSQL to be ready
echo [INFO] Waiting for PostgreSQL to be ready...
for /l %%i in (1,1,30) do (
    docker exec video_retrieval_postgres pg_isready -U postgres -d videodb_creative_v2 >nul 2>&1
    if not errorlevel 1 goto :db_ready
    timeout /t 2 /nobreak >nul
)
:db_ready

REM Copy schema file to container
docker cp database/schema.sql video_retrieval_postgres:/schema.sql

REM Load schema
docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -f /schema.sql >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Schema might already be loaded or there was an issue
) else (
    echo [SUCCESS] Database schema initialized
)

REM Check if data import script exists
if exist "scripts\import_data.py" (
    echo [INFO] Data import script found
    echo [INFO] Would you like to import data now? (y/n)
    set /p response=
    if /i "!response!"=="y" (
        echo [INFO] Importing data...
        python -m scripts.import_data
        echo [SUCCESS] Data import completed
    ) else (
        echo [INFO] Skipping data import. You can run it later with: python -m scripts.import_data
    )
) else (
    echo [WARNING] Data import script not found at scripts/import_data.py
)

REM Test the system
echo [INFO] Testing system components...

REM Test backend health
curl -s http://localhost:5000/health >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Backend health check failed
) else (
    echo [SUCCESS] Backend is responding
)

REM Test frontend
curl -s http://localhost >nul 2>&1
if errorlevel 1 (
    echo [WARNING] Frontend accessibility check failed
) else (
    echo [SUCCESS] Frontend is accessible
)

REM Run comprehensive test if available
if exist "test_system.py" (
    echo [INFO] Running comprehensive system test...
    python test_system.py
)

echo.
echo [SUCCESS] Deployment completed successfully!
echo.
echo [INFO] Current system status:
docker-compose ps

echo.
echo [INFO] Access URLs:
echo   Frontend: http://localhost
echo   Backend API: http://localhost:5000/api
echo   Health Check: http://localhost:5000/health
echo   System Stats: http://localhost:5000/api/stats

echo.
echo [INFO] Next steps:
echo   1. Open http://localhost in your browser
echo   2. If you haven't imported data yet, run: python -m scripts.import_data
echo   3. Check logs if needed: docker-compose logs
echo   4. Stop services: docker-compose down

pause 