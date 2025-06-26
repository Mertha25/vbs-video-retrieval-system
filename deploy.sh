#!/bin/bash

# VBS Video Retrieval System - Deployment Script
# This script automates the complete deployment process

set -e  # Exit on any error

echo "🚀 VBS Video Retrieval System - Deployment Script"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Setup requirements integration
setup_requirements() {
    print_status "Setting up requirements integration..."
    if [ -f "scripts/setup_requirements.py" ]; then
        python scripts/setup_requirements.py
        print_success "Requirements integration setup completed"
    else
        print_warning "Requirements setup script not found"
    fi
}

# Check if Docker is installed
check_docker() {
    print_status "Checking Docker installation..."
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    print_success "Docker and Docker Compose are installed"
}

# Stop any existing containers
stop_existing() {
    print_status "Stopping any existing containers..."
    docker-compose down --remove-orphans 2>/dev/null || true
    print_success "Existing containers stopped"
}

# Build and start services
build_and_start() {
    print_status "Building and starting services..."
    docker-compose up --build -d
    
    # Wait for services to be ready
    print_status "Waiting for services to be ready..."
    sleep 10
    
    print_success "Services started successfully"
}

# Initialize database schema
init_database() {
    print_status "Initializing database schema..."
    
    # Wait for PostgreSQL to be ready
    print_status "Waiting for PostgreSQL to be ready..."
    for i in {1..30}; do
        if docker exec video_retrieval_postgres pg_isready -U postgres -d videodb_creative_v2 >/dev/null 2>&1; then
            break
        fi
        sleep 2
    done
    
    # Copy schema file to container
    docker cp database/schema.sql video_retrieval_postgres:/schema.sql
    
    # Load schema
    if docker exec video_retrieval_postgres psql -U postgres -d videodb_creative_v2 -f /schema.sql >/dev/null 2>&1; then
        print_success "Database schema initialized"
    else
        print_warning "Schema might already be loaded or there was an issue"
    fi
}

# Check if data import script exists
check_import_script() {
    if [ -f "scripts/import_data.py" ]; then
        print_status "Data import script found"
        return 0
    else
        print_warning "Data import script not found at scripts/import_data.py"
        return 1
    fi
}

# Import data (optional)
import_data() {
    if check_import_script; then
        print_status "Would you like to import data now? (y/n)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            print_status "Importing data..."
            python -m scripts.import_data
            print_success "Data import completed"
        else
            print_status "Skipping data import. You can run it later with: python -m scripts.import_data"
        fi
    fi
}

# Test the system
test_system() {
    print_status "Testing system components..."
    
    # Test backend health
    if curl -s http://localhost:5000/health >/dev/null; then
        print_success "Backend is responding"
    else
        print_warning "Backend health check failed"
    fi
    
    # Test frontend
    if curl -s http://localhost >/dev/null; then
        print_success "Frontend is accessible"
    else
        print_warning "Frontend accessibility check failed"
    fi
    
    # Run comprehensive test if available
    if [ -f "test_system.py" ]; then
        print_status "Running comprehensive system test..."
        python test_system.py
    fi
}

# Show status
show_status() {
    print_status "Current system status:"
    docker-compose ps
    
    echo ""
    print_status "Access URLs:"
    echo "  Frontend: http://localhost"
    echo "  Backend API: http://localhost:5000/api"
    echo "  Health Check: http://localhost:5000/health"
    echo "  System Stats: http://localhost:5000/api/stats"
}

# Main deployment function
main() {
    echo ""
    print_status "Starting deployment process..."
    
    setup_requirements
    check_docker
    stop_existing
    build_and_start
    init_database
    import_data
    test_system
    
    echo ""
    print_success "Deployment completed successfully!"
    show_status
    
    echo ""
    print_status "Next steps:"
    echo "  1. Open http://localhost in your browser"
    echo "  2. If you haven't imported data yet, run: python -m scripts.import_data"
    echo "  3. Check logs if needed: docker-compose logs"
    echo "  4. Stop services: docker-compose down"
}

# Handle script arguments
case "${1:-}" in
    "stop")
        print_status "Stopping all services..."
        docker-compose down
        print_success "Services stopped"
        ;;
    "restart")
        print_status "Restarting services..."
        docker-compose restart
        print_success "Services restarted"
        ;;
    "logs")
        print_status "Showing logs..."
        docker-compose logs -f
        ;;
    "status")
        show_status
        ;;
    "test")
        test_system
        ;;
    "requirements")
        setup_requirements
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  (no args)  - Full deployment"
        echo "  stop       - Stop all services"
        echo "  restart    - Restart services"
        echo "  logs       - Show logs"
        echo "  status     - Show status"
        echo "  test       - Test system"
        echo "  requirements - Setup requirements integration"
        echo "  help       - Show this help"
        ;;
    *)
        main
        ;;
esac 