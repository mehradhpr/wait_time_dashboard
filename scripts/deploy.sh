"""
#!/bin/bash

# Healthcare Analytics Deployment Script
# Automated deployment for production environments

set -e  # Exit on any error

# Configuration
APP_NAME="healthcare-analytics"
DEPLOY_DIR="/opt/healthcare-analytics"
BACKUP_DIR="/opt/backups/healthcare-analytics"
LOG_FILE="/var/log/healthcare-analytics-deploy.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a $LOG_FILE
}

error() {
    log "${RED}ERROR: $1${NC}"
    exit 1
}

success() {
    log "${GREEN}SUCCESS: $1${NC}"
}

warning() {
    log "${YELLOW}WARNING: $1${NC}"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   error "This script should not be run as root for security reasons"
fi

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."
    
    # Check Python version
    if ! python3 --version | grep -q "3.[89]"; then
        error "Python 3.8+ is required"
    fi
    
    # Check PostgreSQL
    if ! command -v psql &> /dev/null; then
        error "PostgreSQL client is required"
    fi
    
    # Check Git
    if ! command -v git &> /dev/null; then
        error "Git is required"
    fi
    
    success "Prerequisites check passed"
}

# Create backup of current deployment
backup_current_deployment() {
    if [ -d "$DEPLOY_DIR" ]; then
        log "Creating backup of current deployment..."
        BACKUP_NAME="backup-$(date +%Y%m%d-%H%M%S)"
        mkdir -p "$BACKUP_DIR"
        cp -r "$DEPLOY_DIR" "$BACKUP_DIR/$BACKUP_NAME"
        success "Backup created at $BACKUP_DIR/$BACKUP_NAME"
    fi
}

# Deploy application
deploy_application() {
    log "Deploying application..."
    
    # Create deployment directory
    sudo mkdir -p "$DEPLOY_DIR"
    sudo chown $USER:$USER "$DEPLOY_DIR"
    
    # Clone or update repository
    if [ -d "$DEPLOY_DIR/.git" ]; then
        cd "$DEPLOY_DIR"
        git pull origin main
    else
        git clone https://github.com/healthcare-analytics/wait-times-dashboard.git "$DEPLOY_DIR"
        cd "$DEPLOY_DIR"
    fi
    
    success "Application code deployed"
}

# Setup Python environment
setup_python_environment() {
    log "Setting up Python environment..."
    
    cd "$DEPLOY_DIR"
    
    # Create virtual environment
    python3 -m venv venv
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    pip install -r requirements.txt
    
    success "Python environment configured"
}

# Setup database
setup_database() {
    log "Setting up database..."
    
    cd "$DEPLOY_DIR"
    source venv/bin/activate
    
    # Run database setup
    python scripts/setup_database.py
    
    success "Database setup completed"
}

# Load initial data
load_initial_data() {
    log "Loading initial data..."
    
    cd "$DEPLOY_DIR"
    source venv/bin/activate
    
    if [ -f "data/raw/wait_times_data.xlsx" ]; then
        python scripts/run_etl.py
        success "Initial data loaded"
    else
        warning "No data file found - skipping data load"
    fi
}

# Setup systemd service
setup_systemd_service() {
    log "Setting up systemd service..."
    
    # Create service file
    sudo tee /etc/systemd/system/healthcare-analytics.service > /dev/null <<EOF
[Unit]
Description=Healthcare Analytics Dashboard
After=network.target postgresql.service

[Service]
Type=simple
User=$USER
WorkingDirectory=$DEPLOY_DIR
Environment=PATH=$DEPLOY_DIR/venv/bin
ExecStart=$DEPLOY_DIR/venv/bin/python dashboard/app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd and enable service
    sudo systemctl daemon-reload
    sudo systemctl enable healthcare-analytics
    
    success "Systemd service configured"
}

# Setup nginx reverse proxy
setup_nginx() {
    log "Setting up nginx reverse proxy..."
    
    # Create nginx config
    sudo tee /etc/nginx/sites-available/healthcare-analytics > /dev/null <<EOF
server {
    listen 80;
    server_name _;
    
    location / {
        proxy_pass http://127.0.0.1:8050;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
    
    location /static/ {
        alias $DEPLOY_DIR/dashboard/assets/;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }
}
EOF

    # Enable site
    sudo ln -sf /etc/nginx/sites-available/healthcare-analytics /etc/nginx/sites-enabled/
    sudo nginx -t && sudo systemctl reload nginx
    
    success "Nginx configured"
}

# Start services
start_services() {
    log "Starting services..."
    
    # Start application
    sudo systemctl start healthcare-analytics
    sudo systemctl status healthcare-analytics --no-pager
    
    # Start nginx
    sudo systemctl start nginx
    
    success "Services started"
}

# Health check
health_check() {
    log "Performing health check..."
    
    # Wait for service to start
    sleep 10
    
    # Check if service is running
    if systemctl is-active --quiet healthcare-analytics; then
        success "Application service is running"
    else
        error "Application service failed to start"
    fi
    
    # Check HTTP response
    if curl -s http://localhost:8050 > /dev/null; then
        success "Application is responding to HTTP requests"
    else
        error "Application is not responding"
    fi
}

# Main deployment function
main() {
    log "Starting deployment of Healthcare Analytics Dashboard"
    
    check_prerequisites
    backup_current_deployment  
    deploy_application
    setup_python_environment
    setup_database
    load_initial_data
    setup_systemd_service
    setup_nginx
    start_services
    health_check
    
    success "Deployment completed successfully!"
    log "Application is available at: http://localhost"
    log "Logs can be monitored with: journalctl -u healthcare-analytics -f"
}

# Run main function
main "$@"