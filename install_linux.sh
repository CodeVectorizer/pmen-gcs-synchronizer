#!/bin/bash

# Exit on error
set -e

echo "Installing PMEN Document Sync Service..."

# Create installation directory
INSTALL_DIR="/opt/pmen-sync"
echo "Creating installation directory at $INSTALL_DIR..."
sudo mkdir -p $INSTALL_DIR

# Copy files
echo "Copying files..."
sudo cp clickhouse_to_gcs.py config.py requirements.txt $INSTALL_DIR/
sudo cp .env.linux $INSTALL_DIR/.env

# Install dependencies
echo "Installing Python dependencies..."
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv

# Create virtual environment
python3 -m venv $INSTALL_DIR/venv
source $INSTALL_DIR/venv/bin/activate
pip install -r $INSTALL_DIR/requirements.txt
deactivate

# Create log directory
LOG_DIR="/var/log/pmen-sync"
sudo mkdir -p $LOG_DIR
sudo chown -R www-data:www-data $LOG_DIR

# Create watched directory if it doesn't exist
WATCHED_DIR="/var/www/html/pmen/public/documents"
sudo mkdir -p $WATCHED_DIR
sudo chown -R www-data:www-data $WATCHED_DIR

# Install service file
echo "Installing systemd service..."
sudo cp pmen-sync.service /etc/systemd/system/
sudo systemctl daemon-reload

# Create environment file
ENV_FILE="/etc/default/pmen-sync"
sudo bash -c "cat > $ENV_FILE <<EOL
# Environment variables for PMEN Sync Service
PATH=$INSTALL_DIR/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
PYTHONPATH=$INSTALL_DIR
EOL"

# Set permissions
echo "Setting permissions..."
sudo chown -R www-data:www-data $INSTALL_DIR
sudo chmod 750 $INSTALL_DIR
sudo chmod 640 $INSTALL_DIR/.env

# Enable and start service
echo "Enabling and starting service..."
sudo systemctl enable pmen-sync.service
sudo systemctl start pmen-sync.service

echo "Installation complete!"
echo "Service status: sudo systemctl status pmen-sync"
echo "View logs: sudo journalctl -u pmen-sync -f"
