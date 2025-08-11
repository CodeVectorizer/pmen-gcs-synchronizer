# PMEN Document Sync Service for Linux

This service monitors a directory for new or modified PDF documents, syncs them with a ClickHouse database, and uploads them to Google Cloud Storage.

## Prerequisites

- Linux system with systemd
- Python 3.7+
- Google Cloud Service Account with Storage Admin permissions
- ClickHouse server access

## Installation

1. Clone this repository to `/opt/pmen-sync`:
   ```bash
   sudo mkdir -p /opt
   sudo git clone <repository-url> /opt/pmen-sync
   cd /opt/pmen-sync
   ```

2. Create and configure the `.env` file:
   ```bash
   sudo cp .env.linux .env
   sudo nano .env  # Edit with your configuration
   ```

3. Run the installation script:
   ```bash
   chmod +x install_linux.sh
   sudo ./install_linux.sh
   ```

## Configuration

Edit `/etc/default/pmen-sync` to modify environment variables:

```ini
# ClickHouse Configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=default

# GCS Configuration
GCS_BUCKET_NAME=your-bucket-name
GCS_SERVICE_ACCOUNT_KEY=/path/to/service-account-key.json

# Watch Configuration
WATCHED_FOLDER=/var/www/html/pmen/public/documents

# Logging
LOG_LEVEL=INFO
LOG_TARGET=journal  # or 'file' for file logging
LOG_DIR=/var/log/pmen-sync

# Processing
BATCH_SIZE=50
PROCESSING_INTERVAL=300  # seconds
```

## Service Management

- Start the service:
  ```bash
  sudo systemctl start pmen-sync
  ```

- Stop the service:
  ```bash
  sudo systemctl stop pmen-sync
  ```

- Enable auto-start on boot:
  ```bash
  sudo systemctl enable pmen-sync
  ```

- View logs:
  ```bash
  sudo journalctl -u pmen-sync -f
  ```

## Manual Processing

To manually trigger document processing:

```bash
cd /opt/pmen-sync
source venv/bin/activate
python clickhouse_to_gcs.py
```

## Troubleshooting

1. **Permission Issues**:
   - Ensure the service user (www-data) has read access to the watched directory
   - Check that the service account key file is readable
   - Verify the log directory is writable

2. **Service Not Starting**:
   - Check status: `sudo systemctl status pmen-sync`
   - View logs: `sudo journalctl -u pmen-sync`

3. **File Not Syncing**:
   - Verify the file has a .pdf extension (case sensitive)
   - Check file permissions
   - Ensure the file is closed after writing

## Monitoring

The service logs to the systemd journal by default. To monitor in real-time:

```bash
sudo journalctl -u pmen-sync -f
```

## Updating

To update the service:

```bash
cd /opt/pmen-sync
sudo git pull
source venv/bin/activate
pip install -r requirements-linux.txt --upgrade
sudo systemctl restart pmen-sync
```
