# GCS Synchronizer

This project synchronizes files from a local directory to a Google Cloud Storage (GCS) bucket. It includes validation against a ClickHouse database to ensure data integrity.

## Features

- **File Watching**: Monitors a specified directory for new, updated, or deleted files.
- **Data Validation**: Validates files against records in a ClickHouse database before processing.
- **GCS Integration**: Uploads validated files to a GCS bucket.
- **Caching**: Uses a local cache to manage synchronization state and prevent duplicate processing.
- **Scheduling**: Can be run manually or on a daily schedule.

## Project Structure

```
gcs-synchronizer/
├── .cache/                # Directory for cache files
│   ├── processed.json     # Log of processed files
│   └── update-soon.json   # Log of files pending update
├── config.py              # Configuration settings
├── requirements.txt       # Python dependencies
├── scheduler.py           # Scheduler for automated execution
├── synchronizer.py        # Core synchronization logic
└── README.md              # This file
```

## Setup

1.  **Clone the repository** (or set up the project files as provided).

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure the application**:
    - Edit `config.py` to set your GCS, ClickHouse, and folder path details.

## Usage

### Manual Synchronization

To run the synchronizer manually:

```bash
python synchronizer.py
```

### Scheduled Synchronization

To run the synchronizer on the schedule defined in `scheduler.py`:

```bash
python scheduler.py
```
