# scheduler.py

import schedule
import time
import logging
from synchronizer import process_pending_files

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- Scheduling Logic ---
def job():
    """The job to be scheduled."""
    logging.info("Scheduler triggered. Processing pending files...")
    process_pending_files()

# Schedule the job to run every day at midnight
schedule.every().day.at("00:00").do(job)

# You can also use other schedules, for example:
# schedule.every(10).minutes.do(job)
# schedule.every().hour.do(job)
# schedule.every().monday.do(job)

if __name__ == '__main__':
    logging.info("Scheduler started. Waiting for the scheduled time to run the job.")
    while True:
        schedule.run_pending()
        time.sleep(1)
