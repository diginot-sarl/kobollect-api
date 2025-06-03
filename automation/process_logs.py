import requests
import json
import logging
from tqdm import tqdm
from app.database import get_db
from app.models import Logs
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

def process_logs(db: Session):
    """
    Process logs from the database and send requests based on the log type.
    """
    try:
        # Fetch all logs from the database
        logs = db.query(Logs).all()

        # Loop over each log
        for log in tqdm(logs, desc="Processing logs", unit="log"):
            if log.logs == "process_recensement_form":
                # Case 1: Send request to "/import-from-kobo" route
                response = requests.post(
                    "https://api.hidscollect.com:9443/api/v1/import-from-kobo",
                    json=json.loads(log.data_json)
                )
                if response.status_code != 200:
                    logger.error(f"Error processing log {log.id}: {response.text}")

            elif log.logs == "process_rapport_superviseur_form":
                # Case 2: Skip this log
                continue

            elif log.logs == "process_parcelles_non_baties_form":
                # Case 3: Send request to "/import-parcelle-non-batie" route
                response = requests.post(
                    "https://api.hidscollect.com:9443/api/v1/import-parcelle-non-batie",
                    json=json.loads(log.data_json)
                )
                if response.status_code != 200:
                    logger.error(f"Error processing log {log.id}: {response.text}")

            elif log.logs == "process_immeuble_form":
                # Case 4: Send request to "/import-immeuble" route
                response = requests.post(
                    "https://api.hidscollect.com:9443/api/v1/import-immeuble",
                    json=json.loads(log.data_json)
                )
                if response.status_code != 200:
                    logger.error(f"Error processing log {log.id}: {response.text}")

        logger.info("Logs processed successfully")

    except Exception as e:
        logger.error(f"Error processing logs: {str(e)}")

def main():
    """
    Main function to execute the script.
    """
    # Initialize database session
    db = next(get_db())
    
    # Process logs
    process_logs(db)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main()) 