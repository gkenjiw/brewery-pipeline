
import requests
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from airflow.exceptions import AirflowException

# See https://www.openbrewerydb.org/documentation
logger = logging.getLogger(__name__)

def fetch_brewery_data():
    url = "https://api.openbrewerydb.org/v1/breweries"

    try:
        logger.info("Requesting data from Brewery DB API...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            raise AirflowException("API returned no data.")

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        output_dir = Path("/opt/airflow/data/bronze")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"breweries_{timestamp}.json"

        with open(output_path, "w") as f:
            json.dump(data, f)
        
        logger.info(f"Saved raw data to {output_path}")
        
    except requests.exceptions.RequestException as e:
        logger.exception("API request failed")
        raise AirflowException(f"API error: {str(e)}")
    except Exception as e:
        logger.exception("Unknown error occurred")
        raise AirflowException(f"Unexpected error: {str(e)}")