
import requests
import json
import os
from datetime import datetime, timezone
from pathlib import Path

# See https://www.openbrewerydb.org/documentation
def fetch_brewery_data():
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    output_dir = Path("/opt/airflow/data/bronze")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"breweries_{timestamp}.json"
    with open(output_path, "w") as f:
        json.dump(data, f)
