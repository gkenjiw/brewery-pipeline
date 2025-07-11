import pytest
import pandas as pd
from pathlib import Path
from dags.common.transforms import transform_to_silver, aggregate_gold

sample_raw_data = [
    {"id": 1, "name": "Brewery A", "state": "California", "brewery_type": "micro"},
    {"id": 2, "name": "Brewery B", "state": "California", "brewery_type": "regional"},
    {"id": 3, "name": "Brewery C", "state": "Texas", "brewery_type": "brewpub"}
]

@pytest.fixture
def setup_bronze_data(tmp_path):
    import json
    file_path = tmp_path / "breweries_sample.json"
    with open(file_path, "w") as f:
        json.dump(sample_raw_data, f)
    return file_path

def test_transform_to_silver(monkeypatch, tmp_path, setup_bronze_data):
    monkeypatch.setattr("dags.common.transforms.Path", lambda p="": tmp_path)

    def mock_glob(pattern):
        return [setup_bronze_data]
    
    monkeypatch.setattr(tmp_path, "glob", lambda x: mock_glob(x))

    transform_to_silver()
    output_files = list(tmp_path.glob("*.parquet"))
    assert len(output_files) == 2  # Should create one file per state

def test_aggregate_gold(monkeypatch, tmp_path):
    df_ca = pd.DataFrame([
        {"state": "California", "brewery_type": "micro"},
        {"state": "California", "brewery_type": "regional"}
    ])
    df_tx = pd.DataFrame([
        {"state": "Texas", "brewery_type": "brewpub"}
    ])
    df_ca.to_parquet(tmp_path / "ca.parquet", index=False)
    df_tx.to_parquet(tmp_path / "tx.parquet", index=False)

    monkeypatch.setattr("dags.common.transforms.Path", lambda p="": tmp_path)

    aggregate_gold()
    output_file = tmp_path / "gold" / "breweries_aggregated.csv"
    assert output_file.exists()

    df_out = pd.read_csv(output_file)
    assert {"state", "brewery_type", "count"}.issubset(df_out.columns)
