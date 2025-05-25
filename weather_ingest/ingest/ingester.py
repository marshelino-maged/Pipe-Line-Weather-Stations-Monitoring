import time
import shutil
import pandas as pd
from pathlib import Path
from elasticsearch import Elasticsearch, helpers

INCOMING_DATA_DIR = Path("/data/incoming")
PROCESSED_DATA_DIR = Path("/data/processed")
ES_INDEX = "weather_data"

def ingest_parquet(file_path, es, batch_size=500):
    print(f"Ingesting file: {file_path.name}")

    df = pd.read_parquet(file_path, engine='pyarrow')
    records = df.to_dict(orient="records")

    actions = (
        {
            "_index": ES_INDEX,
            "_source": record,
        }
        for record in records
    )
    helpers.bulk(es, actions, chunk_size=batch_size)
    print(f"Ingested {file_path.name}")

    target_path = PROCESSED_DATA_DIR / file_path.name
    shutil.move(str(file_path), str(target_path))
    print(f"Moved {file_path.name} to processed folder.")
    print()

def main():
    seen = set()
    es = Elasticsearch("http://elasticsearch-kibana:9200")

    print("Scanning for new files...")
    print()

    while True:
        for file in INCOMING_DATA_DIR.glob("*.parquet"):
            if file.name not in seen:
                ingest_parquet(file, es)
                seen.add(file.name)
        time.sleep(5)

if __name__ == "__main__":
    main()
