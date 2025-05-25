import os
import time
import pandas as pd
from pathlib import Path
from elasticsearch import Elasticsearch, helpers

# Load constants from environment variables
PARQUET_FOLDER = Path(os.getenv("PARQUET_FOLDER", "/parquet"))
ES_INDEX = os.getenv("ES_INDEX", "weather_data")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch-kibana:9200")

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
    print(f"Ingested {file_path}\n")

def main():
    seen = set()
    es = Elasticsearch([ELASTICSEARCH_HOST])

    print("Scanning for new files in all subfolders...\n")

    while True:
        for file in PARQUET_FOLDER.rglob("*.parquet"):
            if file.name not in seen:
                ingest_parquet(file, es)
                seen.add(file.name)
        time.sleep(5)

if __name__ == "__main__":
    main()
