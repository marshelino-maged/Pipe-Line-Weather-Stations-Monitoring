import argparse
import csv
import threading
import time
import os
import json
import bitcask

bitcask =  bitcask.BitCaskEngineWithCompaction()

def view_all():
    timestamp = int(time.time())
    filename = f"{timestamp}.csv"
    data = bitcask.view_all()
    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["key", "value"])
        for k, v in data.items():
            writer.writerow([k, v])
    print(f"Exported all data to {filename}")

def view_key(key):
    value = bitcask.get(key)
    print(value if value else f"Key '{key}' not found")

def perf_test(clients):
    timestamp = int(time.time())
    data = bitcask.view_all()
    keys = list(data.keys())

    def worker(thread_id):
        filename = f"{timestamp}_thread_{thread_id}.csv"
        with open(filename, "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["key", "value"])
            for key in keys:
                writer.writerow([key, bitcask.get(key)])
        print(f"Thread {thread_id} done")

    threads = []
    for i in range(clients):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

def load_mock_data():
    current_dir = os.path.dirname(__file__)
    filepath = os.path.abspath(os.path.join(current_dir, "..", "bitcask/weather_mock_data.json"))

    if not os.path.exists(filepath):
        print(f"[!] Warning: mock file '{filepath}' not found.")
        return

    with open(filepath, "r") as f:
        records = json.load(f)

    for record in records:
        key = f"station_{record['station_id']}"
        value = json.dumps(record)
        bitcask.put(key, value)

    print(f"[✓] Loaded {len(records)} records into BitCask.")


if __name__ == "__main__":
    load_mock_data()

    parser = argparse.ArgumentParser()
    parser.add_argument("--view-all", action="store_true")
    parser.add_argument("--view", type=str, help="View key")
    parser.add_argument("--perf", type=int, help="Number of client threads")

    args = parser.parse_args()

    if args.view_all:
        view_all()
    elif args.view:
        view_key(args.view)
    elif args.perf:
        perf_test(args.perf)

