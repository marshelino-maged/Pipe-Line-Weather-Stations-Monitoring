import os
import struct
import time
from threading import Lock
from typing import Dict, Tuple

class BitCaskEngineWithHint:
    def __init__(self, directory='bitcask_store_with_hint', segment_size_limit=1024 * 1024):
        self.directory = directory
        os.makedirs(self.directory, exist_ok=True)
        self.segment_size_limit = segment_size_limit
        self.segment_index = 0
        self.active_segment_file = None
        self.active_hint_file = None
        self.active_segment_path = None
        self.active_hint_path = None
        self.index: Dict[str, Tuple[str, int, int]] = {}
        self.lock = Lock()
        self._initialize_segments_and_index()

    def _initialize_segments_and_index(self):
        segment_files = [f for f in os.listdir(self.directory) if f.endswith('.data')]
        for segment_file in sorted(segment_files):
            hint_file = segment_file.replace('.data', '.hint')
            hint_path = os.path.join(self.directory, hint_file)
            if os.path.exists(hint_path):
                with open(hint_path, 'rb') as hf:
                    while True:
                        header = hf.read(16)
                        if not header:
                            break
                        key_len, offset, val_len, timestamp = struct.unpack('>IIII', header)
                        key = hf.read(key_len).decode()
                        segment_path = os.path.join(self.directory, segment_file)
                        self.index[key] = (segment_path, offset, val_len)

        self.segment_index = max(
            [int(f.split('_')[1].split('.')[0]) for f in segment_files] + [-1]
        ) + 1
        self._open_new_segment()

    def _open_new_segment(self):
        if self.active_segment_file:
            self.active_segment_file.close()
        if self.active_hint_file:
            self.active_hint_file.close()

        self.active_segment_path = os.path.join(self.directory, f"segment_{self.segment_index}.data")
        self.active_hint_path = os.path.join(self.directory, f"segment_{self.segment_index}.hint")
        self.active_segment_file = open(self.active_segment_path, 'ab+')
        self.active_hint_file = open(self.active_hint_path, 'ab+')
        self.segment_index += 1

    def _check_segment_rotation(self):
        if self.active_segment_file.tell() >= self.segment_size_limit:
            self._open_new_segment()

    def put(self, key: str, value: str):
        with self.lock:
            key_bytes = key.encode()
            value_bytes = value.encode()
            timestamp = int(time.time())
            header = struct.pack('>IIQ', len(key_bytes), len(value_bytes), timestamp)
            record = header + key_bytes + value_bytes
            offset = self.active_segment_file.tell()
            self.active_segment_file.write(record)
            self.active_segment_file.flush()

            hint_header = struct.pack('>IIII', len(key_bytes), offset, len(value_bytes), timestamp)
            self.active_hint_file.write(hint_header + key_bytes)
            self.active_hint_file.flush()

            self.index[key] = (self.active_segment_path, offset, len(value_bytes))
            self._check_segment_rotation()

    def get(self, key: str):
        with self.lock:
            if key not in self.index:
                return None
            path, offset, value_size = self.index[key]
            with open(path, 'rb') as f:
                f.seek(offset)
                header = f.read(16)
                key_len, val_len, timestamp = struct.unpack('>IIQ', header)
                read_key = f.read(key_len).decode()
                value = f.read(val_len).decode()
                return value if read_key == key else None

    def view_all(self):
        return {key: self.get(key) for key in self.index.keys()}

class BitCaskEngineWithCompaction(BitCaskEngineWithHint):
    def compact(self):
        with self.lock:
            latest_records = {}
            for key, (path, offset, val_size) in self.index.items():
                with open(path, 'rb') as f:
                    f.seek(offset)
                    header = f.read(16)
                    key_len, val_len, timestamp = struct.unpack('>IIQ', header)
                    k = f.read(key_len).decode()
                    v = f.read(val_len).decode()
                    latest_records[k] = (timestamp, v)

            # Open new segment and hint
            compact_path = os.path.join(self.directory, f"segment_{self.segment_index}_compacted.data")
            hint_path = os.path.join(self.directory, f"segment_{self.segment_index}_compacted.hint")
            segment_file = open(compact_path, 'wb')
            hint_file = open(hint_path, 'wb')

            new_index = {}
            for key, (timestamp, value) in latest_records.items():
                key_bytes = key.encode()
                value_bytes = value.encode()
                header = struct.pack('>IIQ', len(key_bytes), len(value_bytes), timestamp)
                offset = segment_file.tell()
                segment_file.write(header + key_bytes + value_bytes)

                hint_header = struct.pack('>IIII', len(key_bytes), offset, len(value_bytes), timestamp)
                hint_file.write(hint_header + key_bytes)

                new_index[key] = (compact_path, offset, len(value_bytes))

            segment_file.close()
            hint_file.close()

            # ⚠️ Ensure files are closed before deleting
            if self.active_segment_file:
                self.active_segment_file.close()
            if self.active_hint_file:
                self.active_hint_file.close()

            # Remove old files
            for file in os.listdir(self.directory):
                full_path = os.path.join(self.directory, file)
                if file.startswith("segment_") and not file.startswith(f"segment_{self.segment_index}_compacted"):
                    try:
                        os.remove(full_path)
                    except Exception as e:
                        print(f"Could not delete {file}: {e}")

            self.segment_index += 1
            self.index = new_index
            self._open_new_segment()