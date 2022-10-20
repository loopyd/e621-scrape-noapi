import enum
from typing import Optional
import json
import csv
import logging


_log = logging.getLogger("e621-noapi-cli")


class DataStoreFormat(enum):
    """
    An enum that describes the format of the filesystem datastore.
    """
    JSON = 1
    CSV = 2


class DataStore:
    def __init__(
            self, datastore_struct: object, datastore_path: Optional(str) = 'config.json', datastore_format: Optional(DataStoreFormat) = DataStoreFormat.JSON):
        self.datastore_path = datastore_path
        self.datastore_format = datastore_format
        self.datastore_struct = datastore_struct

    def load(self):
        if self.datastore_format == DataStoreFormat.JSON:
            self.datastore_struct.parse_raw(json.load(open(self.datastore_path, 'rb')))
        elif self.datastore_format == DataStoreFormat.CSV:
            self.datastore_struct.parse_raw(
                json.dumps(dict([(key, value) for key, value in csv.DictReader(open(self.datastore_path, encoding='utf-8'))]), indent=4))

    def save(self):
        if self.datastore_format == DataStoreFormat.JSON:
            json.dump(self.datastore_struct.json(), open(self.datastore_path, 'wb'), indent=4)
        elif self.datastore_format == DataStoreFormat.CSV:
            with open(self.datastore_path, 'wb') as csv_file:
                csv.writer(csv_file).writerow(self.datastore_struct.json().keys())
                csv.writer(csv_file).writerows(self.datastore_struct.json().values())
