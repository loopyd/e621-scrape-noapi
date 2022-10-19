import datetime
import gzip
import logging
import os
import time
import urllib.request
import urllib.response
from enum import Enum, unique, auto
from typing import (
    Final, 
    List, 
    Set, 
    Optional, 
    Callable
)
import pandas as pd
from .filesize import (
    bytes_to_gigabytes,
    bytes_to_megabytes
)


_log = logging.getLogger('e621-noapi-cli')
# e621 only stores 4 exports at once (today and the previous 3 days)
MAX_DB_EXPORT_AGE_DAYS: Final[int] = 3
E6_HEADER_USER_AGENT: Final[str] = "e621-noapi-cli/0.0.1"

@unique
class DBExportTarget(Enum):
    POOLS = auto()
    POSTS = auto()
    TAG_ALIASES = auto()
    TAG_IMPLICATIONS = auto()
    TAGS = auto()
    WIKI_PAGES = auto()

    def __str__(self) -> str:
        if self is DBExportTarget.POOLS:
            return "POOLS"
        elif self is DBExportTarget.POSTS:
            return "POSTS"
        elif self is DBExportTarget.TAG_ALIASES:
            return "TAG_ALIASES"
        elif self is DBExportTarget.TAG_IMPLICATIONS:
            return "TAG_IMPLICATIONS"
        elif self is DBExportTarget.TAGS:
            return "TAGS"
        elif self is DBExportTarget.WIKI_PAGES:
            return "WIKI_PAGES"
        else:
            raise AssertionError(f"Unmatched enum value: {self.value}")


class DBExport():
    """
    This class is responsible for managing one database export downloaded from e621.

    Args:
        - export_target (DBExportTarget): The target database export type.
        - days_ago (int): The number of days ago the export was uploaded to the host server
        - base_path (str): The base path where the csv export data is held.
        - refresh (bool): Whether or not to redownload the export data
        managed by this class when sync is called if the export data is already downloaded.
    """
    def __init__(self, export_target: DBExportTarget = DBExportTarget.TAGS, days_ago: int = 0, base_path: str = './db', refresh: bool = False):
        self.export_target = export_target
        self.days_ago = days_ago
        self.base_path = base_path
        self.refresh = refresh


    def server_filename(self) -> str:
        """
        Calculates the filename of a this gzipped .csv database export on e621's servers.
        """
        if self.days_ago not in range(0, MAX_DB_EXPORT_AGE_DAYS + 1):
            raise ValueError(
                f"Argument `days_ago` must be in the range [0, {MAX_DB_EXPORT_AGE_DAYS}] inclusive (was {self.days_ago})")

        utcnow = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=self.days_ago)
        return f"{str(self.export_target).lower()}-{utcnow.year}-{utcnow.month}-{utcnow.day}.csv.gz"


    def server_url(self) -> str:
        """
        Calculates the remote URL of a this gzipped .csv database export on e621's servers.
        """
        export_filename = self.server_filename(self.export_target, self.days_ago)
        return f"https://e621.net/db_export/{export_filename}"


    def local_path(self) -> str:
        """
        Calculates the fully qualified path of this gzipped.csv database export on the local filesystem.
        """
        return self.base_path.rstrip('/') + '/' + self.export_target.__str__().replace('_','-').lower() + '.csv.gz'


    def size_bytes(self) -> int:
        """
        Fetches the size in bytes of this gzipped .csv database export on e621's servers.
        """
        size_check_request = urllib.request.Request(url=self.server_url(), method="HEAD")
        size_check_request.add_header("User-Agent", E6_HEADER_USER_AGENT)
        with urllib.request.urlopen(size_check_request) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Failed to check download size of '{resp.url}' (got HTTP code {resp.status})")
            if "Content-Length" not in resp.headers:
                raise RuntimeError(f"HTTP response object for '{resp.url}' did not have a 'Content-Length' header")
            return int(resp.headers["Content-Length"])


    def sync(self):
        """
        Downloads this gzipped .csv database export from e621's servers.
        """
        if self.refresh is True and os.path.exists(self.local_path()):
            os.remove(self.local_path())
            _log.info(f"Removed '{self.local_path()}' for refresh")
        if self.refresh is False and os.path.exists(self.local_path()):
            _log.info(f"CSV database '{self.local_path()}' exists, not going to redownload it.")
            return

        db_size_mb = bytes_to_megabytes(self.size_bytes(), 2)
        download_url = self.server_url()
        _log.info(f"Downloading '{download_url}' ({db_size_mb} MB) to '{self.local_path()}'")
        dl_request = urllib.request.Request(url=download_url, method="GET")
        dl_request.add_header("User-Agent", E6_HEADER_USER_AGENT)
        with urllib.request.urlopen(dl_request) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Failed to request download of '{resp.url}' (got HTTP code {resp.status})")
            with open(self.local_path(), 'wb') as output_file:
                output_file.write(resp.read())


class DBExportManager:
    def __init__(self, base_path: str = './db', refresh: bool = False, export_targets: List[DBExportTarget] = [DBExportTarget.POSTS, DBExportTarget.TAGS, DBExportTarget.TAG_IMPLICATIONS], days_ago: int = 0):
        self.db_exports = List()
        for export_target in export_targets:
            self.db_exports.append(DBExport(export_target=export_target, base_path=base_path, days_ago=days_ago, refresh=refresh))
    

    def sync(self, export_target: DBExportTarget = DBExportTarget.TAGS):
        """
        Syncronize a single e621 database export target.

        Args:
            export_target (DBExportTarget): The e621 database export target to sync
        """
        res = [ec for ec in self.db_exports if ec.export_target == export_target]
        if res is not None:
            res[0].sync()
        else:
            raise RuntimeError(f"e621 database export target {export_target.__str__()} not found")


    def sync_all(self):
        """
        Syncronize all e621 database export targets

        As of now, you probably don't need to do this, and just sync DBExportTarget.POSTS with sync(DBExportTarget.POSTS)
        """
        for db_export_class in self.db_exports:
            db_export_class.sync()