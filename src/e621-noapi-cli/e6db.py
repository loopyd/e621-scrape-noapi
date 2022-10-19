import datetime
import gzip
import logging
import os
import time
import urllib.request
import urllib.response
from enum import Enum, unique, auto
from typing import Final, List, Set, Optional, Callable
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


class SearchQuery:
    def __init__(self, query: str, e6_df: pd.DataFrame):
        self.query = query
        self.e6_df = e6_df
    
    def build_query_function(and_tags: Optional[Set[str]] = None, not_tags: Optional[Set[str]] = None,
                            or_tags: Optional[Set[str]] = None) -> Callable[[Set[str]], bool]:
        def query_function(tags: Set[str]) -> bool:
            and_success = not and_tags or all([and_tag in tags for and_tag in and_tags])
            if not and_success:
                return False
            not_success = not not_tags or not any([not_tag in tags for not_tag in not_tags])
            if not not_success:
                return False
            or_success = not or_tags or any([or_tag in tags for or_tag in or_tags])
            if not or_success:
                return False
            return and_success and not_success and or_success

        return query_function


    def get_file_url(row) -> str:
            md5: str = row["md5"]
            return f"https://static1.e621.net/data/{md5[:2]}/{md5[2:4]}/{md5}.{row['file_ext']}"

    def filter_posts_by_tags(self, and_tags: Optional[Set[str]] = None, not_tags: Optional[Set[str]] = None,
                            or_tags: Optional[Set[str]] = None) -> pd.DataFrame:
        """
        Queries the e621 post database dataframe using a tag-based filtering criteria.
        :param e6_df: The DataFrame containing the e621 posts. This _must_ contain a column called "tags"
        containing the tags as a List or Set.
        :param and_tags: The set of tags that must all be present in order for a post to pass through the filter.
        Equivalent to just "tag_name" in e621's search syntax.
        :param not_tags: The set of tags of which none can be present in order for a post to pass through the filter.
        Equivalent to "-tag_name" in e621's search syntax.
        :param or_tags: The set of tags that can optionally be present in order to pass through the filter.
        Equivalent to "~tag_name" in e621's search syntax.
        :return: A filtered DataFrame containing the posts that match the tag query.
        """
        return self.e6_df[self.e6_df["tags"].map(self.build_query_function(and_tags, not_tags, or_tags))]