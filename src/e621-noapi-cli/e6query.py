import logging
import time
from typing import List, Set, Optional, Callable
from enum import Enum, unique
import pandas as pd
from .e6db import DBExport
from .filesize import (
    bytes_to_gigabytes
)
import re

@unique
class Category(Enum):
    GENERAL: int = 0
    ARTIST: int = 1
    COPYRIGHT: int = 3
    CHARACTER: int = 4
    SPECIES: int = 5
    INVALID: int = 6
    META: int = 7
    LORE: int = 8

    def __int__(self, in_str: str):
        if in_str.lower() == 'general':
            return Category.GENERAL
        elif in_str.lower() == 'artist':
            return Category.ARTIST
        elif in_str.lower() == 'copyright':
            return Category.COPYRIGHT
        elif in_str.lower() == 'species':
            return Category.SPECIES
        elif in_str.lower() == 'invalid':
            return Category.INVALID
        elif in_str.lower() == 'meta':
            return Category.META
        elif in_str.lower() == 'lore':
            return Category.LORE
        else:
            raise AssertionError(f"Unmatched enum value: {in_str}")

    def __str__(self) -> str:
        if self is Category.GENERAL:
            return "general"
        elif self is Category.ARTIST:
            return "artist"
        elif self is Category.COPYRIGHT:
            return "copyright"
        elif self is Category.SPECIES:
            return "species"
        elif self is Category.INVALID:
            return "invalid"
        elif self is Category.META:
            return "meta"
        elif self is Category.LORE:
            return "lore"
        else:
            raise AssertionError(f"Unmatched enum value: {self.value}")


class PostsSearchQueryManager:
    def __init__(self, posts_db_export: DBExport):
        self.query = ""
        self.posts_db_export = posts_db_export

    def build_query_function(
        self,
        and_tags: Optional[Set[str]] = None,
        not_tags: Optional[Set[str]] = None,
        or_tags: Optional[Set[str]] = None,
    ) -> Callable[[Set[str]], bool]:
        def query_function(tags: Set[str]) -> bool:
            and_success = not and_tags or all([and_tag in tags for and_tag in and_tags])
            if not and_success:
                return False
            not_success = not not_tags or not any(
                [not_tag in tags for not_tag in not_tags]
            )
            if not not_success:
                return False
            or_success = not or_tags or any([or_tag in tags for or_tag in or_tags])
            if not or_success:
                return False
            return and_success and not_success and or_success

        return query_function
    
    def build_search_query(
            self,
            query_string: str):
        not_tags: Set[str] = set()
        or_tags: Set[str] = set()
        and_tags: Set[str] = set()
        query_str: List[str] = List()
        order_str: List[str] = List()
        for token in query_string.split(' '):
            shebang, spec, colon, oper, arg = re.findall(r'^([~-+]*)([a-zA-Z0-9]*)([:]*)([>=!]*)([a-zA-Z0-9]*)$', token)
            if colon is None:
                if shebang == "+":
                    and_tags.add(spec)
                elif shebang == "-":
                    not_tags.add(spec)
                elif shebang == "~":
                    or_tags.add(spec)
                else:
                    and_tags.add(spec)
            else:
                if spec in ['score', 'down_score', 'image_width', 'image_height']:
                    query_str.append(f'{spec} {oper} {arg}')
                elif spec == 'order':
                    order_str.append(f'{arg}')




    def get_url(row) -> str:
        md5: str = row["md5"]
        return f"https://static1.e621.net/data/{md5[:2]}/{md5[2:4]}/{md5}.{row['file_ext']}"

    def filter(
        self,
        and_tags: Optional[Set[str]] = None,
        not_tags: Optional[Set[str]] = None,
        or_tags: Optional[Set[str]] = None,
    ) -> pd.DataFrame:
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
        return self.posts_db_export.e6_df[
            self.posts_db_export.e6_df["tags"].map(
                self.build_query_function(and_tags, not_tags, or_tags)
            )
        ]

    def load(self):
        """
        Loads in posts to a compatible, unform format and applies perliminary filters
        """
        # The e6 database stores the tags as one giant string, but we want to manipulate it as a list of unique tags.
        # So we apply the string splitting and set conversion during CSV load, then rename the column to "tags".
        time_before = time.time()
        len_before = len(self.posts_db_export.e6_df)
        size_before = bytes_to_gigabytes(self.posts_db_export.e6_df["file_size"].sum(), 2)
        self.posts_db_export.e6_df = pd.read_csv(
            self.posts_db_export.local_path(),
            sep=",",
            quotechar='"',
            converters={"tag_string": lambda x: set(x.split(" "))},
            compression="gzip",
        )
        self.posts_db_export.e6_df.rename(columns={"tag_string": "tags"}, inplace=True)
        logging.info(
            f"Loading post database with {len(self.posts_db_export.e6_df)} posts took {time.time() - time_before:.1f}s"
        )
        # See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
        # for an explanation of how to use .query(). In short, use & to AND things together, and
        # @variable_name to reference variables in the outer scope.
        self.posts_db_export.e6_df.query(
            "file_ext in ['jpg', 'jpeg', 'png', 'jiff'] & "
            "image_width >= 512 & "
            "image_height >= 512 & "
            "is_deleted == 'f' & "
            "is_pending == 'f' & "
            "is_flagged == 'f'",
            inplace=True,
        )
        self.posts_db_export.e6_df = self.filter(None, {"young", "cub", "loli", "shota", "gore", "scat"})
        logging.info(
            f"Database cleaning took {time.time() - time_before:.1f}s and eliminated "
            f"{len_before - len(self.posts_db_export.e6_df)} posts, or {size_before - bytes_to_gigabytes(self.posts_db_export.e6_df['file_size'].sum()):.1f} "
            "GB worth of images"
        )
