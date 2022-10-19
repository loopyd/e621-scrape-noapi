import urllib.response
from typing import Final, Set, Optional, Callable
import pandas as pd


class SearchQuery:
    def __init__(self, query: str, e6_df: pd.DataFrame):
        self.query = query
        self.e6_df = e6_df

    def build_query_function(
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

    def get_file_url(row) -> str:
        md5: str = row["md5"]
        return f"https://static1.e621.net/data/{md5[:2]}/{md5[2:4]}/{md5}.{row['file_ext']}"

    def filter_posts_by_tags(
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
        return self.e6_df[
            self.e6_df["tags"].map(
                self.build_query_function(and_tags, not_tags, or_tags)
            )
        ]
