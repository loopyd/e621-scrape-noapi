from dataclasses import dataclass
import os
import pathlib
from typing import List
from .e6db import download_e6_csvs, filter_posts_by_tags, get_file_url
from .filesize import bytes_to_gigabytes, bytes_to_megabytes
import time
import sys
import pandas as pd
import logging
import argparse
from pydantic import dataclasses


_log = logging.getLogger("e621-noapi-cli")


def ExistingWritableDir(dir_name: str) -> str:
    """
    Argument type validation for existing, writable directories.

    Args:
        - dir_name (str): The name of the directory (piped through argparse)
    """
    dirname = str(pathlib.Path(dir_name).resolve()).replace("\\", "/").rstrip("/") + "/"
    if (
        os.path.exists(dirname)
        and os.path.isdir(dirname)
        and os.access(dirname, os.W_OK)
    ):
        return dirname
    else:
        raise argparse.ArgumentTypeError(
            f"{dir_name} is not an existing writable directory."
        )


@dataclass
class AppConfig:
    """
    AppConfig is the configuration class for the application.  It is created by pydantic and so is serializable to/from config.json
    format.

    Args:
        - csv_dir (str): Path to the directory containing csv files.
        - samples_dir (str): Base path to the directory containing sample image folders/files
        - search_query (str): List of strings representing e621 search queries
        - build_tokendb (bool): Whether or not to build the token database (.txt) file for stable diffusion training, and place it in the results folder.
        - refresh_csv (bool): Whether or not to refresh the CSV tag database from e621 regardless of whether or not its already been downloaded.
        - num_samples (int): Number of image samples to download for each search query
        - store_metadata (bool): Store tag metadata information next to each file (.json format)
    """

    csv_dir: str
    samples_dir: str
    search_query: List
    build_tokendb: bool
    refresh_csv: bool
    num_samples: int
    store_metadata: bool


def parse_args(args: str) -> AppConfig:
    parser = argparse.ArgumentParser(
        prog="e621-noapi-cli",
        description="Yiffy Diffusion NoAPI scraper for e621.net by the furry diffusion community.",
        add_help=True,
    )
    parser.add_argument(
        "csv-dir",
        type=str,
        help="The directory to store downloaded e621 CSV data (for our offline API).",
        dest="csv_dir",
    )
    parser.add_argument(
        "refresh-csv",
        type=bool,
        help="Clean and redownload the e621 csv database.",
        dest="refresh_csv",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "search",
        type=List,
        help="Specify your search query, you can use exact e6 search query format without tag limits thanks to our custom parser",
        dest="search_query",
    )
    parser.add_argument(
        "samples",
        type=int,
        help="Number of images (samples) to download",
        dest="num_samples",
    )
    parser.add_argument(
        "sample-dir",
        type=str,
        help="The base directory to store downloaded sample data to",
        dest="sample_dir",
    )
    parser.add_argument(
        "store-metadata",
        type=bool,
        help="Dump metadata information next to each file",
        action="store_true",
        dest="store_metadata",
        default=False,
    )
    parser.add_argument(
        "build-tokendb",
        type=bool,
        help="Build token database file (.txt) for stable diffusion embedding training.",
        action="store_true",
        dest="build_tokendb",
        default=False,
    )

    args = parser.parse_args(args)
    config = AppConfig(
        csv_dir=args.csv_dir,
        samples_dir=args.samples_dir,
        search_query=List([item for item in args.search_query]),
        build_tokendb=args.build_tokendb,
        refresh_csv=args.refresh_csv,
        num_samples=int(args.num_samples),
        store_metadata=args.store_metadata,
    )
    return AppConfig


def main(config: AppConfig):
    download_e6_csvs(delete_gzipped_csvs=False)

    # The e6 database stores the tags as one giant string, but we want to manipulate it as a list of unique tags.
    # So we apply the string splitting and set conversion during CSV load, then rename the column to "tags".
    time_before = time.time()
    posts_df = pd.read_csv(
        f"{config.csv_dir}/posts.csv.gz",
        sep=",",
        quotechar='"',
        converters={"tag_string": lambda x: set(x.split(" "))},
        compression="gzip",
    )
    posts_df.rename(columns={"tag_string": "tags"}, inplace=True)
    logging.info(
        f"Loading post database with {len(posts_df)} posts took {time.time() - time_before:.1f}s"
    )

    # Examples of how to work with this data:

    # Using DataFrame.query() to perform basic SQL-style searches or filtering:
    allowed_file_exts = ["png", "jpg"]
    min_img_size: int = 512
    min_fav_count: int = 30
    min_score: int = 10
    min_down_score: int = -35

    time_before = time.time()
    len_before = len(posts_df)
    size_before = bytes_to_gigabytes(posts_df["file_size"].sum(), 2)

    # See: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    # for an explanation of how to use .query(). In short, use & to AND things together, and
    # @variable_name to reference variables in the outer scope.
    posts_df.query(
        "fav_count >= @min_fav_count & "
        "score >= @min_score & "
        "down_score >= @min_down_score & "
        "file_ext in @allowed_file_exts & "
        "image_width >= @min_img_size & "
        "image_height >= @min_img_size & "
        "is_deleted == 'f' & "
        "is_pending == 'f' & "
        "is_flagged == 'f'",
        inplace=True,
    )
    logging.info(
        f"First-pass filtering with DataFrame.query took {time.time() - time_before:.1f}s and eliminated "
        f"{len_before - len(posts_df)} posts, or {size_before - bytes_to_gigabytes(posts_df['file_size'].sum()):.1f} "
        "GB worth of images"
    )

    # Filtering out undesirable tags:
    time_before = time.time()
    len_before = len(posts_df)
    size_before = bytes_to_gigabytes(posts_df["file_size"].sum(), 2)
    # Equivalent of "-young -cub -gore" etc.
    posts_df = filter_posts_by_tags(
        posts_df, None, {"young", "cub", "loli", "shota", "gore", "scat"}
    )
    logging.info(
        f"Second-pass filtering took {time.time() - time_before:.1f}s and eliminated "
        f"{len_before - len(posts_df)} posts, or {size_before - bytes_to_gigabytes(posts_df['file_size'].sum()):.1f} "
        "GB worth of images"
    )

    # If you want to do the equivalent of "rating:e", that can be accomplished by:
    num_explicit_posts = len(
        posts_df.query("rating == 'e'")
    )  # Note the lack of inplace=True to avoid modifying it
    logging.info(f"The dataset has {num_explicit_posts} explicit posts in it")

    # If you want to do the equivalent of "order:favcount" or "order:score":
    posts_df.sort_values(
        "fav_count", axis=0, ascending=False, inplace=True
    )  # Optionally pass inplace=False...
    logging.info(
        f"The highest fav_count post in the dataset has {posts_df['fav_count'].values[0]} favorites"
    )

    # If you want to match the tags to their category ("artist", "species", etc.)
    # you need to load the 'tags.csv' file.
    #
    # See: https://e621.net/wiki_pages/11262 for descriptions of what the tag categories are.
    # Numerically, they are:
    # 0 general
    # 1 artist
    # 3 copyright
    # 4 character
    # 5 species
    # 6 invalid
    # 7 meta
    # 8 lore
    tags_df = pd.read_csv("tags.csv.gz", sep=",", quotechar='"', compression="gzip")
    species_tags = tags_df.query("category == 5 & post_count > 0")
    logging.info(
        f"There are {len(species_tags)} different species tags across the original unfiltered dataset"
    )

    post_species_tags = [
        t for t in posts_df["tags"].values[0] if t in species_tags["name"].values
    ]
    logging.info(f"Species tags for the highest fav_count post: {post_species_tags}")

    # And filtering using an approximation to the tags search syntax:
    rating_e_order_score = posts_df.query("rating == 'e'").sort_values(
        "score", axis=0, ascending=False
    )

    # Rough equivalent of "rating:e order:score mr._wolf_(the_bad_guys) wolf_o'donnell"
    wolf_and_mr_wolf = filter_posts_by_tags(
        rating_e_order_score, {"mr._wolf_(the_bad_guys)", "wolf_o'donnell"}
    )
    # "rating:e order:score ~mr._wolf_(the_bad_guys) ~wolf_o'donnell"
    wolf_or_mr_wolf = filter_posts_by_tags(
        rating_e_order_score, None, None, {"mr._wolf_(the_bad_guys)", "wolf_o'donnell"}
    )
    # "rating:e order:score ~mr._wolf_(the_bad_guys) ~wolf_o'donnell -fox_mccloud"
    wolf_or_mr_wolf_and_not_fox = filter_posts_by_tags(
        rating_e_order_score,
        None,
        {"fox_mccloud"},
        {"mr._wolf_(the_bad_guys)", "wolf_o'donnell"},
    )

    # Of course, there should be very few results with Wolf and Mr. Wolf, many results with either of them,
    # slightly fewer results when we block Fox McCloud.
    logging.info(
        f"'rating:e order:score mr._wolf_(the_bad_guys) wolf_o'donnell': {len(wolf_and_mr_wolf)} results"
    )
    logging.info(
        f"'rating:e order:score ~mr._wolf_(the_bad_guys) ~wolf_o'donnell': {len(wolf_or_mr_wolf)} results"
    )
    logging.info(
        f"'rating:e order:score ~mr._wolf_(the_bad_guys) ~wolf_o'donnell -fox_mccloud': "
        f"{len(wolf_or_mr_wolf_and_not_fox)} results"
    )

    # And getting the post file URLs:
    filtered_file_urls = wolf_and_mr_wolf.apply(get_file_url, axis=1)
    logging.info(filtered_file_urls.values[0])

    # If you're going to save file URLs to a .txt file, here's what I'd recommend:
    # (Puts each URL on its own line, good for programs that download in bulk
    # like gallery-dl: https://github.com/mikf/gallery-dl)
    # filtered_file_urls.to_csv("urls.txt", header=None, index=None, sep=' ')

    # If you want to save the modified post CSV to a file (a good idea!), then try this:
    wolf_and_mr_wolf.to_csv("wolf_and_mr_wolf.csv", sep=",", quotechar='"', index=False)
    # And when reading it again from the disk, you won't need to convert the "tag_strings" column.
    wolf_and_mr_wolf_again = pd.read_csv("wolf_and_mr_wolf.csv", sep=",", quotechar='"')


if __name__ == "__main__":
    _log.critical("This script cannot be run from the command line by itself.")
