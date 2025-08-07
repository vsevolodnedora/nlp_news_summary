import copy
import os
from calendar import month_name
from collections import defaultdict
from datetime import datetime
# from .preprocess_utils import Preprocessor
import re

from typing import List, Dict, Optional, Callable

from langid import langid

from database import PostsDatabase

from logger import get_logger
logger = get_logger(__name__)


def process_one_article_text(  # noqa: C901
        text:str, date:str, title:str, start_markers: List[str], end_markers: List[str],
        start_marker_constructs:Dict|None,
        skip_start_lines:int|None, max_lines:int|None,
        custom_black_list_starters:List,
        black_list_single_word_lines:List,
        black_list_blocks:List|None,
) -> str:
    """Process one article text and return a snippet."""
    # Find start point from which to cut the article
    start_idx = None
    if start_marker_constructs is not None:
        for start_marker_name, start_marker_func in start_marker_constructs.items():
            if start_marker_name == "date":
                # start marker is a date. Function expects date as a string in YYYY-MM-DD format
                start_markers.append(start_marker_func(date))
            else:
                raise NotImplementedError(f"Start marker {start_marker_name} not implemented.")

    for start_marker in start_markers:
        start_idx_ = text.find(start_marker)
        if start_idx_ == -1 or not start_idx_:
            continue
        else:
            start_idx = start_idx_ + len(start_marker)
            break

    if len(start_markers)>0 and len(end_markers) > 0:
        if not start_idx or start_idx == -1 or start_idx == len(text)-1:
            raise ValueError(f"Start marker not found in {title}, skipping.")


    # find end point up to which to cut the article
    end_idx = None
    for end_marker in end_markers:
        end_idx_ = text.find(end_marker)
        if end_idx_ == -1 or not end_idx_:
            continue
        else:
            end_idx = end_idx_
            break
    if len(start_markers)>0 and len(end_markers) > 0:
        if not end_idx or end_idx == -1 or end_idx == len(text)-1:
            raise ValueError(f"End marker not found in {title}, skipping.")

        # sanity check
        if start_idx > end_idx:
            raise ValueError(f"Start marker {start_idx} is before end marker {end_idx}.")

        # Extract and clean up the snippet
        snippet = text[start_idx:end_idx].strip()
        if len(snippet) == 0:
            raise ValueError("Snippet is empty, skipping.")
    else:
        snippet = copy.deepcopy(text)
        logger.info(f"No start or end marker provided for {title}, Processing the entire text.")

    # check fot file that might be too big
    num_lines = snippet.count("\n") + 1  # Count lines in the markdown
    if max_lines is not None and num_lines > max_lines:
        logger.warn(f"Post '{title}' has {num_lines} lines, which exceeds the max_lines limit of {max_lines}.")

    # remove lines from the file that start with an unwanted element
    if custom_black_list_starters is not None:
        current_lines = snippet.split("\n")
        selected_lines = []
        for line in current_lines:
            if any([line.startswith(element) for element in custom_black_list_starters]):
                continue
            selected_lines.append(line)
        snippet = "\n".join(selected_lines)

    # remove lines from the file that are an unwanted element (1to1)
    if black_list_single_word_lines is not None:
        selected_lines = []
        for line in snippet.split("\n"):
            found = False
            if any(line == element for element in black_list_single_word_lines):
                found = True
            if found:
                continue
            selected_lines.append(line)
        snippet = "\n".join(selected_lines)


    if skip_start_lines > 0:
        lines = snippet.split("\n")
        selected_lines = lines[skip_start_lines:]
        snippet = "\n".join(selected_lines)

    # remove the whole block of text if needed
    if black_list_blocks is not None:
        blocks = re.split(r"(\n\s*\n)", snippet)  # split while preserving newlines
        cleaned_blocks = []

        for block in blocks:
            for black_list_block_component in black_list_blocks:
                if black_list_block_component in block:
                    logger.info(f"removing {black_list_block_component} from block")
                    continue

            cleaned_blocks.append(block)

        snippet = "".join(cleaned_blocks)

    if len(snippet.split('\n')) <= 1:
        logger.warning(
            f"Only one line in snippet, nothing to write after skipping first line. Date:{date} title:{title}\n{snippet}"
        )

    return snippet

def filter_german_posts(posts: List[Dict]) -> List[Dict]:
    """
    From a list of post-dicts, find dates with multiple posts, detect
    German articles among them, log the results, and return only those
    German articles.

    :param posts: List of dicts with keys including "published_on" and "post"
    :return: List of dicts that are in German and share their published_on date
             with at least one other post
    """
    # Group posts by published_on
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for article in posts:
        date = article["published_on"]
        groups[date].append(article)

    german_posts: List[Dict] = []

    # Inspect only dates where there are multiple posts
    for date, articles in groups.items():
        if len(articles) <= 1:
            continue

        found_german = False
        for article in articles:
            lang, _ = langid.classify(article["post"])
            if lang == "de":
                found_german = True
                german_posts.append(article)

        if found_german:
            logger.debug(f"German article found with date={date}")
        else:
            logger.debug(f"No German article found with date={date}")

    return german_posts

def preprocess_posts_for_a_table(
    source_db: PostsDatabase,
    target_db: PostsDatabase,
    table_name: str,
    start_markers: List[str],
    end_markers: List[str],
    start_marker_constructs: Dict[str, Callable] | None = None,
    skip_start_lines: int = 0,
    max_lines: Optional[int] = None,
    custom_black_list_starters: Optional[List[str]] = None,
    black_list_single_word_lines: Optional[List[str]] = None,
    black_list_blocks: Optional[List[str]] = None,
    prefer_german: bool = False,
) -> None:
    """
    For each article in `table_name`, fetch `raw_post` from the DB, run it through
    `process_one_article_text()`, and store the result (compressed) into `clean_post`
    via the `add_clean_post()` method.
    """
    # 1) Get all article metadata (ID, date, title, url)
    articles = source_db.list_posts(table_name=table_name,sort_date=True)
    logger.info(f"Found {len(articles)} articles in table '{table_name}'.")

    # check if the table exists in the target database
    target_db.check_create_table(table_name=table_name)

    if prefer_german:
        articles = filter_german_posts(posts=articles)

    # 2) Iterate and process
    for meta in articles:
        published_on= meta["published_on"]
        title       = meta["title"]
        url         = meta["url"]
        post        = meta["post"]

        if not post:
            logger.warning(f"No post for url={url}; skipping.")
            continue


        # 4) Clean/process the text
        cleaned = process_one_article_text(
            text=post,
            date=published_on,
            title=title,
            start_markers=start_markers,
            start_marker_constructs=start_marker_constructs,
            end_markers=end_markers,
            custom_black_list_starters=custom_black_list_starters,
            black_list_single_word_lines=black_list_single_word_lines,
            skip_start_lines=skip_start_lines,
            black_list_blocks=black_list_blocks,
            max_lines=max_lines,
        )

        # 5) Store compressed cleaned text back into target DB
        target_db.add_post(
            table_name=table_name,
            published_on=published_on,
            title=title,
            post_url=url,
            post=cleaned,
            overwrite=True, # replace previous preprocessed post
        )

    logger.info(f"Completed cleaning for table '{table_name}'.")


class Preprocessor:
    """Process raw posts and remove markdown bloat: links, HTML leftovers."""
    def __init__(self, config: dict) -> None:
        """Initialize the Preprocessor."""
        self.config = config

    @staticmethod
    def date_to_dd_mm_yyyy(datetime_str: datetime.date) -> str:
        """Convert datetime to DD-MM-YYYY format."""
        # Split the string by the dash
        date_part = datetime_str.split("T")[0]  # '2025-07-15'
        year, month, day = date_part.split("-")  # ['2025', '07', '15']
        # Rearrange and return in MM-DD-YYYY format
        return f"{int(day)}.{int(month)}.{year}"

    @staticmethod
    def date_to_yyyy_mm_dd(datetime_str: datetime.date) -> str:
        """Convert datetime to DD-MM-YYYY format."""
        date_part = datetime_str.split("T")[0]  #  e.g., '2025-07-15'
        # Split the string by the dash
        year, month, day = date_part.split("-") # e.g., ['2025', '07', '15']
        # Rearrange and return in MM-DD-YYYY format
        date_part = f"{month}-{day}-{year}"
        formatted_date = date_part.replace("-", "/")  # 'YYYY/MM/DD'
        return formatted_date

    def __call__(self, source_db_path: str, target_db_path: str, table_name:str, out_dir:str) -> None:
        """Process agora raw posts."""
        if not os.path.isfile(source_db_path):
            raise FileNotFoundError(f"source_db not found: {source_db_path}")

        source_db = PostsDatabase(source_db_path)

        if not os.path.isfile(target_db_path):
            logger.info(f"Target database is not found: {target_db_path}. Creating target database with table {table_name}")
            target_db = PostsDatabase(target_db_path)
            target_db.check_create_table(table_name)
        else:
            target_db = PostsDatabase(target_db_path)

        # process posts raw posts and save clean versions into new folder
        try:
            preprocess_posts_for_a_table(
                source_db=source_db,
                target_db=target_db,
                table_name=table_name,
                start_markers=self.config["start_markers"],
                end_markers=self.config["end_markers"],
                start_marker_constructs=self.config.get("start_marker_constructs", None),
                custom_black_list_starters=self.config.get("custom_black_list_starters", None),
                black_list_single_word_lines=self.config.get("black_list_single_word_lines", None),
                black_list_blocks=self.config.get("black_list_blocks", None),
                skip_start_lines=self.config.get("skip_start_lines", 0),
                max_lines=self.config.get("max_lines", None),
                prefer_german=self.config.get("prefer_german", False),

            )
        except Exception as e:
            logger.error(f"Failed preprocessing for {table_name} with exception raised: {e}")
            source_db.close()
            target_db.close()
            raise e

        # save scraped posts as raw .md files for analysis
        target_db.dump_posts_as_markdown(table_name=table_name, out_dir=out_dir)

        source_db.close()
        target_db.close()
        logger.info(f"Finished preprocessing raw posts for {table_name}.")
