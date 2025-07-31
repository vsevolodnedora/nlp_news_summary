import os
from typing import List, Dict, Optional, Callable

from logger import get_logger
logger = get_logger(__name__)

from database import PostsDatabase

def process_one_article_text(
        text:str, date:str, title:str, start_markers: List[str], end_markers: List[str],
        start_marker_constructs:Dict|None,
        skip_start_lines:int|None, max_lines:int|None,
        custom_black_list_starters:List
) -> str:

    # find start point from which to cut the article
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
    if not end_idx or end_idx == -1 or end_idx == len(text)-1:
        raise ValueError(f"End marker not found in {title}, skipping.")

    # sanity check
    if start_idx > end_idx:
        raise ValueError(f"Start marker {start_idx} is before end marker {end_idx}.")

    # Extract and clean up the snippet
    snippet = text[start_idx:end_idx].strip()
    if len(snippet) == 0:
        raise ValueError(f"snippet is empty, skipping.")

    # check fot file that might be too big
    num_lines = snippet.count('\n') + 1  # Count lines in the markdown
    if max_lines is not None and num_lines > max_lines:
        logger.warn(f"Post '{title}' has {num_lines} lines, which exceeds the max_lines limit of {max_lines}.")

    # remove lines from the file
    if not custom_black_list_starters is None:
        current_lines = snippet.split('\n')
        selected_Lines = []
        for line in current_lines:
            if not any([line.startswith(element) for element in custom_black_list_starters]):
                selected_Lines.append(line)
        snippet = '\n'.join(selected_Lines)

    if skip_start_lines > 0:
        lines = snippet.split('\n')
        selected_lines = lines[skip_start_lines:]
        snippet = '\n'.join(selected_lines)

    if len(snippet.split('\n')) <= 1:
        logger.warning(
            f"Only one line in snippet, nothing to write after skipping first line. Date:{date} title:{title}\n{snippet}"
        )

    return snippet

def cut_article_text_from_raw_pages(
        raw_posts_dir:str, cleaned_posts_dir:str, start_markers:list[str],
        end_markers:list[str], start_marker_constructs:dict|None=None,
        skip_start_lines:int=0,max_lines:int|None=None,
        custom_black_list_starters:list[str]|None = None,
):
    """
    Loop through each post in the raw_post_dir and pre-process it and save result in cleaned_posts_dir
    """
    # Ensure the cleaned directory exists
    os.makedirs(cleaned_posts_dir, exist_ok=True)

    if len(start_markers) == 0 and len(start_marker_constructs) == 0:
        raise ValueError(f"Start markers are not given, skipping.")
    if len(end_markers) == 0:
        raise ValueError(f"End markers are not given, skipping.")

    if not os.path.isdir(raw_posts_dir):
        raise ValueError(f"raw_posts_dir {raw_posts_dir} does not exist.")

    # Iterate over files in the raw directory
    for i_file, filename in enumerate(sorted(os.listdir(raw_posts_dir))):
        raw_path = os.path.join(raw_posts_dir, filename)
        cleaned_path = os.path.join(cleaned_posts_dir, filename)

        # Process only markdown files starting with a date
        if not filename.endswith('.md') or not filename[:10].count('-') == 2:
            continue

        # Skip if already cleaned
        if os.path.exists(cleaned_path):
            continue

        # extract date from file name
        date:str = filename[:10] # YYYY-MM-DD (always)
        title:str = filename[10:].replace(".md",'')

        # Read raw content
        with open(raw_path, 'r', encoding='utf-8') as f:
            text = f.read()

        # process the article text
        text_snippet = process_one_article_text(
            text=text, date=date, title=title, start_markers=start_markers, start_marker_constructs=start_marker_constructs,
            end_markers=end_markers, custom_black_list_starters=custom_black_list_starters,
            skip_start_lines=skip_start_lines,
            max_lines=max_lines,
        )

        # Write the cleaned snippet
        with open(cleaned_path, 'w', encoding='utf-8') as f:
            f.write(text_snippet)
        logger.info(f"Cleaned and saved: {filename}")


def cut_article_text_in_db(
    db: PostsDatabase,
    table_name: str,
    start_markers: List[str],
    end_markers: List[str],
    start_marker_constructs: Dict[str, Callable] | None = None,
    skip_start_lines: int = 0,
    max_lines: Optional[int] = None,
    custom_black_list_starters: Optional[List[str]] = None,
) -> None:
    """
    For each article in `table_name`, fetch `raw_post` from the DB, run it through
    `process_one_article_text()`, and store the result (compressed) into `clean_post`
    via the `add_clean_post()` method.
    """
    # 1) Get all article metadata (ID, date, title, url)
    articles = db.list_articles(table_name)
    logger.info(f"Found {len(articles)} articles in table '{table_name}'.")

    # 2) Iterate and process
    for meta in articles:
        article_id = meta["ID"]
        date       = meta["date"]
        title      = meta["title"]
        url        = meta["url"]

        # 3) Fetch and decompress raw_post
        rec = db.get_article(table_name=table_name, article_id=article_id)
        raw_text = rec.get("raw_post")
        if not raw_text:
            logger.warning(f"No raw_post for ID={article_id}; skipping.")
            continue

        # 4) Clean/process the text
        cleaned = process_one_article_text(
            text=raw_text,
            date=date,
            title=title,
            start_markers=start_markers,
            start_marker_constructs=start_marker_constructs,
            end_markers=end_markers,
            custom_black_list_starters=custom_black_list_starters,
            skip_start_lines=skip_start_lines,
            max_lines=max_lines
        )

        # 5) Store compressed cleaned text back into DB
        db.add_clean_post(table_name, article_id, cleaned)

    logger.info(f"Completed cleaning for table '{table_name}'.")