import os
import logging

from logger import get_logger
logger = get_logger(__name__)

def cut_article_text_from_raw_pages(RAW_DIR:str, CLEANED_DIR:str, start_markers:list[str], end_markers:list[str]):
    """
    Loop through markdown files in RAW_DIR, extract content between 'Button' and 'Share this article',
    and save to CLEANED_DIR with the same filename if not already present.
    """
    # Ensure the cleaned directory exists
    os.makedirs(CLEANED_DIR, exist_ok=True)

    if len(start_markers) == 0:
        raise ValueError(f"Start markers are not given, skipping.")
    if len(end_markers) == 0:
        raise ValueError(f"End markers are not given, skipping.")

    if not os.path.isdir(RAW_DIR):
        raise ValueError(f"RAW_DIR {RAW_DIR} does not exist.")

    # Iterate over files in the raw directory
    for filename in os.listdir(RAW_DIR):
        raw_path = os.path.join(RAW_DIR, filename)
        cleaned_path = os.path.join(CLEANED_DIR, filename)

        # Process only markdown files starting with a date
        if not filename.endswith('.md') or not filename[:10].count('-') == 2:
            continue

        # Skip if already cleaned
        if os.path.exists(cleaned_path):
            continue

        # Read raw content
        with open(raw_path, 'r', encoding='utf-8') as f:
            text = f.read()

        # find start point from which to cut the article
        start_idx = None
        for start_marker in start_markers:
            start_idx_ = text.find(start_marker)
            if start_idx_ == -1 or not start_idx_:
                continue
                # continue
            else:
                start_idx = start_idx_ + len(start_marker)
                break
        if not start_idx or start_idx == -1 or start_idx == len(text)-1:
            raise ValueError(f"Start marker not found in {filename}, skipping.")

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
            raise ValueError(f"End marker not found in {filename}, skipping.")

        # sanity check
        if start_idx > end_idx:
            raise ValueError(f"Start marker {start_idx} is before end marker {end_idx}.")

        # Extract and clean up the snippet
        snippet = text[start_idx:end_idx].strip()
        if len(snippet) == 0:
            raise ValueError(f"snippet is empty, skipping.")

        # Write the cleaned snippet
        with open(cleaned_path, 'w', encoding='utf-8') as f:
            f.write(snippet)
        logger.info(f"Cleaned and saved: {filename}")
