import asyncio
import time
import fnmatch
import re, os
from math import lgamma
from pathlib import Path
from crawl4ai import CrawlerRunConfig, AsyncWebCrawler, CacheMode
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy, BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
    URLPatternFilter,
    DomainFilter,
    ContentTypeFilter,
    ContentRelevanceFilter,
    SEOFilter,
)
from crawl4ai.deep_crawling.scorers import (
    KeywordRelevanceScorer,
)

from logger import get_logger
logger = get_logger(__name__)

def url_to_filename(url:str)->str:
    # Remove the base URL prefix
    prefix = "https://www.eex.com/en/newsroom/news?tx_news_pi1%5Bcontroller%5D=News"
    if url.startswith(prefix):
        url = url[len(prefix):]

    # Extract the date and article title
    match = re.match(r"(\d{4}/\d{2}/\d{2})/(.+)", url)
    if not match:
        raise ValueError("URL format is unexpected.")

    date_part = match.group(1).replace("/", "-")  # Format: YYYY-MM-DD
    title_part = match.group(2)

    # Replace hyphens with underscores in the title for readability
    title_part = title_part.replace("-", "_")

    # Combine date and title for the filename
    filename = f"{date_part}_{title_part}.md"
    return filename

def extract_date_from_markdown(markdown_text):
    # Split text into lines
    lines = markdown_text.splitlines()

    # Pattern to match the date format DD/MM/YYYY
    date_pattern = r"\b(\d{2}/\d{2}/\d{4})\b"

    for line in lines:
        if "EEX Press Release" in line or "Volume Report" in line:
            # Search for the date pattern in the line
            match = re.search(date_pattern, line)
            if match:
                return match.group(1).replace("/", "-")

    return None

def reformat_date(date_str):
    # Split the string by the dash
    month, day, year = date_str.split('-')
    # Rearrange and return in YYYY-MM-DD format
    return f"{year}-{month}-{day}"

def invert_date_format(date_str):
    # Split the string by the dash
    year, month, day = date_str.split('-')
    # Rearrange and return in MM-DD-YYYY format
    return f"{month}-{day}-{year}"

async def scrape_eex_news(output_dir:str, clean_output_dir:str) -> None:
    """
    https://www.eex.com/en/newsroom/news?tx_news_pi1%5Bcontroller%5D=News&tx_news_pi1%5BcurrentPage%5D=2&tx_news_pi1%5Bsearch%5D%5BfilteredCategories%5D=&tx_news_pi1%5Bsearch%5D%5BfromDate%5D=&tx_news_pi1%5Bsearch%5D%5Bsubject%5D=&tx_news_pi1%5Bsearch%5D%5BtoDate%5D=&cHash=83e307337837c6f5bd5e40a530acad7a
    :param date_int:
    :param output_dir:
    :param clean_output_dir:
    :return:
    """

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    async with (AsyncWebCrawler() as crawler):

        # Create a filter that only allows URLs with 'guide' in them
        url_filter = URLPatternFilter(patterns=["*_news_*"])

        config = CrawlerRunConfig(
            deep_crawl_strategy=BFSDeepCrawlStrategy(
                max_depth=2,
                include_external=False,
                filter_chain=FilterChain([url_filter]),  # Single filter
            ),
            scraping_strategy=LXMLWebScrapingStrategy(),
            cache_mode=CacheMode.BYPASS,
            verbose=True,
        )

        # collect all results from the webpage
        results = await crawler.arun(url="https://www.eex.com/en/newsroom/", config=config)

        logger.info(f"Crawled {len(results)} pages matching '*_news_*'")
        new_articles = []
        for result in results:  # Show first 3 results
            if fnmatch.fnmatch(result.url, '*_news_*') \
                    and ("EEX Press Release" in result.markdown.raw_markdown or "Volume Report" in result.markdown.raw_markdown) \
                    and "_news_" in result.url:

                date = extract_date_from_markdown(result.markdown.raw_markdown)

                if date is None:
                    logger.info(f"Skipping {result.url}. Could not extract date from markdown.")
                    continue

                new_articles.append(result.url)
                if date is not None:
                    fname = reformat_date(date) # to YYYY-MM-DD
                    if ("EEX Press Release" in result.markdown.raw_markdown):
                        fname+="__eex_press_release"
                    elif ("Volume Report" in result.markdown.raw_markdown):
                        fname+="__volume_report"
                    else:
                        fname+="__unknown"

                fpath = os.path.join(output_dir, f"{fname}.md")
                fpath_expected = os.path.join(clean_output_dir, f"{fname}.md")
                if not os.path.exists(fpath_expected):
                    res = result.markdown.raw_markdown
                    new_articles.append(result.url)
                    logger.debug(f"Saving article {result.url} | Length: {len(result.markdown.raw_markdown)} chars ")
                    os.makedirs(os.path.dirname(fpath), exist_ok=True)
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(res)
                else:
                    logger.debug(f"Article already processed: {fpath}")
        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def process_eex_press_releases(input_dir: str, output_dir: str) -> None:
    """
    Process EEX press release markdown files from input_dir, extracting content between the
    '# EEX Press Release -' header and the '**CONTACT**' marker, writing cleaned files
    into output_dir with the same filenames. Only processes files not already present in output_dir.
    Logs each processed file with logger.info().

    :param input_dir: Path to folder containing raw eex press release files.
    :param output_dir: Path to folder where cleaned files will be written.
    """

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    files = input_path.glob("*__eex_press_release.md")

    # Pattern: YYYY-MM-DD__eex_press_release.md
    for file_path in files:
        output_file = output_path / file_path.name
        if output_file.exists():
            continue  # Skip already processed files

        # Extract and reformat date from filename
        date_part = file_path.stem.split("__")[0]  # 'YYYY-MM-DD'
        date_part = invert_date_format(date_part)
        formatted_date = date_part.replace("-", "/")  # 'YYYY/MM/DD'

        # Read all lines, keeping newline characters
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines(keepends=True)

        # Find start of the press release content
        start_index = next(
            (i for i, line in enumerate(lines) if line.startswith("# EEX Press Release -")),
            None
        )
        if start_index is None:
            start_index = next(
                (i for i, line in enumerate(lines) if line.__contains__(formatted_date)),
                None
            )
            if not start_index is None:
                start_index-=1 # regress to include title

        # Find end marker for the press release content
        end_index = next(
            (i for i, line in enumerate(lines) if line.strip().startswith("**CONTACT**")),
            None
        )
        if end_index is None:
            end_index = next(
                (i for i, line in enumerate(lines)
                 if line.strip().startswith("**_Contacts:_**") or line.strip().startswith("**Contact**")
                 ),
                None
            )

        if start_index is None or end_index is None:
            logger.warning(f"Skipping {file_path.name}: start or end marker not found")
            continue

        # Extract relevant section
        cleaned_lines = lines[start_index:end_index]

        # Write cleaned content to new file
        output_file.write_text("".join(cleaned_lines), encoding="utf-8")

        # Log the processed file
        logger.info(f"Processed {file_path.name} (date {formatted_date})")

def main_scrape_eex_posts(output_dir_raw:str, output_dir_cleaned:str):
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_eex_news(output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned))
    # Clean posts raw posts and save clean versions into new foler
    process_eex_press_releases(input_dir=output_dir_raw, output_dir=output_dir_cleaned)

# Execute the tutorial when run directly
if __name__ == "__main__":
    main_scrape_eex_posts(
        output_dir_raw="../output/posts_raw/eex/",
        output_dir_cleaned="../output/posts_cleaned/eex/"
    )