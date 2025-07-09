import asyncio
import time
import fnmatch
import re, os

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

async def scrape_entsoe_news(output_dir:str, clean_output_dir:str, root_url:str) -> None:

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    def url_to_filename(url:str)->str:
        # Remove the base URL prefix
        prefix = "https://www.entsoe.eu/news/"
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

    async with AsyncWebCrawler() as crawler:

        # Create a filter that only allows URLs with 'guide' in them
        url_filter = URLPatternFilter(patterns=["*news*"])

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
        results = await crawler.arun(url=root_url, config=config)
        if len(results) == 1:
            logger.warning(f"Only one result found for {root_url}. Suspected limit.")

        logger.info(f"Crawled {len(results)} pages matching '*news*'")
        new_articles = []
        for result in results:  # Show first 3 results
            if fnmatch.fnmatch(result.url, '*news/2025/*'):
                fname = url_to_filename(result.url)
                fpath = f"{output_dir}{fname}"
                fpath_exiected = f"{clean_output_dir}{fname}"
                if not os.path.exists(fpath_exiected):
                    res = result.markdown.raw_markdown
                    new_articles.append(result.url)
                    logger.debug(f"Saving article {result.url} | Length: {len(result.markdown.raw_markdown)} chars ")
                    os.makedirs(os.path.dirname(fpath), exist_ok=True)
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(res)
                else:
                    logger.debug(f"Article already processed: {fpath}")
        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def clean_posts(RAW_DIR:str, CLEANED_DIR:str):
    """
    Loop through markdown files in RAW_DIR, extract content between 'Button' and 'Share this article',
    and save to CLEANED_DIR with the same filename if not already present.
    """
    # Ensure the cleaned directory exists
    os.makedirs(CLEANED_DIR, exist_ok=True)

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

        # Find the extraction boundaries
        start_marker = 'Button'
        end_marker = 'Share this article'

        start_idx = text.find(start_marker)
        if start_idx == -1:
            raise ValueError(f"Start marker not found in {filename}, skipping.")
            # continue
        start_idx += len(start_marker)

        end_idx = text.find(end_marker, start_idx)
        if end_idx == -1:
            raise ValueError(f"End marker not found in {filename}, skipping.")
            # continue

        # Extract and clean up the snippet
        snippet = text[start_idx:end_idx].strip()

        # Write the cleaned snippet
        with open(cleaned_path, 'w', encoding='utf-8') as f:
            f.write(snippet)
        logger.info(f"Cleaned and saved: {filename}")

def main_scrape_entsoe_posts(output_dir_raw:str, output_dir_cleaned:str, root_url:str|None=None) -> None:
    if root_url is None:
        root_url = "https://www.entsoe.eu/news-events/"
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_entsoe_news(output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned, root_url=root_url))
    # Clean posts raw posts and save clean versions into new foler
    clean_posts(RAW_DIR=output_dir_raw, CLEANED_DIR=output_dir_cleaned)

# Execute the tutorial when run directly
if __name__ == "__main__":
    main_scrape_entsoe_posts(
        output_dir_raw="../output/posts_raw/entsoe/",
        output_dir_cleaned="../output/posts_cleaned/entsoe/"
    )