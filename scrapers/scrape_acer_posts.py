import asyncio
import time
import fnmatch
import re, os
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

async def scrape_acer_news(output_dir:str, clean_output_dir:str) -> None:

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    def url_to_filename(url:str, md:str)->str:
        # Remove the base URL prefix
        prefix = "https://www.acer.europa.eu/news/"
        if url.startswith(prefix):
            url = url[len(prefix):]

        # Extract the date and article title
        match = re.search(r'\b\d{1,2}\.\d{1,2}\.\d{4}\b', md)
        if not match:
            raise ValueError("URL format is unexpected.")


        day, month, year = match.group().split('.') # Assume that it is DD.MM.YYYY format
        date_part = f"{year}-{int(month):02d}-{int(day):02d}"  # Ensure two-digit month/day

        # Replace hyphens with underscores in the title for readability
        title_part = url.replace("-", "_")

        # Combine date and title for the filename
        filename = f"{date_part}__{title_part}.md"
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
        results = await crawler.arun(url="https://www.acer.europa.eu/news-and-events/news", config=config)

        logger.info(f"Crawled {len(results)} pages matching '*news*'")
        new_articles = []
        for result in results:  # Show first 3 results
            # skip over non-news pages (note this skips some news articles)
            if fnmatch.fnmatch(result.url, '*news*') \
                    and not "news-and-events" in result.url \
                    and not "news-and-engagement" in result.url\
                    and not "events-and-engagement" in result.url:
                # search for a publication date in DD.MM.YYYY formation to distinguish news articles from other posts
                match = re.search(r'\b\d{1,2}\.\d{1,2}\.\d{4}\b', result.markdown)
                if match:
                    fname = url_to_filename(result.url, result.markdown)
                    fpath = f"{output_dir}{fname}"
                    fpath_exiected = f"{clean_output_dir}{fname}"
                    if not os.path.exists(fpath_exiected):
                        res = result.markdown.raw_markdown
                        new_articles.append(result.url)
                        logger.info(f"Saving article {result.url} | Length: {len(result.markdown.raw_markdown)} chars | {fpath} ")
                        os.makedirs(os.path.dirname(fpath), exist_ok=True)
                        with open(fpath, "w", encoding="utf-8") as f:
                            f.write(res)
                    else:
                        logger.debug(f"Article already processed: {fpath}")
        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def invert_date_format(date_str):
    # Split the string by the dash
    year, month, day = date_str.split('-')
    # Rearrange and return in MM-DD-YYYY format
    return f"{int(day)}.{int(month)}.{year}"

def process_acer_press_releases(input_dir: str, output_dir: str) -> None:
    """
    Process ACER press release markdown files from input_dir, extracting content between the

    :param input_dir: Path to folder containing raw eex press release files.
    :param output_dir: Path to folder where cleaned files will be written.
    """

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    files = input_path.glob("*.md")

    # Pattern: YYYY-MM-DD__eex_press_release.md
    for file_path in files:
        output_file = output_path / file_path.name
        if output_file.exists():
            continue  # Skip already processed files

        # Extract and reformat date from filename
        date_part = file_path.stem.split("__")[0]  # 'YYYY-MM-DD'
        formatted_date = invert_date_format(date_part)

        # Read all lines, keeping newline characters
        text = file_path.read_text(encoding="utf-8")
        lines = text.splitlines(keepends=True)

        # Find start of the press release content
        start_index = next(
            (i for i, line in enumerate(lines) if line.__contains__(formatted_date)),
            None
        )

        # Find end marker for the press release content
        end_index = next(
            (i for i, line in enumerate(lines) if line.strip().startswith("![acer]")),
            None
        )

        if start_index is None or end_index is None:
            logger.warning(f"Skipping {file_path.name}: start={start_index} or end={end_index} marker not found")
            continue

        # Extract relevant section
        cleaned_lines = lines[start_index:end_index]

        # Write cleaned content to new file
        output_file.write_text("".join(cleaned_lines), encoding="utf-8")

        # Log the processed file
        logger.info(f"Processed {file_path.name} (date {formatted_date})")

def main_scrape_acer_posts(output_dir_raw:str, output_dir_cleaned:str):
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_acer_news(output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned))
    # Process posts raw posts and save clean versions into new foler
    process_acer_press_releases(input_dir=output_dir_raw, output_dir=output_dir_cleaned)

# Execute the tutorial when run directly
if __name__ == "__main__":
    main_scrape_acer_posts(
        output_dir_raw="../output/posts_raw/acer/",
        output_dir_cleaned="../output/posts_cleaned/acer/"
    )