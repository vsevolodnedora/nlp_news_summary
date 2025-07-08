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

async def scrape_ec_news(root_url:str, output_dir:str, clean_output_dir:str) -> None:

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    def url_to_filename(url: str) -> str:
        # Remove the base URL prefix
        # prefix = "https://energy.ec.europa.eu/news/"
        # if url.startswith(prefix):
        #     url = url[len(prefix):]
        url = url.split("/")[-1]

        # Extract the title and date from the URL
        match = re.match(r"(.+)-(\d{4}-\d{2}-\d{2})_en", url)
        if not match:
            raise ValueError("URL format is unexpected.")

        title_part = match.group(1)
        date_part = match.group(2)

        # Replace hyphens with underscores in the title for readability
        title_part = title_part.replace("-", "_")

        # Combine date and title for the filename
        filename = f"{date_part}__{title_part}.md"
        return filename

    async with AsyncWebCrawler() as crawler:

        # Create a filter that only allows URLs with 'guide' in them
        # Create one filter for each required pattern
        url_filter_news = URLPatternFilter(patterns=["*/news/*"])
        # url_filter_2025 = URLPatternFilter(patterns=["*2025*"])
        url_filter_en = URLPatternFilter(patterns=["*_en"])

        # Chain them so all must pass (AND logic)
        filter_chain = FilterChain([
            url_filter_news,
            # url_filter_2025,
            url_filter_en,
        ])

        config = CrawlerRunConfig(
            deep_crawl_strategy=BFSDeepCrawlStrategy(
                max_depth=3,
                include_external=False,
                filter_chain=filter_chain,  # Single filter
            ),
            scraping_strategy=LXMLWebScrapingStrategy(),
            cache_mode=CacheMode.BYPASS,
            verbose=True,
        )

        # collect all results from the webpage
        results = await crawler.arun(url=root_url, config=config)

        # date_pattern = re.compile(r"https?://[^ ]*/\d{4}-\d{2}-\d{2}[^ ]*") # to remove non-articles entries

        logger.info(f"Crawled {len(results)} pages matching '*news*'")
        new_articles = []
        for result in results:  # Show first 3 results
            if fnmatch.fnmatch(result.url, '*news*') and "news_en" not in result.url:
                # date_match = re.search(r"\d{4}-\d{2}-\d{2}", result.url)
                # if date_match is None:
                #     continue

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
        start_markers = [
            "  * News announcement",
            "  * News article",
            "  * Statement"
        ]
        end_markers = [
            "## Related links",
            "## **Related links**",
            "## Related Links",
            "## **Source list for the article data**",
            "Share this page "
        ]

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

def main_scrape_ec_posts(output_dir_raw:str, output_dir_cleaned:str,root_url:str|None=None):
    if root_url is None:
        root_url = "https://energy.ec.europa.eu/news_en" # default path to latest news
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_ec_news(root_url=root_url, output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned))
    # Clean posts raw posts and save clean versions into new foler
    clean_posts(RAW_DIR=output_dir_raw, CLEANED_DIR=output_dir_cleaned)

# Execute the tutorial when run directly
if __name__ == "__main__":
    main_scrape_ec_posts(
        output_dir_raw="../output/posts_raw/ec/",
        output_dir_cleaned="../output/posts_cleaned/ec/",
        root_url="https://energy.ec.europa.eu/news_en",
        # root_url="https://energy.ec.europa.eu/news_en?page=10",
    )