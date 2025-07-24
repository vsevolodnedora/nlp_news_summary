import asyncio
import time
import fnmatch
import re, os
from datetime import datetime

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

from .utils_scrape import cut_article_text_from_raw_pages

from logger import get_logger
logger = get_logger(__name__)

def extract_and_format_date(markdown: str) -> str | None:
    # Define regex pattern to match dates like "11 June 2024"
    date_pattern = r'\b(\d{1,2}) (January|February|March|April|May|June|July|August|September|October|November|December) (\d{4})\b'

    # Find all matching dates
    matches = re.findall(date_pattern, markdown)

    if matches:
        # Take the last matched date
        day, month, year = matches[-1]
        # Convert to YYYY-MM-DD format
        date_obj = datetime.strptime(f"{day} {month} {year}", "%d %B %Y")
        return date_obj.strftime("%Y-%m-%d")
    else:
        return None

async def scrape_agora_news(root_url:str, output_dir:str, clean_output_dir:str) -> None:

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    async with AsyncWebCrawler() as crawler:

        # Create a filter that only allows URLs with 'guide' in them
        # Create one filter for each required pattern
        url_filter_news = URLPatternFilter(patterns=["*/news-events/*"])
        # url_filter_2025 = URLPatternFilter(patterns=["*2025*"])
        # url_filter_en = URLPatternFilter(patterns=["*_en"])

        # Chain them so all must pass (AND logic)
        filter_chain = FilterChain([
            url_filter_news,
            # url_filter_2025,
            # url_filter_en,
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
        if len(results) == 1:
            logger.warning(f"Only one result found for {root_url}. Suspected limit.")
        # date_pattern = re.compile(r"https?://[^ ]*/\d{4}-\d{2}-\d{2}[^ ]*") # to remove non-articles entries

        logger.info(f"Crawled {len(results)} pages matching '*news-events*'")
        new_articles = []
        for result in results:  # Show first 3 results
            if fnmatch.fnmatch(result.url, '*news-events*') \
                    and result.url.replace(root_url, "") != "" \
                    and "/filter/" not in result.url \
                    and "/page/" not in result.url \
                    and "pdf" not in result.url:

                logger.info(f"Processing {result.url}")
                date_match = extract_and_format_date(result.markdown.raw_markdown)
                if date_match is None:
                    logger.warning(f"Could not extract date for {result.url}")
                    continue

                title_part = result.url.split('/')[-1].replace("-", "_")
                # Combine date and title for the filename
                filename = f"{date_match}__{title_part}.md"

                fpath = f"{output_dir}{filename}"
                fpath_exiected = f"{clean_output_dir}{filename}"
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

def main_scrape_agora_posts(output_dir_raw:str, output_dir_cleaned:str,root_url:str|None=None):
    if root_url is None:
        root_url = "https://www.agora-energiewende.org/news-events" # default path to latest news
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_agora_news(root_url=root_url, output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned))
    # Clean posts raw posts and save clean versions into new foler
    cut_article_text_from_raw_pages(
        RAW_DIR=output_dir_raw,
        CLEANED_DIR=output_dir_cleaned,
        start_markers = [
            "  * Print"
        ],
        end_markers = [
            "##  Stay informed",
            "## Impressions",
            "##  Event details",
            "##  Further reading"
        ],
        max_lines = 30
    )

# Execute the tutorial when run directly
if __name__ == "__main__":
    main_scrape_agora_posts(
        output_dir_raw="../output/posts_raw/agora/",
        output_dir_cleaned="../output/posts_cleaned/agora/",
        root_url="https://www.agora-energiewende.org/news-events",
        # root_url="https://energy.ec.europa.eu/news_en?page=10",
    )