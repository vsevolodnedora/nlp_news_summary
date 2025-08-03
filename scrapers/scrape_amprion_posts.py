import asyncio
import fnmatch
import os
import re
import time
from datetime import datetime

from crawl4ai import AsyncWebCrawler, CacheMode, CrawlerRunConfig
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy, BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.filters import (
    ContentRelevanceFilter,
    ContentTypeFilter,
    DomainFilter,
    FilterChain,
    SEOFilter,
    URLPatternFilter,
)
from crawl4ai.deep_crawling.scorers import (
    KeywordRelevanceScorer,
)

from database import PostsDatabase
from logger import get_logger

logger = get_logger(__name__)

def find_and_format_numeric_date(text:str)->str|None:
    """Extract date from markdown."""
    pattern = r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b"
    match = re.search(pattern, text)

    if not match:
        return None

    day, month, year = match.groups()
    return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")

async def scrape_amprion_news(root_url:str, table_name:str, database: PostsDatabase) -> None:
    """Scrape posts from amprion news page."""
    async with AsyncWebCrawler() as crawler:

        # Create a filter that only allows URLs with 'guide' in them
        url_filter_news = URLPatternFilter(patterns=["*Presse*"])

        # Chain them so all must pass (AND logic)
        filter_chain = FilterChain([
            url_filter_news,
        ])

        config = CrawlerRunConfig(
            deep_crawl_strategy=BFSDeepCrawlStrategy(
                max_depth=2,
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

        logger.info(f"Crawled {len(results)} pages matching '*news*'")
        new_articles = []
        for result in results:  # Show first 3 results
            url = result.url

            if database.is_table(table_name=table_name) and database.is_post(table_name=table_name, post_id=database.create_post_id(post_url=url)):
                logger.info(f"Post already exists in the database. Skipping: {url}")
                continue

            if fnmatch.fnmatch(url, "*Presse*"):

                date_iso = find_and_format_numeric_date(text=result.markdown.raw_markdown)
                if date_iso is None:
                    logger.warning(f"No date found. Skipping: {url}")
                    continue

                # Replace hyphens with underscores in the title for readability
                title = url.split("/")[-1].replace("-", "_")

                # store full article in the database
                database.add_post(
                    table_name=table_name,
                    published_on=date_iso,
                    title=title,
                    post_url=url,
                    post=result.markdown.raw_markdown,
                )

        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def main_scrape_amprion_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None):
    """Scrape transnetbw news articles database."""
    if root_url is None:
        root_url = "https://www.amprion.net/" # default path to latest news

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exists
    news_db.check_create_table(table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_amprion_news(
                root_url=root_url,
                table_name=table_name,
                database=news_db
            )
        )
    except Exception as e:
        logger.error(f"Failed to '{table_name}' run scraper. Aborting... Error raised: {e}")
        news_db.close()
        return

    # save scraped posts as raw .md files for analysis
    news_db.dump_posts_as_markdown(table_name=table_name, out_dir=out_dir)

    news_db.close()

# Execute the tutorial when run directly
if __name__ == "__main__":

    main_scrape_amprion_posts(
        db_path="../database/scraped_posts.db",
        root_url="https://www.amprion.net/",
        table_name="amprion",
        out_dir="../output/posts_raw/amprion/",
    )