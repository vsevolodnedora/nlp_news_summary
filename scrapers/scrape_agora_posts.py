import asyncio
import time
import fnmatch
import re, os
from datetime import datetime
from typing import Optional, List, Dict

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

from database import PostsDatabase

from logger import get_logger
logger = get_logger(__name__)

def extract_and_format_date(markdown: str) -> str | None:
    """Extract date from markdown"""

    # Define regex pattern to match dates like "11 June 2024"
    date_pattern = r"\b(\d{1,2}) (January|February|March|April|May|June|July|August|September|October|November|December) (\d{4})\b"

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

async def scrape_agora_news(root_url:str, database: PostsDatabase, table_name:str) -> None:
    """Scrape agora news articles from agora webpage"""

    async with AsyncWebCrawler() as crawler:

        url_filter_news = URLPatternFilter(patterns=["*/news-events/*"])

        # Chain them so all must pass (AND logic)
        filter_chain = FilterChain([
            url_filter_news,
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

        logger.info(f"Crawled {len(results)} pages matching '*news-events*'")
        new_articles = []
        for result in results:  # Show first 3 results
            url = result.url
            if fnmatch.fnmatch(url, "*news-events*") \
                    and url.replace(root_url, "") != "" \
                    and "/filter/" not in url \
                    and "/page/" not in url \
                    and "pdf" not in url:

                if database.is_table(table_name=table_name) and database.is_post(table_name=table_name, post_id=database.create_post_id(post_url=url)):
                    logger.info(f"Post already exists in the database. Skipping: {url}")
                    continue

                logger.info(f"Processing {url}")
                date_iso = extract_and_format_date(result.markdown.raw_markdown) # Date in YYYY-MM-DD
                if date_iso is None:
                    logger.warning(f"Could not extract date for {url}")
                    continue
                url = url.split("?")[0]
                title_part = url.split("/")[-1].replace("-", "_")

                # store full article in the database
                database.add_post(
                    table_name="agora",
                    published_on=date_iso,
                    title=title_part,
                    post_url=url,
                    post=result.markdown.raw_markdown,
                )

        await asyncio.sleep(5) # to avoid IP blocking

        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def main_scrape_agora_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None):
    """Scrape agora news articles from agora webpage"""

    if root_url is None:
        root_url = "https://www.agora-energiewende.org/news-events" # default path to latest news

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exist
    news_db.check_create_table(table_name=table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_agora_news(
                root_url=root_url,
                database=news_db,
                table_name=table_name,
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
    main_scrape_agora_posts(
        root_url="https://www.agora-energiewende.org/news-events",
        table_name="agora",
        db_path="../database/scraped_posts.db",
        out_dir="../output/posts_raw/agora/",
    )