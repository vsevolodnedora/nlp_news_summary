import asyncio
import fnmatch
import os
import re
import time

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

async def scrape_entsoe_news(root_url: str, database: PostsDatabase,table_name:str) -> None:
    """Get news articles from ENTSO-E."""

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
        for result in results:  # Show first 3 results\
            url = result.url
            if fnmatch.fnmatch(result.url, f"*news/2025/*"):
                prefix = "https://www.entsoe.eu/news/"
                if url.startswith(prefix):
                    url_ = url[len(prefix) :]
                else:
                    url_ = url

                # Extract the date and article title
                match = re.match(r"(\d{4}/\d{2}/\d{2})/(.+)", url_)
                if not match:
                    raise ValueError("URL format is unexpected.")
                date_iso = match.group(1).replace("/", "-")  # Format: YYYY-MM-DD

                title_part = match.group(2)
                # Replace hyphens with underscores in the title for readability
                title = title_part.replace("-", "_")

                if database.is_table(table_name=table_name) and database.is_post(table_name=table_name, post_id=database.create_post_id(post_url=url)):
                    logger.info(f"Post already exists in the database. Skipping: {url}")
                    continue

                # store full article in the database
                database.add_post(
                    table_name=table_name,
                    published_on=date_iso,
                    title=title,
                    post_url=url,
                    post=result.markdown.raw_markdown,
                )
                new_articles.append(url)

        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def main_scrape_entsoe_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None) -> None:
    """Collect news articles from ENTSO-E."""

    if root_url is None:
        root_url = "https://www.entsoe.eu/news-events/"

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exist
    news_db.check_create_table(table_name=table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_entsoe_news(
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
    main_scrape_entsoe_posts(
        db_path="../database/scraped_posts.db",
        out_dir="../output/posts_raw/entsoe/",
        table_name="entsoe",
        root_url = "https://www.entsoe.eu/news-events/"
    )