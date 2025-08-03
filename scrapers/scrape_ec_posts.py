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

async def scrape_ec_news(root_url:str, table_name:str, database: PostsDatabase) -> None:
    """Scrape posts from ec news page."""
    async with AsyncWebCrawler() as crawler:

        # Create a filter that only allows URLs with 'guide' in them
        url_filter_news = URLPatternFilter(patterns=["*/news/*"])
        url_filter_en = URLPatternFilter(patterns=["*_en"])

        # Chain them so all must pass (AND logic)
        filter_chain = FilterChain([
            url_filter_news,
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

            if fnmatch.fnmatch(url, "*news*") and "news_en" not in url:
                # Extract the title and date from the URL
                match = re.match(r"(.+)-(\d{4}-\d{2}-\d{2})_en", url.split("/")[-1])
                if not match:
                    raise ValueError("URL format is unexpected.")

                title = match.group(1)
                date_iso = match.group(2)

                # Replace hyphens with underscores in the title for readability
                title = title.replace("-", "_")

                # store full article in the database
                database.add_post(
                    table_name=table_name,
                    published_on=date_iso,
                    title=title,
                    post_url=url,
                    post=result.markdown.raw_markdown,
                )

        await asyncio.sleep(5) # to avoid IP blocking

        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def main_scrape_ec_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None):
    """Scrape ec news articles database."""

    if root_url is None:
        root_url = "https://energy.ec.europa.eu/news_en" # default path to latest news

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exists
    news_db.check_create_table(table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_ec_news(
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

    #historic backfill
    for i in range(10, 0,-1):
        main_scrape_ec_posts(
            db_path="../database/scraped_posts.db",
            root_url=f"https://energy.ec.europa.eu/news_en?page={i + 1}",
            table_name="ec",
            out_dir="../output/posts_raw/ec/",
        )

    main_scrape_ec_posts(
        db_path="../database/scraped_posts.db",
        root_url="https://energy.ec.europa.eu/news_en",
        table_name="ec",
        out_dir="../output/posts_raw/ec/",
    )