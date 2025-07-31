import asyncio
import time
import fnmatch
import re
import os

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

async def scrape_icis_news(root_url:str, database: PostsDatabase, table_name:str) -> None:
    """Scrape ICIS news posts from webpage."""
    async with AsyncWebCrawler() as crawler:

        url_filter_news = URLPatternFilter(patterns=["*/news/*"])

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

        # date_pattern = re.compile(r"https?://[^ ]*/\d{4}-\d{2}-\d{2}[^ ]*") # to remove non-articles entries

        year = 2025

        logger.info(f"Crawled {len(results)} pages matching '*news*'")
        new_articles = []
        for result in results:  # Show first 3 results
            url = result.url
            if fnmatch.fnmatch(result.url, "*news*") and str(year) in result.url and "news_id" not in result.url:

                # Extract the title and date from the URL
                match = re.match(r".*/news/(\d{4})/(\d{2})/(\d{2})/\d+/([\w-]+)", url)
                if not match:
                    raise ValueError("URL format is unexpected.")

                year, month, day = match.group(1), match.group(2), match.group(3)
                date_iso = f"{year}-{month}-{day}"  # e.g., 2025-07-07

                # Replace hyphens with underscores in the title for readability
                title_part = url.split("/")[-1]
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

def main_scrape_icis_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None):
    """Scrape acer news posts from ICIS webpage."""
    if root_url is None:
        root_url = "https://www.icis.com/explore/resources/news/?page_number=1" # default path to latest news

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exist
    news_db.check_create_table(table_name=table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_icis_news(
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

    main_scrape_icis_posts(
        db_path="../database/scraped_posts.db",
        table_name="icis",
        out_dir="../output/posts_raw/icis/",
        root_url="https://www.icis.com/explore/resources/news/",  # page number has no effect
    )