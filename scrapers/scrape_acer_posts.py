import asyncio
import csv
import fnmatch
import hashlib
import os
import re
import sqlite3
import time
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

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

async def scrape_acer_news(
    root_url: str,
    database: PostsDatabase,
    table_name:str
) -> None:
    """Scrape acer news posts from news-and-engagement database."""
    async with AsyncWebCrawler() as crawler:
        url_filter = URLPatternFilter(patterns=["*news*"])
        config = CrawlerRunConfig(
            deep_crawl_strategy=BFSDeepCrawlStrategy(
                max_depth=2,
                include_external=False,
                filter_chain=FilterChain([url_filter]),
            ),
            scraping_strategy=LXMLWebScrapingStrategy(),
            cache_mode=CacheMode.BYPASS,
            verbose=True,
        )

        results = await crawler.arun(url=root_url, config=config)
        if len(results) == 1:
            logger.warning(f"Only one result found for {root_url}. Suspected limit.")

        logger.info(f"Crawled {len(results)} pages matching '*news*'")
        for result in results:
            logger.debug(f"\tCrawled {result.url}")

        new_articles = []
        for result in results:
            url = result.url
            if (
                fnmatch.fnmatch(url, "*news*")
                and "news-and-events" not in url
                and "news-and-engagement" not in url
                and "events-and-engagement" not in url
            ):
                url = url.split("?")[0]

                match = re.search(r"\b\d{1,2}\.\d{1,2}\.\d{4}\b", result.markdown)
                if not match:
                    logger.warning(f"No date found in {url}; skipping.")
                    continue

                if database.is_table(table_name=table_name) and database.is_post(table_name=table_name, post_id=database.create_post_id(post_url=url)):
                    logger.info(f"Post already exists in the database. Skipping: {url}")
                    continue

                # parse date DD.MM.YYYY -> YYYY-MM-DD
                day, month, year = match.group().split(".")
                date_iso = f"{year}-{int(month):02d}-{int(day):02d} 12:00" # YYYY-MM-DD HH:MM
                title = url.rstrip("/").split("/")[-1].replace("-", "_")

                # store full article in the database
                database.add_post(
                    table_name=table_name,
                    published_on=date_iso,
                    title=title,
                    post_url=url,
                    post=result.markdown.raw_markdown,
                )
                new_articles.append(url)


        logger.info(
            f"Finished: {len(new_articles)} new articles out of {len(results)} crawled."
        )


def main_scrape_acer_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None):
    """Scrape acer news posts from acer webpage."""
    if root_url is None:
        root_url = "https://www.acer.europa.eu/news-and-events/news"

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exist
    news_db.check_create_table(table_name=table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_acer_news(
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
    main_scrape_acer_posts(
        db_path="../database/scraped_posts.db",
        table_name="acer",
        out_dir="../output/posts_raw/acer/",
        root_url = "https://www.acer.europa.eu/news-and-events/news"
    )