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

async def main_scrape_entsoe_posts(root_url: str, database: PostsDatabase,table_name:str) -> None:
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
            if fnmatch.fnmatch(result.url, "*news/2025/*"):
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

        await asyncio.sleep(5) # to avoid IP blocking

        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")