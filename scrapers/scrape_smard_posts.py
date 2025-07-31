"""
Scraping SMARD news I had to fall back on processing each HTML manually since
information is commonly spread our between charts which themselves are not loaded.
In order to prevent overloading LLMs with useless chart technical messeges I remove them manually
using two lists of blacklisted strings as well as manually removing whole blocks of text that contain
references to "Highcharts"
"""

import asyncio
import time
import fnmatch
import re, os
import langid # to detect english articles
from datetime import datetime
from collections import defaultdict

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

async def scrape_smard_news(root_url:str, database: PostsDatabase, table_name:str) -> None:
    """Scrape acer news posts from news-and-engagement database."""
    async with AsyncWebCrawler() as crawler:

        # Create a filter that only allows URLs with 'guide' in them
        # Create one filter for each required pattern
        url_filter_news = URLPatternFilter(patterns=["*smard*"])
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

        # inform about which to process and which to skip
        count = 0
        for result in results:
            if "topic-article" in result.url:
                logger.debug(f"Found article {result.url}")
                count += 1
            else:
                logger.debug(f"Rejecting article {result.url}")
        logger.info(f"Crawled {count} pages matching '*news*' and selected {count} articles with 'topic-article' in url")

        new_articles = []
        for result in results:  # Show first 3 results
            url = result.url
            if fnmatch.fnmatch(result.url, "*topic-article*"):
                # Try to match "YYYY.MM.DD" format first
                full_date_match = re.search(r"\b(\d{2})\.(\d{2})\.(\d{4})\b", result.markdown)

                date_iso = ""
                if full_date_match:
                    day, month, year = full_date_match.groups()
                    date_iso = f"{year}-{month}-{day}"
                else:
                    # Try to match "DD Month YYYY" format
                    date_match = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", result.markdown)
                    if date_match:
                        try:
                            parsed_date = datetime.strptime(f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}", "%d %B %Y")
                            date_iso = parsed_date.strftime("%Y-%m-%d")
                        except ValueError:
                            logger.error(f"Invalid date format in markdown for URL: {url}")
                            continue
                    else:
                        logger.error(f"Date not found in markdown for URL: {url}")
                        continue

                # Extract the last segment of the URL for the title part
                title = url.split("/")[-1].replace("-", "_")

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

def main_scrape_smard_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None):
    """Scrape smard news posts from acer webpage."""
    if root_url is None:
        root_url = "https://www.smard.de/home/energiemarkt-aktuell/energiemarkt-aktuell" # default path to latest news

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exist
    news_db.check_create_table(table_name=table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_smard_news(
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
    main_scrape_smard_posts(
        db_path="../database/scraped_posts.db",
        table_name="smard",
        out_dir="../output/posts_raw/smard/",
        root_url="https://www.smard.de/home/energiemarkt-aktuell/energiemarkt-aktuell",
    )