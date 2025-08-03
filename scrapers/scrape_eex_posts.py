import asyncio
import time
import fnmatch
import re, os
from math import lgamma
from pathlib import Path
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

def extract_date_from_markdown(markdown_text:str):
    """Extract date from markdown text."""

    # Split text into lines
    lines = markdown_text.splitlines()

    # Pattern to match the date format DD/MM/YYYY
    date_pattern = r"\b(\d{2}/\d{2}/\d{4})\b"

    date_str = ""
    for line in lines:
        if "EEX Press Release" in line or "Volume Report" in line:
            # Search for the date pattern in the line
            match = re.search(date_pattern, line)
            if match:
                date_str = match.group(1).replace("/", "-")

    if date_str == "":
        return None

    month, day, year = date_str.split("-")
    # Rearrange and return in YYYY-MM-DD format
    return f"{year}-{month}-{day}"


def invert_date_format(date_str:str):
    """Invert date format."""

    # Split the string by the dash
    year, month, day = date_str.split("-")
    # Rearrange and return in MM-DD-YYYY format
    return f"{month}-{day}-{year}"

async def scrape_eex_news(root_url:str, table_name:str, database: PostsDatabase) -> None:
    """
    Scrape EEX news posts.

    https://www.eex.com/en/newsroom/news?tx_news_pi1%5Bcontroller%5D=News&tx_news_pi1%5BcurrentPage%5D=2&tx_news_pi1%5Bsearch%5D%5BfilteredCategories%5D=&tx_news_pi1%5Bsearch%5D%5BfromDate%5D=&tx_news_pi1%5Bsearch%5D%5Bsubject%5D=&tx_news_pi1%5Bsearch%5D%5BtoDate%5D=&cHash=83e307337837c6f5bd5e40a530acad7a
    :param date_int:
    :param output_dir:
    :param clean_output_dir:
    :return:
    """

    async with (AsyncWebCrawler() as crawler):

        # Create a filter that only allows URLs with 'guide' in them
        url_filter = URLPatternFilter(patterns=["*_news_*"])

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

        logger.info(f"Crawled {len(results)} pages matching '*_news_*'")
        new_articles = []
        for result in results:  # Show first 3 results
            url = result.url

            if fnmatch.fnmatch(url, "*_news_*") \
                    and ("EEX Press Release" in result.markdown.raw_markdown or "Volume Report" in result.markdown.raw_markdown) \
                    and "_news_" in url:

                # extract date from markdown
                date_iso = extract_date_from_markdown(result.markdown.raw_markdown) # YYYY-MM-DD

                if date_iso is None:
                    logger.debug(f"Skipping scraped markdown from {url}. Could not extract date from markdown.")
                    continue

                # select title based on the contenct
                if ("EEX Press Release" in result.markdown.raw_markdown):
                    title="eex_press_release"
                elif ("Volume Report" in result.markdown.raw_markdown):
                    title="volume_report"
                else:
                    title="unknown"

                if database.is_table(table_name=table_name) and database.is_post(table_name=table_name, post_id=database.create_post_id(post_url=url)):
                    logger.info(f"Post already exists in the database. Skipping: {url}")
                    continue

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

def main_scrape_eex_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None) -> None:
    """Scrape EEX news posts."""

    if root_url is None:
        root_url = "https://www.eex.com/en/newsroom/"

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exists
    news_db.check_create_table(table_name=table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_eex_news(
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
    main_scrape_eex_posts(
        db_path="../database/scraped_posts.db",
        root_url="https://www.eex.com/en/newsroom/",
        out_dir="../output/posts_raw/eex/",
        table_name="eex"
    )