import asyncio
import time
import fnmatch
import re, os

from crawl4ai import CrawlerRunConfig, AsyncWebCrawler, CacheMode
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy, BestFirstCrawlingStrategy, DFSDeepCrawlStrategy
from bs4 import BeautifulSoup
import aiohttp
import urllib.parse
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
from crawl4ai.types import URLFilter

from .utils_scrape import cut_article_text_from_raw_pages

from logger import get_logger
logger = get_logger(__name__)

def url_to_filename(url: str) -> str:
    # Extract the filename from the URL
    url = url.split("/")[-1]

    # Match the pattern: date (YYYYMMDD) + underscore + title + optional extension
    match = re.match(r"(\d{4})(\d{2})(\d{2})_([^\.]+)", url)
    if not match:
        raise ValueError("URL format is unexpected: {}".format(url))

    year, month, day, title_part = match.groups()

    # Format the date as YYYY-MM-DD
    date_part = f"{year}-{month}-{day}"

    # Replace hyphens with underscores in the title for readability
    title_part = title_part.replace("-", "_")

    # Combine date and title for the filename
    filename = f"{date_part}__{title_part}.md"
    return filename

async def scrape_bnetza_news(root_url: str, output_dir: str, clean_output_dir: str, news_href_part:str="SharedDocs/Pressemitteilungen/DE/2025") -> None:
    """
    Fetch the root page HTML, extract all links to BNetzA with news_href_part (press-releases)
    then scrape each article as Markdown using crawl4AI and save new ones.
    Special approach is needed due to old HTML page technology.
    """
    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    # Step 1: Download and parse root HTML
    async with aiohttp.ClientSession() as session:
        async with session.get(root_url) as response:
            response.raise_for_status()
            html = await response.text()

    soup = BeautifulSoup(html, 'lxml')

    # Step 2: Collect all hrefs matching news_href_part
    links = set()
    root_url_=root_url.replace("DE/Allgemeines/Aktuelles/", "") # remove part not in the news article link...
    for a in soup.find_all('a', href=True):
        href = a['href']
        if news_href_part in href:
            # create full news artcile url
            full_url = urllib.parse.urljoin(root_url_, href)
            print(full_url)
            links.add(full_url)

    if not links:
        logger.warning(f"No press-release links found at {root_url}")
        return

    logger.info(f"Found {len(links)} candidate press-release URLs")

    new_links = []
    for link in links:
        article_url = link.split('?', 1)[0]
        filename = url_to_filename(article_url)

        clean_path = os.path.join(clean_output_dir, filename)
        if not os.path.exists(clean_path):
            new_links.append(link)

    logger.info(f"Selected {len(new_links)} out of {len(links)} new links")

    # Step 3: Crawl each article and save as Markdown
    new_articles = []
    async with AsyncWebCrawler() as crawler:
        for url in new_links:
            # Configure crawler to fetch only the target page (no deep crawl)
            config = CrawlerRunConfig(
                deep_crawl_strategy=BFSDeepCrawlStrategy(
                    max_depth=0,
                    include_external=False,
                    filter_chain=FilterChain([]),
                ),
                scraping_strategy=LXMLWebScrapingStrategy(),
                cache_mode=CacheMode.BYPASS,
                verbose=True,
            )

            results = await crawler.arun(url=url, config=config)
            if not results:
                logger.warning(f"No crawl result for {url}")
                continue

            # Expect one result per URL
            result = results[0]
            article_url = url.split('?', 1)[0]
            filename = url_to_filename(article_url)

            out_path = os.path.join(output_dir, filename)

            raw_md = result.markdown.raw_markdown
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(raw_md)

            new_articles.append(article_url)
            logger.info(f"Saved article {article_url} (size: {len(raw_md)} chars)")

    logger.info(f"Finished saving {len(new_articles)} new articles out of {len(links)} links")

def main_scrape_bnetza_posts(output_dir_raw:str, output_dir_cleaned:str,root_url:str|None=None):
    if root_url is None:
        root_url = "https://www.bundesnetzagentur.de/DE/Allgemeines/Aktuelles/start.html" # default path to latest news
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_bnetza_news(root_url=root_url, output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned))
    # Clean posts raw posts and save clean versions into new foler
    cut_article_text_from_raw_pages(
        RAW_DIR=output_dir_raw,
        CLEANED_DIR=output_dir_cleaned,
        start_markers = [
            "[Pressemitteilungen](https://www.bundesnetzagentur.de/SharedDocs"
        ],
        end_markers = [
            "[](javascript:void\(0\);) **Inhalte teilen**"
        ],
        skip_start_lines=1
    )

# Execute the tutorial when run directly
if __name__ == "__main__":
    main_scrape_bnetza_posts(
        output_dir_raw="../output/posts_raw/bnetza/",
        output_dir_cleaned="../output/posts_cleaned/bnetza/",
        root_url="https://www.bundesnetzagentur.de/DE/Allgemeines/Aktuelles/start.html",
    )