import asyncio
import copy
import gc
import random
import re
from datetime import datetime
from typing import List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, MemoryAdaptiveDispatcher, RateLimiter, html2text
from crawl4ai.components.crawler_monitor import CrawlerMonitor
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.deep_crawling.filters import (
    FilterChain,
)
from markdownify import markdownify as md
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from database import PostsDatabase
from logger import get_logger

logger = get_logger(__name__)

async def fetch_news_links_with_playwright_async(
    url: str = "https://www.50hertz.com/de/Medien/",
    headless: bool = True,
    timeout_ms: int = 45000,
    wait_after_network_idle: float = 5.0,
) -> List[str]:
    """ Async: load the page with JS executed and extract absolute links containing 'News/Details/' (case-insensitive)."""
    found = set()
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=headless)
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    )
    page = await context.new_page()
    try:
        # Try waiting for true network idle; may timeout on slow CI
        await page.goto(url, wait_until="networkidle", timeout=timeout_ms)
    except PlaywrightTimeoutError as e:
        logger.error("PlaywrightTimeoutError raised for '{}' with {}".format(url, e))
        # Fallback: proceed even if networkidle didn’t happen
        pass

    # Give any late-rendered JS a moment
    await page.wait_for_timeout(int(wait_after_network_idle * 1000))

    # Best‐effort: wait for at least one relevant link
    try:
        await page.wait_for_selector('a[href*="News/Details"]', timeout=15000)
    except PlaywrightTimeoutError as e:
        logger.error("PlaywrightTimeoutError raised for '{}' with {}".format(url, e))
        pass

    # Scrape all anchors
    anchors = await page.query_selector_all("a[href]")
    for a in anchors:
        href = (await a.get_attribute("href") or "").strip()
        if not href or href.lower().startswith(("javascript:", "#", "mailto:")):
            continue
        if re.search(r"news/details", href, re.IGNORECASE):
            found.add(urljoin(url, href))

    # Fallback: regex scan of the fully rendered HTML
    content = await page.content()
    for m in re.findall(
        r"https?://[^\s\"'>]+/News/Details/[^\s\"'>]+", content, re.IGNORECASE
    ):
        found.add(m)

    # Clean up
    await context.close()
    await browser.close()
    await playwright.stop()

    return sorted(found)

def find_and_format_numeric_date(text:str)->str|None:
    """Extract date from markdown."""
    pattern = r"\b(\d{1,2})\.(\d{1,2})\.(\d{4})\b"
    match = re.search(pattern, text)

    if not match:
        return None

    day, month, year = match.groups()
    return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")

def is_challenge_page(markdown: str) -> bool:
    """Heuristic for detecting Cloudflare / human-verification interstitials."""
    if markdown is None:
        return True

    lowered = markdown.lower()
    return (
        "verifying you are human" in lowered
        or "ray id" in lowered and "cloudflare" in lowered
        or "please enable javascript" in lowered and "security check" in lowered
    )

async def scrape_page_with_crawl4ai(link: str) -> str|None:
    """Scrape page using Crawl4AI."""
    # Shared dispatcher and rate limiter (reuse for all crawls)
    rate_limiter = RateLimiter(
        base_delay=(20.0, 110.0),
        max_delay=400.0,
        max_retries=10,
    )
    dispatcher = MemoryAdaptiveDispatcher(
        memory_threshold_percent=85.0,
        check_interval=3.0,
        max_session_permit=1,
        monitor=CrawlerMonitor(),
        rate_limiter=rate_limiter,
    )

    # Base run config (can be mutated per attempt if needed)
    base_config = CrawlerRunConfig(
        deep_crawl_strategy=BFSDeepCrawlStrategy(
            max_depth=0,
            include_external=False,
            filter_chain=FilterChain([]),
        ),
        scraping_strategy=LXMLWebScrapingStrategy(),
        cache_mode=CacheMode.BYPASS,
        verbose=True,
        page_timeout=600_000,
    )

    plausable_user_agents = [
        # A few realistic modern user agents; rotate if challenged.
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    ]

    # retry logic
    attempt = 0
    max_attempts = 3
    last_markdown = None
    success = False

    # Create a crawler with initial BrowserConfig (will be reused unless we need to rotate UA)
    async with AsyncWebCrawler(
        config=BrowserConfig(
            user_agent=plausable_user_agents[0],
            headless=True,
            use_persistent_context=True,  # to keep cookies between requests if beneficial
        )
    ) as crawler:
        while attempt < max_attempts and not success:
            # Rotate user-agent on retry
            user_agent = random.choice(plausable_user_agents) if attempt > 0 else plausable_user_agents[0]
            if attempt > 0:
                # Reconfigure crawler's user-agent by rebuilding browser context.
                # Lightweight since it's only on failure.
                await crawler.close()  # close previous context
                crawler = AsyncWebCrawler(
                    config=BrowserConfig(
                        user_agent=user_agent,
                        headless=True,
                        use_persistent_context=True,
                    )
                )
                await crawler.__aenter__()  # re-enter context manually for retries

            # Small jitter to reduce fingerprinting
            await asyncio.sleep(1 + random.random() * 2)

            results = None
            try:
                config = copy.deepcopy(base_config)
                results = await crawler.arun(url=link, config=config, dispatcher=dispatcher)
            except Exception as e:
                logger.warning(f"Exception while crawling {link} on attempt {attempt + 1}: {e}")
                attempt += 1
                await asyncio.sleep(2**attempt)
                continue

            if results is None or not results:
                logger.warning(f"No crawl result for {link} on attempt {attempt + 1}")
                attempt += 1
                await asyncio.sleep(2**attempt)
                continue

            # accessing the article page (should be only one in the list)
            result = results[0]
            raw_md = result.markdown
            last_markdown = raw_md

            if is_challenge_page(raw_md):
                logger.warning(f"Detected challenge page for {link} on attempt {attempt + 1}; retrying with different UA/backoff. Returning raw markdown: {result.markdown}")
                attempt += 1
                await asyncio.sleep(2**attempt)
                continue  # retry

            # Extract date
            date = find_and_format_numeric_date(raw_md)
            if date is None:
                logger.warning(f"Could not locate date. Skipping: {link}")
                continue

            success = True

        # check if at the and the download was successfull
        if not success:
            logger.warning(f"Failed to scrape challenge {link} after {max_attempts} attempts. Last markdown snippet:\n{last_markdown}")
            await asyncio.sleep(5)

    if success:
        return raw_md
    else:
        return None

async def scrape_page_with_playwright(url: str, content_selector: str = ".n-grid.xsp-news-item", timeout: int = 30) -> str:
    """
    Scrape the JS-rendered content at `url`, extract the element matching `content_selector`,
    strip scripts/styles, and convert to Markdown.

    Args:
        url: The page URL to scrape.
        content_selector: CSS selector for the main article container.
        timeout: Seconds to wait for the element to appear.
        chrome_driver_path: Path to your chromedriver executable.

    Returns:
        A Markdown string of the article.
    """
    # 1. Launch headless Chrome
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome( options=options)

    try:
        # 2. Load page and wait for the article container
        driver.get(url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, content_selector)))

        # 3. Grab the outer HTML of that container
        elem = driver.find_element(By.CSS_SELECTOR, content_selector)
        raw_html = elem.get_attribute("outerHTML")
    finally:
        driver.quit()

    # 4. Clean with BeautifulSoup
    soup = BeautifulSoup(raw_html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()

    # 5. Convert to Markdown
    markdown = md(str(soup), heading_style="ATX")
    return markdown

async def scrape_50hz_news(root_url: str, table_name: str, database: PostsDatabase|None) -> None:
    """Scrape 50hz news pages."""
    links = await fetch_news_links_with_playwright_async(url=root_url)
    for link in links:
        logger.debug(f"Found: {link}")
    if len(links) == 0:
        raise Exception("No links found")

    new_articles = []
    for link in links:
        logger.info(f"Processing {link}")

        # check for post in the database before trying to pull it as it is long
        if database is not None and database.is_table(table_name=table_name) and database.is_post(table_name=table_name, post_id=database.create_post_id(post_url=link)):
            logger.info(f"Post already exists in the database. Skipping: {link}")
            continue

        # attempt scraping with crawl4ai
        raw_md = await scrape_page_with_crawl4ai(link=link)

        if raw_md is None:
            raw_md = await scrape_page_with_playwright(url=link)

        if raw_md is None:
            logger.error(f"All attempts to scrape the page failed. Url: {link}")
            continue

        # locate date
        date = find_and_format_numeric_date(raw_md)
        if date is None:
            logger.warning(f"Could not locate date in {link}\n{raw_md}")
            continue

        # add post to the database
        article_title = link.rstrip("/").split("/")[-1].replace("-", "_")
        if database is not None:
            database.add_post(
                table_name=table_name,
                published_on=date,
                title=article_title,
                post_url=link,
                post=raw_md,
            )

        new_articles.append(link)
        logger.info(f"Saved article {link} (size: {len(raw_md)} chars)")

        await asyncio.sleep(5)  # to avoid IP blocking
        gc.collect()  # clean memory

    logger.info(f"Saving {len(new_articles)} new posts out of {len(links)} total links")

def main_scrape_50hz_posts(db_path:str, table_name:str, out_dir:str, root_url:str|None=None):
    """Scrape 50hz news articles database."""
    if root_url is None:
        root_url = "https://www.50hertz.com/de/Medien/" # default path to latest news

    # --- initialize / connect to DB ---
    news_db = PostsDatabase(db_path=db_path)

    # create acer table if it does not exists
    news_db.check_create_table(table_name)

    # try to scrape articles and add them to the database
    try:
        # --- scrape & store ---
        asyncio.run(
            scrape_50hz_news(
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

    main_scrape_50hz_posts(
        db_path="../database/scraped_posts.db",
        root_url="https://www.50hertz.com/de/Medien/",
        table_name="50hz",
        out_dir="../output/posts_raw/50hz/",
    )