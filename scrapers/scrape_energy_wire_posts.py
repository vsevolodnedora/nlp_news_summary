import asyncio
import time
import fnmatch
import re, os
from datetime import datetime
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import urllib.parse
import shutil  # for file copying/renaming


from crawl4ai import CrawlerRunConfig, AsyncWebCrawler, CacheMode, MemoryAdaptiveDispatcher
from crawl4ai.components.crawler_monitor import CrawlerMonitor
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
from crawl4ai import RateLimiter

from .utils_scrape import cut_article_text_from_raw_pages

from logger import get_logger
logger = get_logger(__name__)

async def fetch_articles(page_url: str):

    async with aiohttp.ClientSession() as session:
        async with session.get(page_url) as resp:
            resp.raise_for_status()
            html = await resp.text()

    soup = BeautifulSoup(html, 'lxml')

    articles = []
    # Loop through each teaser article
    for art in soup.select('article.m-node--list--teaser'):
        # Title & URL
        a = art.select_one('h3.m-node--list--teaser__title > a')
        if not a:
            continue
        href  = a['href']
        title = a.get_text(strip=True)
        url   = urllib.parse.urljoin(page_url, href)

        # Date
        date_tag = art.select_one('span.date-display-single')
        date_str = date_tag.get_text(strip=True) if date_tag else None

        articles.append({
            'url':   url,
            'title': title,
            'date':  date_str,
        })

    return articles

def extract_and_format_date(markdown: str) -> str | None:
    # Define regex pattern to match dates like "11 June 2024"
    date_pattern = r'\b(\d{1,2}) (January|February|March|April|May|June|July|August|September|October|November|December) (\d{4})\b'

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

async def scrape_energy_wire_news(root_url:str, output_dir:str, clean_output_dir:str) -> None:

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    articles = await fetch_articles(root_url)
    for article_ in articles:
        logger.debug(f"Found: {article_['date']} {article_['url']}")

    new_articles = []
    async with AsyncWebCrawler() as crawler:
        for article in articles:

            article_url = article["url"].split('?', 1)[0]
            # sometimes page returns invalid core URL
            article_url = "https://www.cleanenergywire.org/news/" + article_url.split('/')[-1]

            article_title = article_url.split('/')[-1]
            article_title = article_title.replace('-','_')

            dt = datetime.strptime(article["date"], "%d %b %Y - %H:%M")
            formatted_datetime = dt.strftime("%Y-%m-%d_%H-%M")

            filename = f"{formatted_datetime}__{article_title}.md"
            out_path = os.path.join(output_dir, filename)

            # check if file exists and if so, skip
            if os.path.isfile(out_path):
                logger.info(f"Skipping article that already pulled {article_url}")
                continue

            logger.info(f"Processing {article['date']} {article['url']}")

            rate_limiter = RateLimiter(
                base_delay = (10.0, 40.0),
                max_delay = 100.0,
                max_retries = 3,
                # rate_limit_codes: List[int] = None,
            )

            dispatcher = MemoryAdaptiveDispatcher(
                memory_threshold_percent=80.0,
                check_interval=1.0,
                max_session_permit=1,
                monitor=CrawlerMonitor(),
                rate_limiter=rate_limiter
            )

            config = CrawlerRunConfig(
                deep_crawl_strategy=BFSDeepCrawlStrategy(
                    max_depth=0,
                    include_external=False,
                    filter_chain=FilterChain([]),
                ),
                scraping_strategy=LXMLWebScrapingStrategy(),
                cache_mode=CacheMode.BYPASS,
                verbose=True,
                page_timeout=100_000
            )

            article_url = article['url']

            results = await crawler.arun(url=article_url, config=config, dispatcher=dispatcher)
            if not results:
                logger.warning(f"No crawl result for {article_url}")
                continue
            # Expect one result per URL

            result = results[0]
            raw_md = result.markdown.raw_markdown
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(raw_md)

            new_articles.append(article_url)
            logger.info(f"Saved article {article_url} (size: {len(raw_md)} chars)")

            await asyncio.sleep(10)

    logger.info(f"Finished saving {len(new_articles)} new articles out of {len(articles)} links")


def main_scrape_energy_wire_posts(output_dir_raw:str, output_dir_cleaned:str,root_url:str|None=None):
    if root_url is None:
        root_url = "https://www.cleanenergywire.org/news/" # default path to latest news
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_energy_wire_news(root_url=root_url, output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned))
    # Clean posts raw posts and save clean versions into new foler
    cut_article_text_from_raw_pages(
        RAW_DIR=output_dir_raw,
        CLEANED_DIR=output_dir_cleaned,
        start_markers = [
            "Clean Energy Wire / Handelsblatt",
            "Tagesspiegel / Clean Energy Wire ",
            "# In brief ",
            "[](javascript:window.print\(\))"
        ],
        end_markers = [
            "#### Further Reading",
            "### Ask CLEW",
        ],
        max_lines = 30,
        custom_black_list_starters=[
            "### ", "  * ", "[News](https://www.cleanenergywire.org/news",
            "[« previous news]","[](https://www.facebook.com","[](https://twitter.com/",
            "[](https://www.linkedin.com",
            "[Benjamin Wehrmann](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Electricity](https://www.cleanenergywire.org",
            "[Carolina Kyllmann](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Business & Jobs](https://www.cleanenergywire.org",
            "[Factsheet](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Kira Taylor](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Sören Amelang](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Julian Wettengel](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Ruby Russel](https://www.cleanenergywire.org/about-us-clew-team)",
            "All texts created by the Clean Energy Wire",
            "[Ferdinando Cotugno](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Dossier](https://www.cleanenergywire.org",
            "[Sam Morgan](https://www.cleanenergywire.org/about-us-clew-team)",
            "[![](https://www.cleanenergywire.org",
            "[Dave Keating](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Kerstine Appunn](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Edgar Meza](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Cars](https://www.cleanenergywire.org",
            "[Jack McGovan ](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Cost & Prices](https://www.cleanenergywire.org",
            "[Interview](https://www.cleanenergywire.org",
            "[Elections & Politics](https://www.cleanenergywire.org",
            "[Michael Phillis](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Renewables](https://www.cleanenergywire.org",
            "[Wind](https://www.cleanenergywire.org",
            "[Industry](https://www.cleanenergywire.org",
            "[Climate & CO2](https://www.cleanenergywire.org",
            "[Jennifer Collins](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Municipal heat planning](https://www.cleanenergywire.org",
            "[Heating](https://www.cleanenergywire.org",
            "[Franca Quecke](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Business](https://www.cleanenergywire.org",
            "[Technology](https://www.cleanenergywire.org",
            "[Emanuela Barbiroglio](https://www.cleanenergywire.org/about-us-clew-team)",
            "![](https://www.cleanenergywire.org/sites",
            "[Resources & Recycling](https://www.cleanenergywire.org",
            "[Construction](https://www.cleanenergywire.org",
            "[Gas](https://www.cleanenergywire.org",
            "[Security](https://www.cleanenergywire.org",
            "[Gas](https://www.cleanenergywire.org",
            "[Rudi Bressa](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Giorgia Colucci](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Ferdinando Cotugno](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Yasmin Appelhans](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Bennet Ribbeck](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Julian Wettengel](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Franca Quecke](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Julian Wettengel](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Transport](https://www.cleanenergywire.org",
            "[Adaptation](https://www.cleanenergywire.org",
            "[Gas](https://www.cleanenergywire.org/",
            "[Hydrogen](https://www.cleanenergywire.org/",
            "[Company climate claims](https://www.cleanenergywire.org/topics/Company+climate+claims)",
            "[Agriculture](https://www.cleanenergywire.org/topics/Agriculture)",
            "[Solar](https://www.cleanenergywire.org/topics/Solar)",
            "[Bennet Ribbeck](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Joey Grostern](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Mobility](https://www.cleanenergywire.org/topics/Mobility)",
            "[Cities](https://www.cleanenergywire.org/topics/Cities)",
            "[Gas](https://www.cleanenergywire.org/topics/Gas)",
            "[Security](https://www.cleanenergywire.org/topics/Security)",
            "[Ben Cooke](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Make a Donation](https://www.cleanenergywire.org/support-us)",
            "[Grid](https://www.cleanenergywire.org/topics/Grid)",
            "[Storage](https://www.cleanenergywire.org/topics/Storage)",
            "[Solar](https://www.cleanenergywire.org/topics/Solar)",
            "[Business & Jobs](https://www.cleanenergywire.org/topics/Business+%26+Jobs)",
            "[Transport](https://www.cleanenergywire.org/topics/Transport)",
            "[Business & Jobs](https://www.cleanenergywire.org/topics/Business+%26+Jobs)",
            "[Factsheet](https://www.cleanenergywire.org/factsheets/",
            "[Policy](https://www.cleanenergywire.org/topics/Policy)",
            "[Elections & Politics](https://www.cleanenergywire.org/topics/Elections+%26+Politics)",
            "[Carbon removal](https://www.cleanenergywire.org/topics/Carbon+removal)",
            "[Industry](https://www.cleanenergywire.org/topics/Industry)",
            "[EU](https://www.cleanenergywire.org/topics/EU)",
            "[Security](https://www.cleanenergywire.org/topics/Security)",
            "[Milou Dirkx](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Rachel Waldholz](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Benjamin Wehrmann](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Julian Wettengel](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Camille Lafrance ](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Juliette Portala](https://www.cleanenergywire.org/about-us-clew-team)",
            "If you enjoyed reading this article, please consider",
            "#### Support our work",
            "[Efficiency](https://www.cleanenergywire.org/topics/Efficiency)",
            "[Heating](https://www.cleanenergywire.org/topics/Heating)",
            "[Society](https://www.cleanenergywire.org/topics/Society)",
            "[Isabel Sutton](https://www.cleanenergywire.org/about-us-clew-team)",
            "[Society](https://www.cleanenergywire.org/topics/Society)",
            "[International](https://www.cleanenergywire.org/topics/International)",
        ]
    )



# Execute the tutorial when run directly
if __name__ == "__main__":

    main_scrape_energy_wire_posts(
        output_dir_raw="../output/posts_raw/energy_wire/",
        output_dir_cleaned="../output/posts_cleaned/energy_wire/",
        root_url=f"https://www.cleanenergywire.org/news/",
        # root_url="https://energy.ec.europa.eu/news_en?page=10",
    )

    # --- Historic backfill ---
    # for i in range(24):
    #     main_scrape_energy_wire_posts(
    #         output_dir_raw="../output/posts_raw/energy_wire/",
    #         output_dir_cleaned="../output/posts_cleaned/energy_wire/",
    #         root_url=f"https://www.cleanenergywire.org/news?page={i+1}",
    #         # root_url="https://energy.ec.europa.eu/news_en?page=10",
    #     )