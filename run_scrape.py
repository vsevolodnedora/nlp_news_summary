import os
import sys

from scrapers.scrape_acer_posts import main_scrape_acer_posts
from scrapers.scrape_agora_posts import main_scrape_agora_posts
from scrapers.scrape_bnetza_posts import main_scrape_bnetza_posts
from scrapers.scrape_ec_posts import main_scrape_ec_posts
from scrapers.scrape_eex_posts import main_scrape_eex_posts
from scrapers.scrape_energy_wire_posts import main_scrape_energy_wire_posts
from scrapers.scrape_entsoe_posts import main_scrape_entsoe_posts
from scrapers.scrape_icis_posts import main_scrape_icis_posts
from scrapers.scrape_smard_posts import main_scrape_smard_posts
from scrapers.scrape_transnetbw_posts import main_scrape_transnetbw_posts
from scrapers.scrape_tennet_posts import main_scrape_tennet_posts
from scrapers.scrape_amprion_posts import main_scrape_amprion_posts
from scrapers.scrape_50hz_posts import main_scrape_50hz_posts

from logger import get_logger
logger = get_logger(__name__)

def main_scrape(source:str):  # noqa: C901
    """Scrape the news source."""
    # Configuration for all sources
    SOURCE_CONFIG = {
        "entsoe": {
            "root_url": "https://www.entsoe.eu/news-events/",
            "table_name": "entsoe",
            "scraper_func": main_scrape_entsoe_posts,
            "out_dir": "./output/posts_raw/entsoe/",
        },
        "eex": {
            "root_url": "https://www.eex.com/en/newsroom/",
            "table_name": "eex",
            "scraper_func": main_scrape_eex_posts,
            "out_dir": "./output/posts_raw/eex/",
        },
        "acer": {
            "root_url": "https://www.acer.europa.eu/news-and-events/news",
            "table_name": "acer",
            "scraper_func": main_scrape_acer_posts,
            "out_dir": "./output/posts_raw/acer/",
        },
        "ec": {
            "root_url": "https://energy.ec.europa.eu/news_en",
            "table_name": "ec",
            "scraper_func": main_scrape_ec_posts,
            "out_dir": "./output/posts_raw/ec/",
        },
        "icis": {
            "root_url": "https://www.icis.com/explore/resources/news/",
            "table_name": "icis",
            "scraper_func": main_scrape_icis_posts,
            "out_dir": "./output/posts_raw/icis/",
        },
        "bnetza": {
            "root_url": "https://www.bundesnetzagentur.de/DE/Allgemeines/Aktuelles/start.html",
            "table_name": "bnetza",
            "scraper_func": main_scrape_bnetza_posts,
            "out_dir": "./output/posts_raw/bnetza/",
        },
        "smard": {
            "root_url": "https://www.smard.de/home/energiemarkt-aktuell/energiemarkt-aktuell",
            "table_name": "smard",
            "scraper_func": main_scrape_smard_posts,
            "out_dir": "./output/posts_raw/smard/",
        },
        "agora": {
            "root_url": "https://www.agora-energiewende.org/news-events",
            "table_name": "agora",
            "scraper_func": main_scrape_agora_posts,
            "out_dir": "./output/posts_raw/agora/",
        },
        "energy_wire": {
            "root_url": "https://www.cleanenergywire.org/news/",
            "table_name": "energy_wire",
            "scraper_func": main_scrape_energy_wire_posts,
            "out_dir": "./output/posts_raw/energy_wire/",
        },
        "transnetbw": {
            "root_url": "https://www.transnetbw.de/de/newsroom/",
            "table_name": "transnetbw",
            "scraper_func": main_scrape_transnetbw_posts,
            "out_dir": "./output/posts_raw/transnetbw/",
        },
        "tennet": {
            "root_url": "https://www.tennet.eu/de/news-de",
            "table_name": "tennet",
            "scraper_func": main_scrape_tennet_posts,
            "out_dir": "./output/posts_raw/tennet/",
        },
        "50hz": {
            "root_url": "https://www.50hertz.com/de/Medien/",
            "table_name": "50hz",
            "scraper_func": main_scrape_50hz_posts,
            "out_dir": "./output/posts_raw/50hz/",
        },
        "amprion": {
            "root_url": "https://www.amprion.net/",
            "table_name": "amprion",
            "scraper_func": main_scrape_amprion_posts,
            "out_dir": "./output/posts_raw/amprion/",
        },
    }

    # # Flattened mapping if needed elsewhere
    # source_url_mapping = {k: v["root_url"] for k, v in SOURCE_CONFIG.items()}

    db_path = "./database/scraped_posts.db"

    if source == "all":
        targets = list(SOURCE_CONFIG.keys())
    else:
        if source not in SOURCE_CONFIG:
            raise ValueError(f"Unknown source '{source}'. Valid options: {', '.join(SOURCE_CONFIG.keys())} or 'all'.")
        targets = [source]

    for src in targets:
        config = SOURCE_CONFIG[src]
        scraper = config["scraper_func"]
        out_dir = config["out_dir"]
        os.makedirs(out_dir, exist_ok=True)  # ensure output directory exists

        # Call the appropriate scraper
        scraper(
            root_url=config["root_url"],
            table_name=config["table_name"],
            db_path=db_path,
            out_dir=out_dir,
        )

        logger.info(f"Scraping {src} done.")

if __name__ == "__main__":

    print("launching run_scrape.py")   # noqa: T201

    if len(sys.argv) != 2:
        source = "amprion"
    else:
        source = str(sys.argv[1])

    main_scrape(source=source)