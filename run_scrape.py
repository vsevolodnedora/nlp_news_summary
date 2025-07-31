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


def main_scrape(source:str):  # noqa: C901
    """Scrape posts from various sources."""
    if source == "entsoe" or source == "all":
        main_scrape_entsoe_posts(
            root_url="https://www.entsoe.eu/news-events/",
            table_name="entsoe",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/entsoe/",
        )

    if source == "eex" or source == "all":
        main_scrape_eex_posts(
            root_url="https://www.eex.com/en/newsroom/",
            table_name="eex",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/eex/",
        )

    if source == "acer" or source == "all":
        main_scrape_acer_posts(
            root_url="https://www.acer.europa.eu/news-and-events/news",
            table_name="acer",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/acer/",
        )

    if source == "ec" or source == "all":
        main_scrape_ec_posts(
            root_url="https://energy.ec.europa.eu/news_en",
            table_name="ec",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/ec/",
        )

    if source == "icis" or source == "all":
        main_scrape_icis_posts(
            root_url="https://www.icis.com/explore/resources/news/",
            table_name="icis",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/icis/",
        )

    if source == "bnetza" or source == "all":
        main_scrape_bnetza_posts(
            root_url="https://www.bundesnetzagentur.de/DE/Allgemeines/Aktuelles/start.html",
            table_name="bnetza",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/bnetza/",
        )

    if source == "smard" or source == "all":
        main_scrape_smard_posts(
            root_url="https://www.smard.de/home/energiemarkt-aktuell/energiemarkt-aktuell",
            table_name="smard",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/smard/",
        )

    if source == "agora" or source == "all":
        main_scrape_agora_posts(
            root_url="https://www.agora-energiewende.org/news-events",
            table_name="agora",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/agora/",
        )

    if source == "energy_wire" or source == "all":
        main_scrape_energy_wire_posts(
            root_url="https://www.cleanenergywire.org/news/",
            table_name="energy_wire",
            db_path="./database/scraped_posts.db",
            out_dir="./output/posts_raw/energy_wire/",
        )

    if source not in ["entsoe", "eex", "acer", "ec", "icis", "bnetza", "smard", "agora", "energy_wire", "all"]:
        raise ValueError(f"invalid source={source}")

if __name__ == "__main__":

    print("launching run_scrape.py")   # noqa: T201

    if len(sys.argv) != 2:
        source = "all"
    else:
        source = str(sys.argv[1])

    main_scrape(source=source)