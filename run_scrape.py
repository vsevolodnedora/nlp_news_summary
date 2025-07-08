import sys

from scrapers.scrape_entsoe_posts import main_scrape_entsoe_posts
from scrapers.scrape_eex_posts import main_scrape_eex_posts
from scrapers.scrape_acer_posts import main_scrape_acer_posts
from scrapers.scrape_ec_posts import main_scrape_ec_posts

if __name__ == '__main__':

    print("launching run_scrape.py")

    if len(sys.argv) != 2:
        source = "entsoe"
    else:
        source = str(sys.argv[1])

    if source == "entsoe" or source == "all":
        main_scrape_entsoe_posts(
            output_dir_raw="./output/posts_raw/entsoe/",
            output_dir_cleaned="./output/posts_cleaned/entsoe/"
        )

    if source == "eex" or source == "all":
        main_scrape_eex_posts(
            output_dir_raw="./output/posts_raw/eex/",
            output_dir_cleaned="./output/posts_cleaned/eex/"
        )

    if source == "acer" or source == "all":
        main_scrape_acer_posts(
            output_dir_raw="./output/posts_raw/acer/",
            output_dir_cleaned="./output/posts_cleaned/acer/"
        )

    if source == "ec" or source == "all":
        main_scrape_ec_posts(
            output_dir_raw="./output/posts_raw/ec/",
            output_dir_cleaned="./output/posts_cleaned/ec/"
        )

    if not source in ["entsoe", "eex", "acer", "ec", "all"]:
        raise ValueError(f"invalid source={source}")