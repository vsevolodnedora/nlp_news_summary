import sys

from scrapers.scrape_entsoe_posts import main_scrape_entsoe_posts

if __name__ == '__main__':

    print("launching run_scrape.py")

    if len(sys.argv) != 2:
        source = "entsoe"
    else:
        source = str(sys.argv[1])

    if source == "entsoe":
        main_scrape_entsoe_posts(
            output_dir_raw="./output/posts_raw/entsoe/",
            output_dir_cleaned="./output/posts_cleaned/entsoe/"
        )
    else:
        raise ValueError(f"invalid source={source}")