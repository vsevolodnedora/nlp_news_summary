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

from utils_scrape import cut_article_text_from_raw_pages

from logger import get_logger
logger = get_logger(__name__)

async def scrape_smard_news(root_url:str, output_dir:str, clean_output_dir:str) -> None:

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(clean_output_dir, exist_ok=True)

    def url_to_filename(url: str, markdown: str) -> str:
        # Try to match "YYYY.MM.DD" format first
        full_date_match = re.search(r'\b(\d{2})\.(\d{2})\.(\d{4})\b', markdown)

        date_part = ""
        if full_date_match:
            day, month, year = full_date_match.groups()
            date_part = f"{year}-{month}-{day}"
        else:
            # Try to match "DD Month YYYY" format
            date_match = re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', markdown)
            if date_match:
                try:
                    parsed_date = datetime.strptime(
                        f"{date_match.group(1)} {date_match.group(2)} {date_match.group(3)}", "%d %B %Y"
                    )
                    date_part = parsed_date.strftime("%Y-%m-%d")
                except ValueError:
                    logger.error(f"Invalid date format in markdown for URL: {url}")
            else:
                logger.error(f"Date not found in markdown for URL: {url}")

        # Extract the last segment of the URL for the title part
        title_part = url.split("/")[-1].replace("-", "_")

        # Combine date and title for the filename
        filename = f"{date_part}__{title_part}.md"
        return filename

    async with AsyncWebCrawler() as crawler:

        # Create a filter that only allows URLs with 'guide' in them
        # Create one filter for each required pattern
        url_filter_news = URLPatternFilter(patterns=["*smard*"])
        # url_filter_2025 = URLPatternFilter(patterns=["*home*"])
        # url_filter_en = URLPatternFilter(patterns=["*page*"])

        # Chain them so all must pass (AND logic)
        filter_chain = FilterChain([
            url_filter_news,
            # url_filter_en,
            # url_filter_2025,
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
            if fnmatch.fnmatch(result.url, '*topic-article*'):

                fname = url_to_filename(url=result.url, markdown=result.markdown)
                fpath = f"{output_dir}{fname}"
                fpath_exiected = f"{clean_output_dir}{fname}"
                if not os.path.exists(fpath_exiected):
                    res = result.markdown.raw_markdown
                    new_articles.append(result.url)
                    logger.debug(f"Saving article {result.url} | Length: {len(result.markdown.raw_markdown)} chars ")
                    os.makedirs(os.path.dirname(fpath), exist_ok=True)
                    with open(fpath, "w", encoding="utf-8") as f:
                        f.write(res)
                else:
                    logger.debug(f"Article already processed: {fpath}")
        logger.info(f"Finished saving {len(new_articles)} new articles out of {len(results)} articles")

def main_scrape_smard_posts(output_dir_raw:str, output_dir_cleaned:str,root_url:str|None=None):
    if root_url is None:
        root_url = "https://energy.ec.europa.eu/news_en" # default path to latest news
    # scrape news posts from ENTSO-E into a folder with raw posts
    asyncio.run(scrape_smard_news(root_url=root_url, output_dir=output_dir_raw, clean_output_dir=output_dir_cleaned))
    # Clean posts raw posts and save clean versions into new foler
    clean_smard_posts(
        RAW_DIR=output_dir_raw,
        CLEANED_DIR=output_dir_cleaned,
    )
    # cut_article_text_from_raw_pages(
    #     RAW_DIR=output_dir_raw,
    #     CLEANED_DIR=output_dir_cleaned,
    #     start_markers = [
    #         "## Energiemarkt aktuell"
    #     ],
    #     end_markers = [
    #         "## Schlagwörteliste"
    #     ]
    # )

black_list_line_starts = [
    "Suchbegriff eingeben Bitte",
    "[Direkt zum Inhalt springen.]",
    "![Logo der Bundesnetzagentur]",
    "[ ![Logo der Bundesnetzagentur]",
    "### Suchformular",
    "[ ![Strommarktdaten Logo]",
    "## Menü",
    "[ Menü Menu ]",
    "  * [Startseite]",
    "  * [Bundesnetzagentur.de]",
    "  * [Datennutzung]",
    "  * [Benutzerhandbuch]",
    "  * [ Informationen in Gebärdensprache ]",
    "  * [ Informationen in leicht verständlicher Sprache ]",
    "  * [ Login ]",
    "  * Link kopieren",
    "  * [ RSS-Feed ]",
    "  * [ English  ]",
    "  * [Energiemarkt aktuell]",
    "  * [Energiedaten kompakt]",
    "  * [Marktdaten visualisieren]",
    "  * [Deutschland im Überblick]",
    "  * [Energiemarkt erklärt]",
    "  * [Daten herunterladen]",
    "Hinweis: Diese Webseite",
    "Feedbackformular schließen",
    "  * [Strom]",
    "  * [Gas]",
    "## Strom",
    "# Energieträgerscharfe",
    "  *     * [ Drucken ]",
    "    * Teilen auf Twitter",
    "    * Teilen auf Facebook",
    "    * Teilen auf Xing",
    "    * Teilen auf Linkedin",
    "    * Teilen auf Whatsapp",
    "    * Artikel zu Favoriten hinzufügen",
    "    * Über E-Mail teilen",
    "  * Feedback",
    "Importe je Energieträger und Land",
    "Tabelle anzeigen",
    "Diagramm anzeigen",
    "  * ### Grafik exportieren",
    "    * PDF",
    "    * SVG",
    "    * PNG",
    "    * JPEG",
    "  * ### Tabelle exportieren",
    "    * CSV",
    "    * XLS",
    "## Schlagwörteliste",
    "  * [Außenhandel]",
    "[Link](https://www.smard.de",
    "© Bundesnetzagentur 2025",
    "  * [Tickerhistorie]",
    "  * [Datenschutzerklärung]",
    "  * [Impressum]",
    "  * [Über SMARD]",
    "  * Wir verwenden optionale Cookies,",
    "Alle Cookies zulassen",
    "Feedbackformular schließen",
    "# Feedback mitteilen",
    "Weitere Informationen zur Berechnungsmethode",
    "_____________________________________",
    "Die Adresse dieser Seite wird beim Absenden übermittelt.",
    "Pflichtfelder sind mit einem",
    "Die Übermittlung ist fehlgeschlagen",
    "  * [--- accessibility.error.message ---]",
    "Name |  ",
    "---|---",
    "Thema |",
    "E-Mail |",
    "Text* |",
    "Phone |",
    "[--- notification.close ---]",
    "[--- dialog.name.close ---]",
    "# Namen eingeben",
    "Geben Sie der von Ihnen getroffenen",
    "Default-Daten Live-Daten",
    "# Link kopieren",

    "  * [Alle Artikel]",
    "  * [2025]",
    "  * [2024]",
    "  * [2023]",
    "  * [2022]",
    "  * [2021]",
    "  * [2020]",
    "  * [2019]",
    "  * [2018]",
    "  * [2017]",
    "  * Stromerzeugung",
    "  * Stromverbrauch",
    "  * Markt",
    "  * Systemstabilität",
    "  * Realisierte Erzeugung",
    "  * Prognostizierte Erzeugung Day-Ahead",
    "  * Prognostizierte Erzeugung Intraday",
    "  * Installierte Erzeugungsleistung",
    "# Marktdaten visualisieren",
    "aktualisierte Daten verfügbar",
    "  * [Rekordwerte]",
    "  * [Verbindungsleitungen]",
    "[Marktdaten visualisieren]",
    "![](https://www.smard.de/resource",
    "  * [Kraftwerksabschaltung]",
    "  * [Marktdesign]",
    "  * [Großhandelsstrompreis]",
    "  * [Erneuerbare Energien]",
    "#  Lesen Sie auch",
    "  * [ Der Strommarkt im",
    "  * [Netzstabilität]",
    "  * [Netzengpassmanagement]",
    "  * [ Netzengpassmanagement",
    "  * [ Energiemarkt aktuell",
    "  * [Sturmtief]",
    "  * [Systemstabilität]",
    "  * [ Der Stromhandel im",
    "  * [ Die Stromerzeugung im",
    "  * [ Verbraucherkennzahlen",
    "  * [ Stromerzeugung und Stromhandel",
    "  * [Engpassbewirtschaftung]",
    "  * [Nettoimport]",
    "  * [ Status quo zur",
    "  * [ Monitoringbericht",
    "  * [auffällige ",
    "![Stromverbrauch bei Nacht]",
    "![Solarpanel und Windkraftanlagen im Sommer.]",
    "![Electricity trade prices]",
    "![Ein Umspannwerk zur Verteilung des gehandelten Stroms]",
    "  * [Nettoexport]",
    "> Quelle: smard.de  ",
    "Die Bausteine konnten nicht hinzugefügt",
    "  * Erdgas- und EU CO2-Zertifikatspreise",
    "  * Erzeugung sonstiger Energieträger",
    "  * Kommerzieller Außenhandel - Großhandelspreise",
    "  * Kommerzieller Außenhandel Belgien",
    "  * Kommerzieller Außenhandel Norwegen",
    "  * Prognostizierte Erzeugung",
    "  * Realisierte Stromerzeugung",
    "  * Realisierte Werte Erzeugung und Verbrauch",
    "  * Steinkohlekraftwerk Mehrum",
    "  * Test Admin Gui",
    "  * Wasserkraft",
    "  * [Corona]",
    "  * [Jahresauswertung]",
    "  * [ Rückblick Gasversorgung",
    "[Im folgenden Abschnitt",
    "![Strommarkt im Wandel]",
    "![Der Stromhandel im ",
    "![Auch bei Höchtpreisen",
    "  * [Erzeugung]",
    "  * [Verbrauch]",
    "  * [ Der Kohleausstieg ",
    "![Stromerzeugung ",
    "  * [Stromerzeugung]",
    "  * [Strompreis]",
    "  * [Stromübertragung]",
    "  * [Netz]",
    "  * [Netzausbau]",
    "  * [Gaspreis]"
]

black_list_single_word_lines = [
    "Deutschland/Luxemburg","Dänemark 1","Dänemark 2","Frankreich","Niederlande","Österreich","Polen",
    "Schweden 4","Schweiz","Tschechien","DE/AT/LU","Italien (Nord)","Slowenien","Ungarn",
    "Biomasse","Wasserkraft","Wind Offshore","Wind Onshore","Photovoltaik",
    "Sonstige Erneuerbare","Kernenergie","Braunkohle","Steinkohle","Erdgas",
    "Pumpspeicher","Sonstige Konventionelle",
    "Stromverbrauch - Realisierter Stromverbrauch","Netzlast",
    "Niederlande (Export)","Niederlande (Import)","Schweiz (Export)","Schweiz (Import)",
    "Tschechien (Export)","Tschechien (Import)","Österreich (Export)","Österreich (Import)",
    "Dänemark (Export)","Dänemark (Import)","Frankreich (Export)","Frankreich (Import)",
    "Nettoexport","Luxemburg (Export)","Luxemburg (Import)","Schweden (Export)","Schweden (Import)",
    "Polen (Export)","Polen (Import)","Belgien (Export)","Belgien (Import)","Deutschland/Luxemburg (Großhandelspreis)",
    "Norwegen (Export)","Norwegen (Import)","Belgien (Export)","Belgien (Import)",
    "Belgien (Großhandelspreis)",
    "diese Artikel",
    "URL:",
    "Nach oben",
    "Auflösung ändern",
    "Auflösung ändernAbbrechen",
    "Importe je Energieträger",
    "Mehr",
    "Mehr ",
    "Annehmen ",
    "Es trat ein Fehler bei der Erstellung der Exportdatei auf.",
    "  * 1",
    "  * 2",
]

def clean_smard_posts(RAW_DIR: str, CLEANED_DIR: str):
    """
    Loop through markdown files in RAW_DIR, extract content
    and save to CLEANED_DIR with the same filename if not already present.
    """
    # Ensure the cleaned directory exists
    os.makedirs(CLEANED_DIR, exist_ok=True)

    if not os.path.isdir(RAW_DIR):
        raise ValueError(f"RAW_DIR {RAW_DIR} does not exist.")

    # Step 1: Group files by date
    files_by_date = defaultdict(list)
    for filename in os.listdir(RAW_DIR):
        # Process only markdown files starting with a date
        if not filename.endswith('.md') or not filename[:10].count('-') == 2:
            logger.info(f"Skipping cleaning {filename}")
            continue
        # get date and file
        match = re.match(r"(\d{4}-\d{2}-\d{2})__(.+)\.md", filename)
        if match:
            date = match.group(1)
            files_by_date[date].append(filename)
    logger.info(f"Found {len(files_by_date)} files in {RAW_DIR}")

    # Step 2: Detect languages and select preferred file per date
    for ifile, (date, files) in enumerate(sorted(files_by_date.items())):
        lang_map = {}

        for file in files:
            filepath = os.path.join(RAW_DIR, file)
            with open(filepath, 'r', encoding='utf-8') as f:
                text = f.read()

            lang, _ = langid.classify(text)
            lang_map[lang] = file

        # Check for both German and English files on the same date
        if 'de' in lang_map and 'en' in lang_map:
            logger.debug(f"Files with same date {date} in two languages: {lang_map['de']} and {lang_map['en']}")
        if not 'de' in lang_map and 'en' in lang_map:
            logger.info(f"Only english article found {lang_map['en']}. Skipping...")
            continue


        # Prefer German if it exists
        filename = lang_map.get('de') or lang_map.get('en')

        logger.info(f"Processing {filename}...")
        raw_path = os.path.join(RAW_DIR, filename)
        cleaned_path = os.path.join(CLEANED_DIR, filename)

        # Read raw content
        with open(raw_path, 'r', encoding='utf-8') as f:
            text = f.read()

        # Remove block of text containing "Created with Highcharts"
        # Find blocks of text between empty lines
        blocks = re.split(r'(\n\s*\n)', text)  # split while preserving newlines
        cleaned_blocks = []

        for block in blocks:
            if "Created with Highcharts" in block or "Chart Created with Highstock" in block:
                logger.info("removing chart")
                continue
            cleaned_blocks.append(block)

        cleaned_text = ''.join(cleaned_blocks)

        cleaned_lines = []
        for line in cleaned_text.split('\n'):
            # Keep lines that do NOT start with any blacklisted element
            if any(line.startswith(element) for element in black_list_line_starts):
                continue
            if any(line == element for element in black_list_single_word_lines):
                continue
            # append line
            cleaned_lines.append(line)

        cleaned_text = '\n'.join(cleaned_lines)

        with open(cleaned_path, 'w', encoding='utf-8') as f:
            f.write(cleaned_text)

        logger.info(f"Cleaned and saved: {filename}")


# Execute the tutorial when run directly
if __name__ == "__main__":
    main_scrape_smard_posts(
        output_dir_raw="../output/posts_raw/smard/",
        output_dir_cleaned="../output/posts_cleaned/smard/",
        root_url="https://www.smard.de/home/energiemarkt-aktuell/energiemarkt-aktuell",
    )