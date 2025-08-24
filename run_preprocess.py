import os
import sys

from logger import get_logger
from preprocessing.preprocess_raw_posts import Preprocessor

logger = get_logger(__name__)

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
    "  * [2024](https://www.smard.de/home/",
    "  * [2023](https://www.smard.de/home/",
    "  * [2022](https://www.smard.de/home/",
    "  * [2021](https://www.smard.de/home/",
    "  * [2020](https://www.smard.de/home/",
    "  * [2019](https://www.smard.de/home/",
    "  * [2018](https://www.smard.de/home/",
    "  * [2017](https://www.smard.de/home/",
    "  * [2016](https://www.smard.de/home/",
    "  * [2015](https://www.smard.de/home/",
    "  * [2014](https://www.smard.de/home/",
    "  * [2013](https://www.smard.de/home/",
    "  * [2012](https://www.smard.de/home/",
    "  * [2011](https://www.smard.de/home/",
    "  * [2010](https://www.smard.de/home/",
    "  * [2009](https://www.smard.de/home/",
    "  * [2008](https://www.smard.de/home/",
    "  * [2007](https://www.smard.de/home/",
    "  * [2006](https://www.smard.de/home/",
    "  * [2005](https://www.smard.de/home/",
    "  * [2004](https://www.smard.de/home/",
    "  * [2003](https://www.smard.de/home/",
    "  * [2002](https://www.smard.de/home/",
    "  * [2001](https://www.smard.de/home/",
    "  * [2000](https://www.smard.de/home/",
    "  * [2025](https://www.smard.de/home/",
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
    "Baden-Württemberg","Bayern","Berlin","Brandenburg","Bremen","Hamburg","Hessen","Mecklenburg-Vorpommern",
    "Niedersachsen","Nordrhein-Westfalen","Rheinland-Pfalz","Saarland","Sachsen","Sachsen-Anhalt","Schleswig-Holstein",
    "Thüringen","Anrainerstaaten","nicht zutreffend (z.B. Börse)","Anrainerstaaten",
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
    "  * 1", "  * 2", "  * 3", "  * 4",
]
black_list_blocks=[
    "Created with Highcharts",
    "Chart Created with Highstock",
]
black_list_starters_energy_wire = [
    "### ",
    "  * ",
    "[News](https://www.cleanenergywire.org/news",
    "[« previous news]",
    "[](https://www.facebook.com",
    "[](https://twitter.com/",
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

def main_preprocess(source:str):  # noqa: C901
    """Scrape the news source."""
    # Configuration for all sources
    SOURCE_CONFIG = {
        "entsoe": {
            "table_name": "entsoe",
            "preprocessor_config": {
                "start_markers": [
                    "Button",
                    "#  news ",
                ],
                "end_markers": [
                    "Share this article",
                    "Sign up for press updates"
                ],
                "max_lines": 30,
            },
            "out_dir": "./output/posts_cleaned/entsoe/",
        },
        "eex": {
            "table_name": "eex",
            "preprocessor_config": {
                "start_markers": ["# EEX Press Release -"],
                "start_marker_constructs": {"date": Preprocessor.date_to_yyyy_mm_dd},
                "end_markers": ["**CONTACT**", "**_Contacts:_**", "**Contact**", "**KONTAKT**"],
                "max_lines": 30,
            },
            "out_dir": "./output/posts_cleaned/eex/",
        },
        "acer": {
            "table_name": "acer",
            "preprocessor_config": {
                "start_marker_constructs": {"date": Preprocessor.date_to_dd_mm_yyyy},
                "start_markers": [],
                "end_markers": ["## ↓ Related News", "![acer]"],
                "custom_black_list_starters":["Share on: [Share]"],
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/acer/",
        },
        "ec": {
            "table_name": "ec",
            "preprocessor_config": {
                "start_markers": [
                    "  2. News",
                    "  * News blog",
                    "  * News announcement",
                    "  * News article",
                    "  * Statement",
                ],
                "end_markers": ["## Related links", "## **Related links**", "## Related Links", "## **Source list for the article data**", "Share this page ", "info(at)acer.europa.eu"],
                "max_lines": 30,
            },
            "out_dir": "./output/posts_cleaned/ec/",
        },
        "icis": {
            "table_name": "icis",
            "preprocessor_config": {
                "start_markers": [
                    "[Home](https://www.icis.com/explore)"
                ],
                "end_markers": [
                    "## Related news",
                ],
                "custom_black_list_starters":[
                    "[Full story](https://www.icis.com/explore/resources/news",
                    "[Related news](https://www.icis.com/explore/resources/news",
                    "[Related content](https://www.icis.com/explore/resources/news",
                    "[Contact us](https://www.icis.com/explor",
                    "[Try ICIS](https://www.icis.com/explore/contact",
                ],
                "black_list_single_word_lines":[
                    "Jump to",
                ],
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/icis/",
        },
        "bnetza": {
            "table_name": "bnetza",
            "preprocessor_config": {
                "start_markers": [
                    "[Pressemitteilungen](https://www.bundesnetzagentur.de/SharedDocs"
                ],
                "end_markers": [
                    "[](javascript:void\(0\);) **Inhalte teilen**"
                ],
                "skip_start_lines":1,
                "max_lines": 30,
            },
            "out_dir": "./output/posts_cleaned/bnetza/",
        },
        "smard": {
            "table_name": "smard",
            "preprocessor_config": {
                "start_markers": [],
                "end_markers": [],
                "custom_black_list_starters": black_list_line_starts,
                "black_list_single_word_lines": black_list_single_word_lines,
                "black_list_blocks": black_list_blocks,
                "prefer_german": True,
                "max_lines": 30,
            },
            "out_dir": "./output/posts_cleaned/smard/",
        },
        "agora": {
            "table_name": "agora",
            "preprocessor_config": {
                "start_markers": [
                    "  * Print"
                ],
                "end_markers": [
                    "##  Stay informed",
                    "## Impressions",
                    "##  Event details",
                    "##  Further reading"
                ],
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/agora/",
        },
        "energy_wire": {
            "table_name": "energy_wire",
            "preprocessor_config": {
                "start_markers": [
                    "Clean Energy Wire / Handelsblatt",
                    "Tagesspiegel / Clean Energy Wire ",
                    "# In brief ",
                    "[](javascript:window.print\(\))"
                ],
                "end_markers": [
                    "#### Further Reading",
                    "### Ask CLEW",
                ],
                "custom_black_list_starters": black_list_starters_energy_wire,
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/energy_wire/",
        },
        "transnetbw": {
            "table_name": "transnetbw",
            "preprocessor_config": {
                "start_markers": [
                    "Nach oben scrollen",
                ],
                "end_markers": [
                    "https://de.linkedin.com/company/transnetbw-gmbh"
                ],
                "custom_black_list_starters":[
                    "  * [Impressum]","  * [Datenschutz]","  * [Nutzungsbedingungen]","  * [AEB]","  * [Kontakt]","  * [Netiquette ]",
                    "![](https://www.transnetbw.de/_Resources",
                    "Andrea JungLeiterin Unternehmenskommunikationa",
                    "Kathrin EggerPressesprecherink",
                    "PDF",
                    "Clemens von WalzelTeamleiter",
                    "Matthias RuchserPressesprecherm",
                    "JPG5", "JPG1",
                    "  * [www.transnetbw.de/de/",
                    "  * [Starte Download von: ",
                    "[www.stromgedacht.de]",
                    "Copyright Bild: ",
                    "Pressemitteilung:",
                    "[www.transnetbw.de/de/newsroom",
                    "[www.powerlaendle.de]",
                    "[www.sonnen.de]",
                    "[www.transnetbw.de/de/netzentwicklung",
                    "![](https://www.transnetbw.de/",
                    "![](https://www.transnetbw.de/_Resources/",
                    "/ / / / / / / / ",
                    "<https://ip.ai/",
                ],
                "black_list_single_word_lines":[
                    "Mathias Bloch","Pressesprecher","m.bloch@sonnen.de","ZurückWeiter",
                ],
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/transnetbw/",
        },
        "tennet": {
            "table_name": "tennet",
            "preprocessor_config": {
                "start_markers": [
                    "Zuletzt aktualisiert",
                ],
                "end_markers": [
                    "## Downloads",
                    "Notwendige Cookies akzeptieren",
                ],
                "custom_black_list_starters":[
                    "[Cookies](https://www.tennet.eu/de/datenschutz)",
                ],
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/tennet/",
        },
        "50hz": {
            "table_name": "50hz",
            "preprocessor_config": {
                "start_markers": [
                    "Projektmeldung",
                    "Pressemitteilung",
                ],
                "end_markers": [
                    "Artikel teilen:",
                ],
                "custom_black_list_starters":[
                    "![](/DesktopModules/LotesNewsXSP",
                    "[Download der Pressemitteilung als PDF-Datei]",
                ],
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/50hz/",
        },
        "amprion": {
            "table_name": "amprion",
            "preprocessor_config": {
                "start_markers": [
                    "  2. [ ](https://www.amprion.net/Presse/Pressemitteilungen",
                ],
                "end_markers": [
                    "Seite teilen:"
                ],
                "custom_black_list_starters":[
                    "/Presse%C3%BCbersicht_aktuell.html)",
                    "  1. [ ](https://www.amprion.net/",
                    "  2. [ ](https://www.amprion.net/",
                    "  3. [ ](https://www.amprion.net/",
                    "  4. [ ](https://www.amprion.net/",
                    "  * [Presse](https://www.amprion.net",
                    "    * [ ](https://www.amprion.net",
                    "[](tel:+",
                ],
                "max_lines": 30
            },
            "out_dir": "./output/posts_cleaned/amprion/",
        },
    }

    source_db_path = "./database/scraped_posts.db"
    target_db_path = "./database/preprocessed_posts.db"

    if source == "all":
        targets = list(SOURCE_CONFIG.keys())
    else:
        if source not in SOURCE_CONFIG:
            raise ValueError(f"Unknown source '{source}'. Valid options: {', '.join(SOURCE_CONFIG.keys())} or 'all'.")
        targets = [source]

    for src in targets:
        config = SOURCE_CONFIG[src]
        preprocessor_config = config["preprocessor_config"]
        out_dir = config["out_dir"]
        os.makedirs(out_dir, exist_ok=True)  # ensure output directory exists

        preprocessor = Preprocessor(config=preprocessor_config)
        preprocessor(
            source_db_path=source_db_path,
            target_db_path=target_db_path,
            table_name=config["table_name"],
            out_dir=out_dir,
        )

        logger.info(f"Preprocessing {src} done.")

if __name__ == "__main__":

    print("launching run_scrape.py")   # noqa: T201

    if len(sys.argv) != 2:
        source = "all"
    else:
        source = str(sys.argv[1])

    main_preprocess(source=source)