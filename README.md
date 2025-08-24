# ⚡ Energy Heartbeat  

_A personal project to track and summarize the pulse of the European energy industry._  

---

## 📌 Motivation  

After moving from academia into the energy industry, I quickly realized how fragmented and overwhelming it can be to stay up to date with both technical and business news. Unlike academia—where all relevant work is centralized—industry knowledge is spread across regulators, TSOs, exchanges, think tanks, and specialized media.  

**Energy Heartbeat** addresses this by:  
- Collecting news across key European (Germany-focused) sources  
- Cleaning and normalizing content into consistent text  
- Using NLP and LLMs to chunk, analyze, and summarize events  
- Building a **temporally-aware knowledge graph** to highlight how entities and relationships evolve over time  
- Delivering short, digestible updates and quantitative metrics through a web interface  

---

## 🏗️ Project Structure  

1. **Data Collection (Scraping)**  
   Scrapers (running via GitHub Actions) pull content daily from major European energy sources, including:  
   - Bundesnetzagentur (German regulator)  
   - SMARD (market data platform)  
   - ENTSO-E (European TSOs association)  
   - 50Hertz, TenneT, Amprion, TransnetBW (German TSOs)  
   - European Commission (DG Energy) & ACER  
   - Agora Energiewende (think tank)  
   - EEX (European Energy Exchange)  
   - ICIS (market analytics)  
   - Clean Energy Wire (journalism)  

   ⚠️ **Note:** All scraped data is stored privately (GitHub private repo). No raw text is exposed to avoid any legal issues.  

2. **Cleaning & Pre-processing**  
   - HTML and links removed with regex rules  
   - Dynamic pages handled with Playwright (e.g., 50Hertz, Bundesnetzagentur)  
   - German-language content automatically translated to English  

3. **Semantic Chunking**  
   - Each article is split into **meaningful chunks** using OpenAI embeddings  
   - Similarity-based chunking ensures sections correspond to ideas, not arbitrary lengths  
   - Chunking output forms the basis for knowledge graph extraction  

4. **Knowledge Graph (Temporal)**  
   - Following the [OpenAI Cookbook](https://cookbook.openai.com/examples/partners/temporal_agents_with_knowledge_graphs/temporal_agents_with_knowledge_graphs)  
   - Entities, relations, and events are extracted with **timestamps**  
   - Enables time-aware reasoning and tracking of evolving relationships  

5. **Summarization & Output**  
   - LLM-powered summaries of recent news (weekly focus)  
   - Numeric and textual insights (e.g., keyword frequency, coherence scores, new entities introduced)  
   - Final deliverable: **static webpage dashboard** with recent updates + key metrics  

---

## 🔧 Key Technologies  

- **Scraping & Automation**: [Crawl4AI](https://github.com/yourgithub/crawl4ai), Playwright, GitHub Actions  
- **Data Processing**: Python, SQL  
- **NLP & LLMs**: OpenAI Embeddings + GPT models  
- **Visualization**: Temporal knowledge graphs, keyword frequency charts, semantic coherence metrics  
- **Deployment**: Static site (frontend in progress)  

---

## 🚀 Current Status  

- ✅ Scrapers for 13+ sources live on GitHub Actions  
- ✅ Cleaned & translated SQL database of posts  
- ✅ Prototype semantic chunker (`Chunker.generate_transcripts_and_chunks`) implemented  
- 🔄 In progress: temporal knowledge graph pipeline & visualization dashboard  

---

## 📊 Planned Preliminary Results  

Initial analysis will focus on **ENTSO-E** as a single-source case study:  
- Distribution of semantic chunk sizes  
- Coherence and similarity across chunks  
- Temporal keyword frequency (e.g., “TYNDP”, “ERAA”, “market coupling”)  
- Evolution of entity/relationship coverage in the knowledge graph  

---

## 🌍 Vision  

Ultimately, **Energy Heartbeat** aims to be a personal “industry intelligence companion”:  
- **Bite-sized weekly summaries** of the German & European energy landscape  
- **Interactive knowledge graph** to explore evolving policies, projects, and companies  
- **Quantitative indicators** (topic frequencies, novelty detection, coherence metrics)  

---

## 📖 License  

This project is for **personal, educational, and non-commercial use only**.  
All rights to the original articles remain with their respective publishers.  

---