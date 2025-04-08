# Hackathon

⚖️ Kenya Law AI-Powered Legal Case Summarization

This project leverages data engineering, natural language processing, and cloud-based AI tools to automate the extraction and summarization of legal rulings from the Kenya Law website. Using OpenAI GPT-4, Azure Data Engineering tools, and PostgreSQL, this solution makes judicial case records more accessible and digestible for legal researchers, analysts, and the general public.

🔍 Overview The goal of the project is to build an end-to-end data pipeline that scrapes legal case metadata and ruling content from Kenya Law, transforms it, and uses GPT-4 to generate intelligent summaries. The output is stored in Microsoft Fabric Delta Tables and visualized via SQL Views for easy access.

The project uses:

🧹 Web scraping (BeautifulSoup in PySpark)

🛢 Azure PostgreSQL as a staging DB

🧬 Microsoft Fabric for scalable data processing & orchestration

🤖 OpenAI GPT-4 for summarization

📊 Delta Tables and Lakehouse Views for insights and exploration

🏗 Architecture & Pipeline Here’s a high-level breakdown of the pipeline architecture and process:

1️⃣ Web Scraping & Staging A Fabric Notebook runs a PySpark script that scrapes Kenya Law’s rulings page.

It dynamically extracts metadata and the ruling content from uploaded PDFs.

Extracted data is loaded into an Azure PostgreSQL staging database.

The table schema is dynamic — it adjusts to different structures per link scraped.

2️⃣ Ingestion into Microsoft Fabric A Copy Activity pipeline in Fabric ingests data from PostgreSQL to Delta Tables in the Lakehouse.

This ingestion step is selective: it captures only required metadata and the "Ruling" content column.

3️⃣ GPT-4 Summarization A dedicated Fabric Notebook environment is created with all required library modules.

A secure connection to OpenAI's GPT-4 API is established.

Functions are created to:

Connect to the Lakehouse Delta Tables.

Retrieve Case Number and Ruling Summary.

Generate concise summaries via GPT-4.

Append results back into the Delta Table.

4️⃣ Handling Token Limits A second orchestration pipeline is used to iterate through summarization scripts, especially for entries not processed on the first run due to OpenAI token rate limits.

5️⃣ Visualization & SQL Views A SQL endpoint view in the Fabric Lakehouse is created to query and gain insights from the generated summaries.

This can be integrated with visualization tools like Power BI for further analysis.

📦 Technologies Used Microsoft Fabric (Notebooks, Pipelines, Delta Lake, Lakehouse SQL)

Azure Database for PostgreSQL

Python (with BeautifulSoup, PySpark)

OpenAI GPT-4

GitHub for version control and collaboration

📈 Use Cases Legal case research & comparison

Automated summarization for case tracking tools

Enhancing access to justice through simplified legal data
