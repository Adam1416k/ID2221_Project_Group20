# ID2221_Project_Group20



# Project Setup
## Installation
- pip install -r requirements.txt

## Environment Variables
Create a .env file (not committed to git) based on .env.example

Open .env and add your Trafiklab API key:
- TRAFIKLAB_API_KEY= ...




# Running the Pipeline
## Step 1 – Ingest Data
Downloads and extracts GTFS Sverige 2 data, then uploads it to HDFS:
- python -m src.ingest_gtfs

What happens:
- Fetches monthly dataset from Trafiklab
- Extracts ZIP to data/raw/<year-month>/
- Uploads to HDFS under /data/gtfs_sverige/<year-month>/gtfs

## Step 2 – Process Data
Cleans and normalizes the raw CSVs into Parquet tables for analytics:
- spark-submit src/process_gtfs.py

Outputs written to:
- hdfs://namenode:8020/data/gtfs_sverige/<year-month>/curated/



# NEXT STEPS AFTER PROCESSING:
- Run aggregations – compute busiest and least busy stops, average trip times, etc.

- Create visualizations – use Matplotlib or a notebook to plot top 10/lowest 10 stops.

- Automate – add a Docker Compose setup or daily cron job to re-ingest new monthly data.

- Extend – compare regions, detect anomalies, or integrate with streaming data (Kafka).