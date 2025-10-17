# GTFS Analytics with Docker Spark

Process Swedish transit data using Docker-based Spark and HDFS.

## Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux)

## Setup

1. **Clone repository**
   ```bash
   git clone https://github.com/Adam1416k/ID2221_Project_Group20.git
   cd ID2221_Project_Group20
   ```

2. **Start Docker cluster**
   ```bash
   docker-compose up -d
   ```

3. **Verify services are running**
   ```bash
   docker ps
   ```
   Should show: spark-master, spark-worker, namenode, datanode1

## Upload Data to HDFS

Configure the data period in `src/docker_analytics.py` (default is October 1, 2025). Data is fetched automatically.

```python
month, day = "10", "01"
```

## Run Spark Analytics

```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/docker_analytics.py
```
## Plot Results
To plot results, you need to do so locally, outside of Docker.
1. Create a venv and install dependencies from `requirements.txt:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
2. Run the plotting script (adjust the path to your results file as needed):
```bash
python3 src/plot_results.py ../data/analytics_results/gtfs_summary_2025-10-01.json
```

## Access Web Interfaces

- **Spark Master**: http://localhost:8080
- **HDFS Namenode**: http://localhost:9870

## Troubleshooting

**Containers not starting**:
```bash
docker-compose down
docker-compose up -d
```

**No analytics results**:
- Check data exists in `./data/raw/2025-10/`
- Verify containers are running: `docker ps`
- Check logs: `docker-compose logs spark-master`

**Permission errors**: Make sure Docker has access to project folder.