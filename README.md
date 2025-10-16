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

**Option 1: Direct file copy (recommended)**
- Place GTFS files in `./data/raw/2025-10/` folder
- Files are automatically accessible in containers at `/data/`

**Option 2: Upload to HDFS manually**
```bash
# Copy files into namenode container
docker cp your-gtfs-file.txt namenode:/tmp/

# Upload to HDFS
docker exec namenode hdfs dfs -put /tmp/your-gtfs-file.txt /data/
```

## Run Spark Analytics

```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/docker_analytics.py
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