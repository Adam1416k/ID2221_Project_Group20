# ID2221 GTFS Analytics Project - Swedish Transit Data Analysis

A comprehensive data pipeline for analyzing Swedish public transit data using Hadoop, HDFS, and PySpark. This project processes over **7.4 million transit events** from Swedish GTFS data to generate insights about stop frequency, route performance, and temporal patterns.

## 🚀 Quick Start

### Prerequisites
- **Docker Desktop** - [Download here](https://www.docker.com/products/docker-desktop/)
- **Python 3.9+** - [Download here](https://www.python.org/downloads/)
- **Git** - [Download here](https://git-scm.com/download/win)

### 5-Minute Setup
```bash
# 1. Clone the repository
git clone https://github.com/Adam1416k/ID2221_Project_Group20.git
cd ID2221_Project_Group20

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start Hadoop cluster (Docker-based)
docker-compose up -d

# 4. Wait 2-3 minutes, then upload data to HDFS
python src\upload_manual_data.py data\raw\2025-09\gtfs

# 5. Run analytics
python src\local_gtfs_analytics.py data\raw\2025-09\gtfs analytics_results
```

**🎉 Success!** You now have analytics results for 47,540+ Swedish transit stops.

## 📊 What You'll Get

### Data Scale
- **47,540 transit stops** across Sweden
- **7,435,943 stop events** processed
- **354,959 scheduled trips** analyzed
- **8,386 different routes** covered

### Generated Analytics
- `analytics_results/busiest_stops/` - Top 20 busiest transit stops
- `analytics_results/least_busy_stops/` - Underutilized stops
- `analytics_results/route_stats/` - Complete route performance data
- `analytics_results/hourly_patterns/` - 24-hour trip distribution

### Key Insights
- **Peak hours:** 4-5 PM (500K+ trips)
- **Busiest routes:** Gothenburg trams (Routes 6, 11, 5)
- **Geographic coverage:** Major Swedish cities (Stockholm, Gothenburg, Malmö)

## 🏗️ Architecture

### Hadoop Setup (Docker-based)
```yaml
# docker-compose.yml
services:
  namenode:    # HDFS Namenode (Web UI: localhost:9870)
  datanode:    # HDFS Datanode (Storage)
```

### Project Structure
```
ID2221_Project_Group20/
├── docker-compose.yml          # Hadoop cluster configuration
├── requirements.txt            # Python dependencies
├── data/raw/2025-09/gtfs/     # Sample GTFS data (392MB)
├── src/
│   ├── docker_hdfs_utils.py   # Docker-based HDFS utilities
│   ├── upload_manual_data.py  # Upload GTFS data to HDFS
│   ├── local_gtfs_analytics.py # Local file analytics (recommended)
│   ├── gtfs_analytics.py      # HDFS-based analytics
│   └── hdfs_utils.py          # Traditional HDFS utilities
└── analytics_results/         # Generated analytics output
```

## 📝 Detailed Setup Guide

### Option 1: Docker Setup (Recommended)

#### Step 1: Install Prerequisites
- **Docker Desktop for Windows** - Ensure it's running (whale icon in system tray)
- **Python 3.9+** - Verify with `python --version`
- **Git** - For cloning the repository

#### Step 2: Clone and Setup
```bash
git clone https://github.com/Adam1416k/ID2221_Project_Group20.git
cd ID2221_Project_Group20
pip install -r requirements.txt
```

#### Step 3: Start Hadoop Cluster
```bash
# Start containers in background
docker-compose up -d

# Check status (wait 2-3 minutes for startup)
docker ps
```

**Expected output:**
```
CONTAINER ID   IMAGE                                             STATUS
e89676365f14   bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8   Up X minutes (healthy)
0e28d5fd650e   bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8   Up X minutes (healthy)
```

#### Step 4: Verify Setup
- **HDFS Web UI:** http://localhost:9870
- **File Browser:** http://localhost:9870/explorer.html
- **Test connectivity:** `python src\docker_hdfs_utils.py status`

### Option 2: Native Windows Installation (Advanced)

If you prefer native Hadoop installation:

#### Prerequisites
- **Java 8 or 11** - Set JAVA_HOME environment variable
- **Hadoop 3.3.4** - Download from Apache Hadoop
- **Windows utils** - Download winutils.exe and hadoop.dll

#### Quick Script Setup
```bash
# Run the automated setup script
setup_hadoop.bat
```

This script:
1. Creates `C:\hadoop` directory
2. Downloads Hadoop 3.3.4 (manual step required)
3. Sets up Windows binaries (winutils.exe)
4. Configures environment variables

## 🔧 Available Commands

### HDFS Management
```bash
# Docker-based HDFS commands
python src\docker_hdfs_utils.py status          # Check HDFS status
python src\docker_hdfs_utils.py list /data      # List HDFS contents
python src\docker_hdfs_utils.py setup           # Create directory structure

# Traditional HDFS commands (if native installation)
python src\hdfs_utils.py status                 # Check connectivity
python src\hdfs_utils.py list                   # List available datasets
python src\hdfs_utils.py inspect <dataset>      # Inspect dataset
python src\hdfs_utils.py validate <dataset>     # Validate GTFS files
python src\hdfs_utils.py create-dirs            # Create directories
python src\hdfs_utils.py clean <dataset>        # Delete dataset
```

### Data Upload
```bash
# Upload manual GTFS data to HDFS
python src\upload_manual_data.py <local_gtfs_directory>

# Example
python src\upload_manual_data.py data\raw\2025-09\gtfs

# Batch upload using Windows script
upload_gtfs.bat
```

### Analytics
```bash
# Local file analytics (recommended)
python src\local_gtfs_analytics.py <input_dir> <output_dir>
python src\local_gtfs_analytics.py data\raw\2025-09\gtfs analytics_results

# HDFS-based analytics (requires network configuration)
python src\gtfs_analytics.py <hdfs_input_path> <hdfs_output_path>
```

## 📈 Running Analytics

### Method 1: Local File Processing (Recommended)
```bash
# Process local GTFS files with PySpark
python src\local_gtfs_analytics.py data\raw\2025-09\gtfs results

# Output:
# Successfully loaded GTFS data
#   - agency: 66 records
#   - routes: 8,386 records
#   - stops: 47,540 records
#   - trips: 354,959 records
#   - stop_times: 7,435,943 records
```

### Method 2: HDFS-based Processing
```bash
# First upload data to HDFS
python src\upload_manual_data.py data\raw\2025-09\gtfs

# Run analytics on HDFS data
python src\gtfs_analytics.py hdfs://namenode:8020/data/gtfs_sverige/2025-09/gtfs results
```

### Method 3: Automatic Download and Processing
```bash
# Set up environment variables first
# Create .env file with TRAFIKLAB_API_KEY=your_key

# Download and process automatically
python -m src.ingest_gtfs
spark-submit src/process_gtfs.py
```

## 🛠️ Troubleshooting

### Docker Issues
```bash
# Restart containers
docker-compose down
docker-compose up -d

# Check logs
docker-compose logs namenode
docker-compose logs datanode

# Check container health
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Python Issues
```bash
# Reinstall dependencies
pip install --upgrade -r requirements.txt

# Verify PySpark installation
python -c "import pyspark; print(pyspark.__version__)"

# Check Java (required for PySpark)
java -version
```

### HDFS Connectivity
```bash
# Test Docker HDFS connection
docker exec -it id2221_project_group20-namenode-1 hdfs dfs -ls /

# Test Python utilities
python src\docker_hdfs_utils.py status

# Check if ports are accessible
curl http://localhost:9870
```

### Common Error Solutions

#### "Could not check Docker containers"
- Ensure Docker Desktop is running
- Restart Docker Desktop if needed

#### "Connection timed out" (Spark to HDFS)
- Use local analytics instead: `src\local_gtfs_analytics.py`
- Check Docker networking configuration

#### "HDFS command not found"
- Use Docker-based utilities: `src\docker_hdfs_utils.py`
- Or install Hadoop natively using `setup_hadoop.bat`

## 📋 Data Pipeline Overview

### Input Data
- **Source:** Swedish GTFS data from Trafiklab
- **Format:** CSV files (agency.txt, routes.txt, stops.txt, etc.)
- **Size:** ~392MB (including 368MB stop_times.txt)
- **Coverage:** National Swedish public transit

### Processing Steps
1. **Ingestion:** Download/extract GTFS ZIP files
2. **Upload:** Transfer to HDFS for distributed processing
3. **Analytics:** PySpark analysis of stops, routes, temporal patterns
4. **Output:** CSV and Parquet results for visualization

### Output Analytics
- **Stop Analysis:** Busiest/least busy stops, frequency statistics
- **Route Analysis:** Performance metrics, trip counts
- **Temporal Analysis:** Hourly patterns, peak/off-peak identification
- **Geographic Analysis:** Regional distribution patterns

## 🚀 Next Steps

### Immediate Extensions
- **Visualizations:** Create charts using matplotlib/pandas from analytics results
- **Regional Analysis:** Compare Stockholm vs Gothenburg vs Malmö patterns
- **Performance Optimization:** Tune Spark configurations for larger datasets

### Advanced Features
- **Real-time Processing:** Integrate with Kafka for live transit updates
- **Machine Learning:** Predict delays, optimize routes
- **Web Dashboard:** Build interactive visualizations
- **Automated Pipeline:** Schedule daily/weekly analytics runs

### Collaboration Features
- **Docker Compose:** Fully containerized development environment
- **Version Control:** Git-based collaboration with clear documentation
- **Reproducible Results:** Consistent environment across team members

## 📞 Support

### Success Criteria
You have successfully set up the project when:
1. ✅ Docker containers are running and healthy
2. ✅ HDFS UI is accessible at http://localhost:9870
3. ✅ GTFS data upload completes without errors
4. ✅ Analytics generates results in `analytics_results/` directory
5. ✅ You can see insights for 47,540+ stops and 7.4M+ events

### Getting Help
If you encounter issues:
1. Check that Docker Desktop is running
2. Verify all containers show "healthy" status
3. Ensure ports 8020 and 9870 are not blocked by firewall
4. Try restarting Docker containers: `docker-compose restart`
5. Use local analytics as fallback: `src\local_gtfs_analytics.py`

---

**🎯 This project successfully processes over 7.4 million Swedish transit events to generate actionable insights about public transportation patterns across Sweden!**