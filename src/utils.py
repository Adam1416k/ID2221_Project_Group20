import os
from dotenv import load_dotenv

load_dotenv()

TRAFIKLAB_API_KEY = os.getenv("TRAFIKLAB_API_KEY")
GTFS_YEAR = os.getenv("GTFS_YEAR", "2025")
GTFS_MONTH = os.getenv("GTFS_MONTH", "09")  # zero-padded
HDFS_URI = os.getenv("HDFS_URI", "hdfs://namenode:8020")
HDFS_BASE = os.getenv("HDFS_BASE", "/data/gtfs_sverige")

def ensure_env():
    missing = []
    if not TRAFIKLAB_API_KEY:
        missing.append("TRAFIKLAB_API_KEY")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")
