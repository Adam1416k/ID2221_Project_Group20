import os
from dotenv import load_dotenv

load_dotenv()

TRAFIKLAB_API_KEY = os.getenv("TRAFIKLAB_API_KEY")
GTFS_YEAR = os.getenv("GTFS_YEAR", "2025")
GTFS_MONTH = os.getenv("GTFS_MONTH", "09")  # zero-padded
HDFS_URI = os.getenv("HDFS_URI", "hdfs://localhost:9000")
HDFS_BASE = os.getenv("HDFS_BASE", "/data/gtfs_sverige")

def ensure_env():
    missing = []
    if not TRAFIKLAB_API_KEY:
        missing.append("TRAFIKLAB_API_KEY")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

def get_hdfs_paths():
    """Get current HDFS input and output paths"""
    year = GTFS_YEAR
    month = GTFS_MONTH
    hdfs_input_path = f"{HDFS_URI}{HDFS_BASE}/{year}-{month}/gtfs"
    hdfs_output_path = f"{HDFS_URI}{HDFS_BASE}/{year}-{month}/analytics"
    return hdfs_input_path, hdfs_output_path