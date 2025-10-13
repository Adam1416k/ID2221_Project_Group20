import os
import io
import zipfile
import subprocess
from pathlib import Path
import requests

from utils import ensure_env, TRAFIKLAB_API_KEY, GTFS_MONTH, GTFS_YEAR, HDFS_URI, HDFS_BASE

def hdfs_put(local_path: Path, hdfs_path: str):
    # Uses the hdfs CLI. Assumes container/host has Hadoop client available.
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_path], check=True)
    # Put all files
    for p in local_path.iterdir():
        if p.is_file():
            subprocess.run(["hdfs", "dfs", "-put", "-f", str(p), hdfs_path], check=True)

def main():
    ensure_env()
    year = GTFS_YEAR
    month = GTFS_MONTH

    base_url = f"https://data.samtrafiken.se/trafiklab/gtfs-sverige-2/{year}/{month}/gtfs-sverige-2.zip"
    url = f"{base_url}?apikey={TRAFIKLAB_API_KEY}"

    out_root = Path(__file__).resolve().parents[1] / "data" / "raw" / f"{year}-{month}"
    out_root.mkdir(parents=True, exist_ok=True)
    zip_path = out_root / "gtfs-sverige-2.zip"

    print(f"Downloading {url}")
    r = requests.get(url, timeout=120)
    r.raise_for_status()

    # Save zip
    with open(zip_path, "wb") as f:
        f.write(r.content)
    print(f"Saved ZIP to {zip_path}")

    # Extract
    extract_dir = out_root / "gtfs"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        zf.extractall(extract_dir)
    print(f"Extracted into {extract_dir}")

    # Push to HDFS
    hdfs_target = f"{HDFS_URI}{HDFS_BASE}/{year}-{month}/gtfs"
    print(f"Copying to HDFS: {hdfs_target}")
    hdfs_put(extract_dir, hdfs_target)

    print("✅ Ingestion complete.")

if __name__ == "__main__":
    main()
