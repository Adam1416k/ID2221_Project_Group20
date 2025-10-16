"""
Script to upload manually downloaded GTFS data to HDFS.
Use this when you have manually downloaded GTFS data files and want to upload them to HDFS.
"""
import os
import sys
import subprocess
from pathlib import Path
from utils import HDFS_URI, HDFS_BASE, GTFS_YEAR, GTFS_MONTH

def hdfs_put(local_path: Path, hdfs_path: str):
    """Upload a local directory to HDFS"""
    try:
        # Create HDFS directory structure
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_path], check=True)
        print(f"Created HDFS directory: {hdfs_path}")
        
        # Upload all files from local directory
        for file_path in local_path.iterdir():
            if file_path.is_file():
                print(f"Uploading {file_path.name} to HDFS...")
                subprocess.run(["hdfs", "dfs", "-put", "-f", str(file_path), hdfs_path], check=True)
                
        print(f"Successfully uploaded all files to {hdfs_path}")
        
    except subprocess.CalledProcessError as e:
        print(f"Error uploading to HDFS: {e}")
        return False
    return True

def check_hdfs_health():
    """Check if HDFS is accessible"""
    try:
        subprocess.run(["hdfs", "dfs", "-ls", "/"], capture_output=True, text=True, check=True)
        print("HDFS is accessible")
        return True
    except subprocess.CalledProcessError:
        print("HDFS is not accessible. Make sure Hadoop is running and configured.")
        return False
    except FileNotFoundError:
        print("HDFS command not found. Make sure Hadoop is installed and in PATH.")
        return False

def list_gtfs_files(local_dir: Path):
    """List and validate GTFS files"""
    required_files = [
        "agency.txt", "routes.txt", "trips.txt", "stops.txt", 
        "stop_times.txt", "calendar.txt"
    ]
    
    optional_files = [
        "calendar_dates.txt", "shapes.txt", "frequencies.txt",
        "transfers.txt", "feed_info.txt"
    ]
    
    found_files = [f.name for f in local_dir.iterdir() if f.is_file() and f.suffix == '.txt']
    
    print(f"\nFound {len(found_files)} GTFS files in {local_dir}")
    
    missing_required = [f for f in required_files if f not in found_files]
    found_optional = [f for f in optional_files if f in found_files]
    
    if missing_required:
        print(f"WARNING: Missing required files: {', '.join(missing_required)}")
        return False
    
    print("All required GTFS files found:")
    for file in required_files:
        if file in found_files:
            print(f"  - {file}")
    
    if found_optional:
        print("Optional files found:")
        for file in found_optional:
            print(f"  - {file}")
    
    return True

def main():
    if len(sys.argv) != 2:
        print("Usage: python upload_manual_data.py <path_to_gtfs_directory>")
        print("Example: python upload_manual_data.py ../data/raw/2025-10/gtfs")
        sys.exit(1)
    
    local_gtfs_dir = Path(sys.argv[1]).resolve()
    
    if not local_gtfs_dir.exists():
        print(f"ERROR: Directory does not exist: {local_gtfs_dir}")
        sys.exit(1)
    
    if not local_gtfs_dir.is_dir():
        print(f"ERROR: Path is not a directory: {local_gtfs_dir}")
        sys.exit(1)
    
    print("Starting HDFS upload for manually downloaded GTFS data")
    print(f"Local directory: {local_gtfs_dir}")
    
    # Check HDFS health
    if not check_hdfs_health():
        sys.exit(1)
    
    # Validate GTFS files
    if not list_gtfs_files(local_gtfs_dir):
        print("ERROR: GTFS validation failed. Please check your files.")
        sys.exit(1)
    
    # Determine HDFS target path
    year = GTFS_YEAR
    month = GTFS_MONTH
    hdfs_target = f"{HDFS_URI}{HDFS_BASE}/{year}-{month}/gtfs"
    
    print(f"\nUploading to HDFS: {hdfs_target}")
    
    # Upload to HDFS
    if hdfs_put(local_gtfs_dir, hdfs_target):
        print("\nUpload completed successfully!")
        print(f"HDFS location: {hdfs_target}")
        
        # List uploaded files
        print("\nVerifying upload...")
        try:
            result = subprocess.run(
                ["hdfs", "dfs", "-ls", hdfs_target], 
                capture_output=True, text=True, check=True
            )
            print("Files in HDFS:")
            print(result.stdout)
        except subprocess.CalledProcessError:
            print("ERROR: Could not verify upload")
    else:
        print("ERROR: Upload failed")
        sys.exit(1)

if __name__ == "__main__":
    main()