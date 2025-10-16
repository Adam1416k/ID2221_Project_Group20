"""
Upload GTFS data to HDFS for distributed processing.
This script uploads local GTFS files to the HDFS cluster for Spark processing.
"""
import os
import sys
import subprocess
from pathlib import Path
from utils import HDFS_URI, HDFS_BASE, GTFS_YEAR, GTFS_MONTH, get_hdfs_paths

def check_hdfs_connection():
    """Check if HDFS is accessible"""
    try:
        result = subprocess.run([
            "docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("HDFS connection successful")
            return True
        else:
            print(f"HDFS connection failed: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print("HDFS connection timeout - check if containers are running")
        return False
    except Exception as e:
        print(f"Error checking HDFS: {e}")
        return False

def create_hdfs_directories():
    """Create necessary directories in HDFS"""
    hdfs_input_path, hdfs_output_path = get_hdfs_paths()
    
    # Remove hdfs:// prefix for docker commands
    input_dir = hdfs_input_path.replace(HDFS_URI, "")
    output_dir = hdfs_output_path.replace(HDFS_URI, "")
    
    directories = [
        HDFS_BASE,
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}",
        input_dir,
        output_dir
    ]
    
    for dir_path in directories:
        try:
            result = subprocess.run([
                "docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", dir_path
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"Created directory: {dir_path}")
            else:
                print(f"Directory might exist: {dir_path}")
        except Exception as e:
            print(f"Error creating directory {dir_path}: {e}")

def upload_gtfs_files(local_gtfs_path):
    """Upload GTFS files to HDFS"""
    if not os.path.exists(local_gtfs_path):
        print(f"Local GTFS path not found: {local_gtfs_path}")
        return False
    
    hdfs_input_path, _ = get_hdfs_paths()
    hdfs_dir = hdfs_input_path.replace(HDFS_URI, "")
    
    # Get list of GTFS files
    gtfs_files = [
        "agency.txt", "routes.txt", "stops.txt", "trips.txt", 
        "stop_times.txt", "calendar.txt", "calendar_dates.txt",
        "feed_info.txt", "transfers.txt"
    ]
    
    uploaded_count = 0
    
    for filename in gtfs_files:
        local_file = os.path.join(local_gtfs_path, filename)
        if os.path.exists(local_file):
            try:
                # Copy file from host to namenode container
                copy_result = subprocess.run([
                    "docker", "cp", local_file, f"namenode:/tmp/{filename}"
                ], capture_output=True, text=True)
                
                if copy_result.returncode != 0:
                    print(f"Failed to copy {filename} to container")
                    continue
                
                # Upload from container to HDFS
                upload_result = subprocess.run([
                    "docker", "exec", "namenode", "hdfs", "dfs", "-put", 
                    f"/tmp/{filename}", f"{hdfs_dir}/{filename}"
                ], capture_output=True, text=True)
                
                if upload_result.returncode == 0:
                    print(f"Uploaded {filename}")
                    uploaded_count += 1
                else:
                    print(f"Failed to upload {filename}: {upload_result.stderr}")
                
                # Clean up temp file
                subprocess.run([
                    "docker", "exec", "namenode", "rm", f"/tmp/{filename}"
                ], capture_output=True, text=True)
                
            except Exception as e:
                print(f"Error uploading {filename}: {e}")
        else:
            print(f"File not found: {local_file}")
    
    print(f"\nUpload Summary: {uploaded_count}/{len(gtfs_files)} files uploaded")
    return uploaded_count > 0

def verify_hdfs_upload():
    """Verify that files were uploaded correctly"""
    hdfs_input_path, _ = get_hdfs_paths()
    hdfs_dir = hdfs_input_path.replace(HDFS_URI, "")
    
    try:
        result = subprocess.run([
            "docker", "exec", "namenode", "hdfs", "dfs", "-ls", hdfs_dir
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"\n HDFS Directory Contents ({hdfs_dir}):")
            print(result.stdout)
            
            # Count files
            file_count = len([line for line in result.stdout.split('\n') 
                            if line.strip() and not line.startswith('Found')])
            print(f"Total files in HDFS: {file_count}")
            return True
        else:
            print(f"Failed to list HDFS directory: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error verifying upload: {e}")
        return False

def main():
    """Main upload process"""
    if len(sys.argv) < 2:
        print("HDFS Upload Script")
        print("Usage: python upload_to_hdfs.py <local_gtfs_directory>")
        print("Example: python upload_to_hdfs.py data/raw/2025-09/gtfs")
        sys.exit(1)
    
    local_gtfs_path = sys.argv[1]
    
    print("Starting HDFS Upload Process")
    print(f"Local GTFS path: {local_gtfs_path}")
    print(f"HDFS URI: {HDFS_URI}")
    print(f"Target: {GTFS_YEAR}-{GTFS_MONTH}")
    
    # Step 1: Check HDFS connection
    print("\nStep 1: Checking HDFS connection...")
    if not check_hdfs_connection():
        print("Cannot connect to HDFS. Please ensure Docker containers are running:")
        print("   docker-compose up -d")
        sys.exit(1)
    
    # Step 2: Create directories
    print("\nStep 2: Creating HDFS directories...")
    create_hdfs_directories()
    
    # Step 3: Upload files
    print(f"\nStep 3: Uploading GTFS files from {local_gtfs_path}...")
    if not upload_gtfs_files(local_gtfs_path):
        print("Upload failed")
        sys.exit(1)
    
    # Step 4: Verify upload
    print("\nStep 4: Verifying upload...")
    if verify_hdfs_upload():
        print("\n HDFS upload completed successfully!")
        print(" You can now run HDFS-based analytics:")
        print(" python src/hdfs_gtfs_analytics.py")
    else:
        print("Upload verification failed")

if __name__ == "__main__":
    main()