"""
HDFS management utilities for GTFS data pipeline.
Provides tools to check HDFS status, manage directories, and inspect data.
"""
import subprocess
import sys
import json
from pathlib import Path
from utils import HDFS_URI, HDFS_BASE, GTFS_YEAR, GTFS_MONTH

def run_hdfs_command(command_args, capture_output=True):
    """Run HDFS command and return result"""
    try:
        cmd = ["hdfs", "dfs"] + command_args
        result = subprocess.run(
            cmd, 
            capture_output=capture_output, 
            text=True, 
            check=True
        )
        if capture_output:
            return result.stdout.strip(), result.stderr.strip()
        return None, None
    except subprocess.CalledProcessError as e:
        if capture_output:
            return None, e.stderr
        return None, str(e)
    except FileNotFoundError:
        return None, "HDFS command not found. Make sure Hadoop is installed and in PATH."

def check_hdfs_status():
    """Check HDFS cluster status and connectivity"""
    print("Checking HDFS Status...")
    
    # Test basic connectivity
    stdout, stderr = run_hdfs_command(["-ls", "/"])
    if stderr:
        print(f"ERROR: HDFS connectivity failed: {stderr}")
        return False
    
    print("HDFS is accessible")
    
    # Check cluster health
    stdout, stderr = run_hdfs_command(["-df", "-h"])
    if stdout:
        print("\nHDFS Cluster Storage:")
        print(stdout)
    
    return True

def list_gtfs_data():
    """List available GTFS datasets in HDFS"""
    print(f"\nListing GTFS datasets in {HDFS_URI}{HDFS_BASE}")
    
    stdout, stderr = run_hdfs_command(["-ls", f"{HDFS_BASE}"])
    if stderr:
        print(f"ERROR: Could not list GTFS data: {stderr}")
        return []
    
    datasets = []
    if stdout:
        lines = stdout.strip().split('\n')
        for line in lines:
            if line.strip():
                parts = line.split()
                if len(parts) >= 8:
                    path = parts[-1]
                    dataset_name = path.split('/')[-1]
                    datasets.append(dataset_name)
        
        if datasets:
            print("Available datasets:")
            for dataset in datasets:
                print(f"  - {dataset}")
        else:
            print("No datasets found")
    
    return datasets

def inspect_dataset(dataset_name=None):
    """Inspect a specific GTFS dataset"""
    if not dataset_name:
        dataset_name = f"{GTFS_YEAR}-{GTFS_MONTH}"
    
    dataset_path = f"{HDFS_BASE}/{dataset_name}"
    print(f"\nInspecting dataset: {dataset_name}")
    print(f"Path: {HDFS_URI}{dataset_path}")
    
    # Check if dataset exists
    stdout, stderr = run_hdfs_command(["-test", "-d", dataset_path])
    if stderr:
        print(f"ERROR: Dataset does not exist: {dataset_path}")
        return False
    
    # List contents
    stdout, stderr = run_hdfs_command(["-ls", "-R", dataset_path])
    if stderr:
        print(f"ERROR: Could not list dataset contents: {stderr}")
        return False
    
    print("\nDataset contents:")
    if stdout:
        lines = stdout.strip().split('\n')
        for line in lines:
            if line.strip():
                parts = line.split()
                if len(parts) >= 8:
                    permissions = parts[0]
                    size = parts[4]
                    path = parts[-1]
                    filename = path.split('/')[-1]
                    
                    if not permissions.startswith('d'):  # Not a directory
                        # Convert size to human readable
                        size_int = int(size)
                        if size_int < 1024:
                            size_str = f"{size_int} B"
                        elif size_int < 1024 * 1024:
                            size_str = f"{size_int / 1024:.1f} KB"
                        else:
                            size_str = f"{size_int / (1024 * 1024):.1f} MB"
                        
                        print(f"   {filename} ({size_str})")
                    else:
                        print(f"  {filename}/")
    
    return True

def check_gtfs_files(dataset_name=None):
    """Check if all required GTFS files are present"""
    if not dataset_name:
        dataset_name = f"{GTFS_YEAR}-{GTFS_MONTH}"
    
    required_files = [
        "agency.txt", "routes.txt", "trips.txt", "stops.txt", 
        "stop_times.txt", "calendar.txt"
    ]
    
    optional_files = [
        "calendar_dates.txt", "shapes.txt", "frequencies.txt",
        "transfers.txt", "feed_info.txt"
    ]
    
    gtfs_path = f"{HDFS_BASE}/{dataset_name}/gtfs"
    print(f"\nValidating GTFS files in {gtfs_path}")
    
    found_files = []
    missing_files = []
    
    for file in required_files + optional_files:
        file_path = f"{gtfs_path}/{file}"
        _, stderr = run_hdfs_command(["-test", "-f", file_path])
        
        if not stderr:  # File exists
            found_files.append(file)
        else:
            if file in required_files:
                missing_files.append(file)
    
    print(f"\nFound {len(found_files)} GTFS files:")
    for file in found_files:
        marker = "[R]" if file in required_files else "[O]"
        print(f"  {marker} {file}")
    
    if missing_files:
        print("\nERROR: Missing required files:")
        for file in missing_files:
            print(f"  ERROR: {file}")
        return False
    
    print("\nAll required GTFS files are present!")
    return True

def clean_dataset(dataset_name):
    """Remove a dataset from HDFS"""
    dataset_path = f"{HDFS_BASE}/{dataset_name}"
    
    print(f"WARNING: Are you sure you want to delete dataset '{dataset_name}'?")
    print(f"Path: {HDFS_URI}{dataset_path}")
    
    confirmation = input("Type 'yes' to confirm deletion: ").strip().lower()
    if confirmation != 'yes':
        print("ERROR: Deletion cancelled")
        return False
    
    _, stderr = run_hdfs_command(["-rm", "-r", "-f", dataset_path])
    if stderr:
        print(f"ERROR: Failed to delete dataset: {stderr}")
        return False
    
    print(f"Successfully deleted dataset: {dataset_name}")
    return True

def create_hdfs_directories():
    """Create necessary HDFS directory structure"""
    print("Creating HDFS directory structure...")
    
    directories = [
        HDFS_BASE,
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}",
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}/gtfs",
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}/analytics",
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}/curated"
    ]
    
    for directory in directories:
        _, stderr = run_hdfs_command(["-mkdir", "-p", directory])
        if stderr and "File exists" not in stderr:
            print(f"ERROR: Failed to create directory {directory}: {stderr}")
        else:
            print(f"Created/verified directory: {directory}")
    
    return True

def check_analytics_results(dataset_name=None):
    """Check analytics results for a dataset"""
    if not dataset_name:
        dataset_name = f"{GTFS_YEAR}-{GTFS_MONTH}"
    
    analytics_path = f"{HDFS_BASE}/{dataset_name}/analytics"
    print(f"\nChecking analytics results in {analytics_path}")
    
    # Check if analytics directory exists
    stdout, stderr = run_hdfs_command(["-test", "-d", analytics_path])
    if stderr:
        print(f"ERROR: No analytics results found: {analytics_path}")
        return False
    
    # List analytics results
    stdout, stderr = run_hdfs_command(["-ls", analytics_path])
    if stderr:
        print(f"ERROR: Could not list analytics results: {stderr}")
        return False
    
    if stdout:
        print("Available analytics results:")
        lines = stdout.strip().split('\n')
        for line in lines:
            if line.strip():
                parts = line.split()
                if len(parts) >= 8:
                    path = parts[-1]
                    result_name = path.split('/')[-1]
                    print(f"  {result_name}")
    
    return True

def main():
    """Main function for HDFS management utility"""
    if len(sys.argv) < 2:
        print("HDFS Management Utility for GTFS Data")
        print("\nUsage: python hdfs_utils.py <command> [args]")
        print("\nCommands:")
        print("  status          - Check HDFS status and connectivity")
        print("  list            - List available GTFS datasets")
        print("  inspect [name]  - Inspect a dataset (default: current month)")
        print("  validate [name] - Validate GTFS files in dataset")
        print("  create-dirs     - Create HDFS directory structure")
        print("  analytics [name]- Check analytics results")
        print("  clean <name>    - Delete a dataset (use with caution)")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "status":
        check_hdfs_status()
    
    elif command == "list":
        list_gtfs_data()
    
    elif command == "inspect":
        dataset_name = sys.argv[2] if len(sys.argv) > 2 else None
        inspect_dataset(dataset_name)
    
    elif command == "validate":
        dataset_name = sys.argv[2] if len(sys.argv) > 2 else None
        check_gtfs_files(dataset_name)
    
    elif command == "create-dirs":
        create_hdfs_directories()
    
    elif command == "analytics":
        dataset_name = sys.argv[2] if len(sys.argv) > 2 else None
        check_analytics_results(dataset_name)
    
    elif command == "clean":
        if len(sys.argv) < 3:
            print("ERROR: Dataset name required for clean command")
            sys.exit(1)
        clean_dataset(sys.argv[2])
    
    else:
        print(f"ERROR: Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()