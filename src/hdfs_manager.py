"""
HDFS Management Helper
Utility script for managing HDFS operations, checking status, and troubleshooting.
"""
import subprocess
import sys
from utils import get_hdfs_paths, HDFS_URI

def run_docker_command(command, description=""):
    """Run a docker command and return the result"""
    try:
        result = subprocess.run(command, capture_output=True, text=True, shell=True)
        if description:
            print(f" {description}")
        return result
    except Exception as e:
        print(f" Error running command: {e}")
        return None

def check_docker_containers():
    """Check if HDFS Docker containers are running"""
    print(" Checking Docker containers status...")
    
    result = run_docker_command("docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'")
    if result and result.returncode == 0:
        print(result.stdout)
        
        # Check specifically for HDFS containers
        if "namenode" in result.stdout and "datanode" in result.stdout:
            print(" HDFS containers are running")
            return True
        else:
            print(" HDFS containers not found")
            print(" Start them with: docker-compose up -d")
            return False
    else:
        print(" Error checking Docker containers")
        return False

def check_hdfs_health():
    """Check HDFS cluster health"""
    print("\n Checking HDFS cluster health...")
    
    # Check namenode status
    result = run_docker_command(
        "docker exec namenode hdfs dfsadmin -report",
        "Checking HDFS cluster report"
    )
    
    if result and result.returncode == 0:
        print(" HDFS cluster is healthy")
        print(result.stdout)
        return True
    else:
        print(" HDFS cluster health check failed")
        if result and result.stderr:
            print(f"Error: {result.stderr}")
        return False

def list_hdfs_contents(path="/"):
    """List contents of HDFS directory"""
    print(f"\n Listing HDFS contents: {path}")
    
    result = run_docker_command(
        f"docker exec namenode hdfs dfs -ls {path}",
        f"Listing {path}"
    )
    
    if result and result.returncode == 0:
        if result.stdout.strip():
            print(result.stdout)
        else:
            print(" Directory is empty")
        return True
    else:
        print(f" Failed to list {path}")
        if result and result.stderr:
            print(f"Error: {result.stderr}")
        return False

def check_gtfs_data():
    """Check if GTFS data exists in HDFS"""
    print("\n Checking for GTFS data in HDFS...")
    
    hdfs_input_path, _ = get_hdfs_paths()
    hdfs_dir = hdfs_input_path.replace(HDFS_URI, "")
    
    result = run_docker_command(
        f"docker exec namenode hdfs dfs -ls {hdfs_dir}",
        f"Checking GTFS data at {hdfs_dir}"
    )
    
    if result and result.returncode == 0:
        print(" GTFS data found in HDFS:")
        print(result.stdout)
        
        # Count files
        file_count = len([line for line in result.stdout.split('\n') 
                         if line.strip() and not line.startswith('Found')])
        print(f" Total GTFS files: {file_count}")
        return True
    else:
        print(" GTFS data not found in HDFS")
        print(" Upload data with: python src/upload_to_hdfs.py data/raw/2025-09/gtfs")
        return False

def check_analysis_results():
    """Check if analysis results exist in HDFS"""
    print("\n Checking for analysis results in HDFS...")
    
    _, hdfs_output_path = get_hdfs_paths()
    hdfs_dir = hdfs_output_path.replace(HDFS_URI, "")
    
    result = run_docker_command(
        f"docker exec namenode hdfs dfs -ls {hdfs_dir}",
        f"Checking analysis results at {hdfs_dir}"
    )
    
    if result and result.returncode == 0:
        print(" Analysis results found in HDFS:")
        print(result.stdout)
        return True
    else:
        print(" No analysis results found in HDFS")
        print(" Run analytics with: python src/hdfs_gtfs_analytics.py")
        return False

def clean_hdfs_data():
    """Clean all HDFS data (use with caution)"""
    print("\n Cleaning HDFS data...")
    
    confirmation = input("  This will delete ALL data in HDFS. Continue? (yes/no): ")
    if confirmation.lower() != 'yes':
        print(" Operation cancelled")
        return
    
    hdfs_input_path, _ = get_hdfs_paths()
    base_dir = hdfs_input_path.replace(HDFS_URI, "").split('/')[:-1]
    base_path = '/'.join(base_dir)
    
    result = run_docker_command(
        f"docker exec namenode hdfs dfs -rm -r {base_path}",
        f"Removing {base_path}"
    )
    
    if result and result.returncode == 0:
        print(" HDFS data cleaned successfully")
    else:
        print(" Failed to clean HDFS data")
        if result and result.stderr:
            print(f"Error: {result.stderr}")

def download_analysis_results(local_output_dir="hdfs_results"):
    """Download analysis results from HDFS to local filesystem"""
    print(f"\n  Downloading analysis results to {local_output_dir}/...")
    
    import os
    os.makedirs(local_output_dir, exist_ok=True)
    
    _, hdfs_output_path = get_hdfs_paths()
    hdfs_dir = hdfs_output_path.replace(HDFS_URI, "")
    
    # List available results
    result = run_docker_command(f"docker exec namenode hdfs dfs -ls {hdfs_dir}")
    if not result or result.returncode != 0:
        print(" No analysis results found to download")
        return
    
    # Download each result directory
    for line in result.stdout.split('\n'):
        if line.strip() and not line.startswith('Found'):
            parts = line.split()
            if len(parts) >= 8:
                dir_name = parts[-1].split('/')[-1]
                if dir_name:
                    print(f" Downloading {dir_name}...")
                    download_result = run_docker_command(
                        f"docker exec namenode hdfs dfs -get {hdfs_dir}/{dir_name} /tmp/{dir_name}"
                    )
                    if download_result and download_result.returncode == 0:
                        # Copy from container to host
                        copy_result = run_docker_command(
                            f"docker cp namenode:/tmp/{dir_name} {local_output_dir}/"
                        )
                        if copy_result and copy_result.returncode == 0:
                            print(f" Downloaded {dir_name}")
                        else:
                            print(f" Failed to copy {dir_name} from container")
                    else:
                        print(f" Failed to download {dir_name}")
    
    print(f"\n Results saved to: {local_output_dir}/")

def main():
    """Main HDFS management interface"""
    if len(sys.argv) < 2:
        print("HDFS Management Helper")
        print("Usage: python hdfs_manager.py <command>")
        print("\nAvailable commands:")
        print("  status     - Check overall HDFS status")
        print("  health     - Check HDFS cluster health")
        print("  list       - List HDFS root contents")
        print("  list-data  - Check for GTFS data")
        print("  list-results - Check for analysis results")
        print("  download   - Download analysis results to local")
        print("  clean      - Clean all HDFS data (DANGEROUS)")
        print("\nExamples:")
        print("  python src/hdfs_manager.py status")
        print("  python src/hdfs_manager.py download")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    print(f" HDFS Manager - {command.upper()}")
    print(f" HDFS URI: {HDFS_URI}")
    
    if command == "status":
        print("\n" + "="*50)
        containers_ok = check_docker_containers()
        if containers_ok:
            check_hdfs_health()
            check_gtfs_data()
            check_analysis_results()
    
    elif command == "health":
        check_hdfs_health()
    
    elif command == "list":
        list_hdfs_contents("/")
    
    elif command == "list-data":
        check_gtfs_data()
    
    elif command == "list-results":
        check_analysis_results()
    
    elif command == "download":
        download_analysis_results()
    
    elif command == "clean":
        clean_hdfs_data()
    
    else:
        print(f" Unknown command: {command}")
        print(" Run without arguments to see available commands")

if __name__ == "__main__":
    main()