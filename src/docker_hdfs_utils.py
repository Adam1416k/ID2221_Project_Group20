"""
Docker-based HDFS utilities for GTFS data pipeline.
Uses Docker containers to interact with HDFS instead of local HDFS client.
"""
import subprocess
import sys
import os
from pathlib import Path
from utils import HDFS_BASE, GTFS_YEAR, GTFS_MONTH

def run_hdfs_docker_command(command_args, capture_output=True):
    """Run HDFS command through Docker container"""
    try:
        # Use the namenode container to run HDFS commands
        cmd = ["docker", "exec", "-i", "id2221_project_group20-namenode-1", "hdfs", "dfs"] + command_args
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
        return None, "Docker not found. Make sure Docker is installed and running."

def check_hdfs_status():
    """Check HDFS cluster status and connectivity"""
    print("Checking HDFS Status via Docker...")
    
    # Check if namenode container is running
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=namenode", "--format", "{{.Status}}"],
            capture_output=True, text=True, check=True
        )
        if "Up" not in result.stdout:
            print("ERROR: Namenode container is not running. Start with: docker-compose up -d")
            return False
    except Exception:
        print("ERROR: Could not check Docker containers. Make sure Docker is running.")
        return False
    
    # Test basic HDFS connectivity
    stdout, stderr = run_hdfs_docker_command(["-ls", "/"])
    if stderr:
        print(f"ERROR: HDFS connectivity failed: {stderr}")
        return False
    
    print("HDFS is accessible via Docker")
    
    # Check cluster health
    stdout, stderr = run_hdfs_docker_command(["-df", "-h"])
    if stdout:
        print("\nHDFS Cluster Storage:")
        print(stdout)
    
    return True

def create_hdfs_directories():
    """Create necessary HDFS directory structure"""
    print("Creating HDFS directory structure via Docker...")
    
    directories = [
        HDFS_BASE,
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}",
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}/gtfs",
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}/analytics",
        f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}/curated"
    ]
    
    for directory in directories:
        _, stderr = run_hdfs_docker_command(["-mkdir", "-p", directory])
        if stderr and "File exists" not in stderr:
            print(f"ERROR: Failed to create directory {directory}: {stderr}")
        else:
            print(f"Created/verified directory: {directory}")
    
    return True

def upload_local_files(local_path, hdfs_path):
    """Upload local files to HDFS via Docker"""
    print(f"[UPLOAD] Uploading {local_path} to HDFS: {hdfs_path}")
    
    # First create the HDFS directory
    _, stderr = run_hdfs_docker_command(["-mkdir", "-p", hdfs_path])
    
    # Copy files from host to container, then to HDFS
    local_path = Path(local_path).resolve()
    
    # Get the container name
    container_name = "id2221_project_group20-namenode-1"
    temp_container_path = "/tmp/gtfs_upload"
    
    try:
        # Create temp directory in container
        subprocess.run([
            "docker", "exec", container_name, "mkdir", "-p", temp_container_path
        ], check=True)
        
        # Copy files from host to container
        for file_path in local_path.iterdir():
            if file_path.is_file():
                print(f"   Copying {file_path.name}...")
                subprocess.run([
                    "docker", "cp", str(file_path), 
                    f"{container_name}:{temp_container_path}/{file_path.name}"
                ], check=True)
        
        # Move files from container temp to HDFS (upload each file individually)
        for file_path in local_path.iterdir():
            if file_path.is_file():
                file_in_container = f"{temp_container_path}/{file_path.name}"
                _, stderr = run_hdfs_docker_command(["-put", file_in_container, hdfs_path])
                if stderr:
                    print(f"ERROR: Error uploading {file_path.name}: {stderr}")
                    return False
        
        # Clean up temp directory
        subprocess.run([
            "docker", "exec", container_name, "rm", "-rf", temp_container_path
        ], check=True)
        
        print("Upload completed successfully!")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Error during upload: {e}")
        return False

def list_hdfs_contents(hdfs_path="/"):
    """List contents of HDFS directory"""
    print(f"Listing HDFS contents: {hdfs_path}")
    
    stdout, stderr = run_hdfs_docker_command(["-ls", hdfs_path])
    if stderr:
        print(f"ERROR: Could not list directory: {stderr}")
        return False
    
    if stdout:
        print(stdout)
    else:
        print("Directory is empty")
    
    return True

def main():
    """Main function for Docker-based HDFS management"""
    if len(sys.argv) < 2:
        print("Docker-based HDFS Management Utility for GTFS Data")
        print("\nUsage: python docker_hdfs_utils.py <command> [args]")
        print("\nCommands:")
        print("  status              - Check HDFS status via Docker")
        print("  create-dirs         - Create HDFS directory structure")
        print("  upload <local_path> - Upload local directory to HDFS")
        print("  list [hdfs_path]    - List HDFS directory contents")
        print("  ls [hdfs_path]      - Alias for list")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "status":
        check_hdfs_status()
    
    elif command == "create-dirs":
        if check_hdfs_status():
            create_hdfs_directories()
    
    elif command == "upload":
        if len(sys.argv) < 3:
            print("ERROR: Local path required for upload command")
            sys.exit(1)
        
        local_path = sys.argv[2]
        hdfs_path = f"{HDFS_BASE}/{GTFS_YEAR}-{GTFS_MONTH}/gtfs"
        
        if check_hdfs_status():
            upload_local_files(local_path, hdfs_path)
    
    elif command in ["list", "ls"]:
        hdfs_path = sys.argv[2] if len(sys.argv) > 2 else "/"
        if check_hdfs_status():
            list_hdfs_contents(hdfs_path)
    
    else:
        print(f"ERROR: Unknown command: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()