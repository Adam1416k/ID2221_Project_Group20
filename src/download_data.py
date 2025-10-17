import os
import zipfile


def download_data(url: str, output_path: str) -> str:
    """Curl download GTFS data from a given URL and save it to output_path."""
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))

    # Download the file
    temp_file = output_path + "_raw"
    os.system(f"curl -o {temp_file} {url}")

    # Unzip the file
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    with zipfile.ZipFile(temp_file, "r") as zip_ref:
        zip_ref.extractall(output_path)

    print(f"Downloaded and extracted to: {output_path}")

    os.remove(temp_file)  # Clean up zip file after extraction

    return output_path


def download_period(
    start_date: str, end_date: str, base_url: str, output_dir: str
) -> bool:
    """Download GTFS data for a given period from start_date to end_date."""
    from datetime import datetime, timedelta

    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_date_dt:
        date_str = current_date.strftime("%Y%m%d")
        url = f"{base_url}/sweden-{date_str}.zip"
        output_path = os.path.join(output_dir, f"sweden-{date_str}")

        print(f"Downloading GTFS data for {date_str}...")
        download_data(url, output_path)

        current_date += timedelta(days=1)

    print("All data downloaded.")
    return True


if __name__ == "__main__":
    # Example usage
    base_url = "https://data.samtrafiken.se/trafiklab/gtfs-sverige-2/2025/10"
    output_dir = "../data/raw/2025-10"
    download_period("2025-10-01", "2025-10-05", base_url, output_dir)

    # Or, single download
    hdfs_path = download_data("https://data.samtrafiken.se/trafiklab/gtfs-sverige-2/2025/10/sweden-20251015.zip", "../data/raw/sweden-20251015")
