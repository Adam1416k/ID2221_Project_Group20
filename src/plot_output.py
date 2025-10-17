from matplotlib import pyplot as plt
import json


def plot_top_stops(top_stops: list[dict]):
    """Plot the top N most visited stops. Bar chart."""
    stop_names = [stop["stop_name"] for stop in top_stops]
    frequencies = [stop["frequency"] for stop in top_stops]

    plt.figure(figsize=(12, 6))
    plt.barh(stop_names, frequencies, color="skyblue")
    plt.xlabel("Number of Visits")
    plt.title(f"Top {len(top_stops)} Most Visited Stops")
    plt.gca().invert_yaxis()  # Highest values on top
    plt.tight_layout()
    plt.show()


def plot_least_stops(least_stops: list[dict]):
    """Plot the top N least visited stops. Bar chart."""
    stop_names = [stop["stop_name"] for stop in least_stops]
    frequencies = [stop["frequency"] for stop in least_stops]

    plt.figure(figsize=(12, 6))
    plt.barh(stop_names, frequencies, color="salmon")
    plt.xlabel("Number of Visits")
    plt.title(f"Top {len(least_stops)} Least Visited Stops")
    plt.gca().invert_yaxis()  # Highest values on top
    plt.tight_layout()
    plt.show()


def main(path_to_results):
    """Generate plots based on the analysis results file"""
    with open(path_to_results, "r") as f:
        results = json.load(f)

    if results["top_stops"]:
        plot_top_stops(results["top_stops"][:30])
    else:
        print("No top stops data available for plotting.")
    
    if results["least_stops"]:
        plot_least_stops(results["least_stops"][:30])
    else:
        print("No least stops data available for plotting.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python plot_output.py <path_to_results_json>")
    else:
        main(sys.argv[1])
