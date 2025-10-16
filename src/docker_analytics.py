"""
GTFS analytics for Docker Spark environment with local results output
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc, asc
import os
import sys
from datetime import datetime
from typing import Any, Dict, List


def save_results_locally(results_dict, output_dir="/data/analytics_results"):
    """Save analytics results to local files (UTF-8)."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(output_dir, f"gtfs_summary_{timestamp}.txt")

    # Save summary with UTF-8 encoding for Swedish characters
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"GTFS Analytics Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 50 + "\n\n")

        f.write("Dataset Summary:\n")
        f.write(f"- Agency records: {results_dict['counts']['agency']:,}\n")
        f.write(f"- Routes records: {results_dict['counts']['routes']:,}\n")
        f.write(f"- Stops records: {results_dict['counts']['stops']:,}\n")
        f.write(f"- Trips records: {results_dict['counts']['trips']:,}\n")
        f.write(f"- Stop times records: {results_dict['counts']['stop_times']:,}\n")
        f.write("\n")

        # Top stops
        f.write("Top 10 Most Visited Stops:\n")
        f.write("-" * 30 + "\n")
        for i, row in enumerate(results_dict['top_stops'], 1):
            stop_name = row['stop_name'] or "Unknown"
            f.write(f"{i:2d}. {stop_name[:50]} ({row['frequency']:,} visits)\n")
        f.write("\n")

        # Least visited stops
        f.write("Top 10 Least Visited Stops:\n")
        f.write("-" * 30 + "\n")
        for i, row in enumerate(results_dict['least_stops'], 1):
            stop_name = row['stop_name'] or "Unknown"
            f.write(f"{i:2d}. {stop_name[:50]} ({row['frequency']:,} visits)\n")
        f.write("\n")

        # Route stats
        f.write("Route Statistics:\n")
        f.write("-" * 30 + "\n")
        f.write(f"- Total unique routes: {results_dict['route_stats']['total_routes']:,}\n")
        f.write(f"- Average trips per route: {results_dict['route_stats']['avg_trips_per_route']:.1f}\n")

    print(f"Results saved to: {out_path}")
    return out_path

def main():
    # Create Spark session
    spark = SparkSession.builder.appName("GTFS_Analytics_Docker").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    
    print("Starting GTFS Analytics with Docker Spark")
    print("Python runtime:", sys.version)
    
    hdfs_path = "/data/raw/sweden-20251015"
    
    try:
        # Load data
        print("Loading GTFS data...")
        
        agency = spark.read.csv(hdfs_path + "/agency.txt", header=True)
        routes = spark.read.csv(hdfs_path + "/routes.txt", header=True)
        stops = spark.read.csv(hdfs_path + "/stops.txt", header=True)
        trips = spark.read.csv(hdfs_path + "/trips.txt", header=True)
        stop_times = spark.read.csv(hdfs_path + "/stop_times.txt", header=True)
        
        print("Data loaded successfully!")
        
        # Collect results dictionary
        results: Dict[str, Any] = {
            'counts': {
                'agency': agency.count(),
                'routes': routes.count(),
                'stops': stops.count(),
                'trips': trips.count(),
                'stop_times': stop_times.count()
            },
            'top_stops': [],
            'least_stops': [],
            'route_stats': {}
        }
        
        print("\nDataset Summary:")
        for key, value in results['counts'].items():
            print(f"{key.title()}: {value:,}")
        
        # Analyze top stops (most visited)
        print("\nAnalyzing most visited stops...")
        top_stops = stop_times.groupBy("stop_id") \
            .agg(count("*").alias("frequency")) \
            .join(stops, "stop_id", "left") \
            .select("stop_id", "stop_name", "frequency") \
            .orderBy(desc("frequency")) \
            .limit(10)
        
    # Build top stops list
        print("Top 10 Most Visited Stops:")
        for i, row in enumerate(top_stops.collect(), 1):
            stop_name = row.stop_name or "Unknown"
            output_line = f"{i:2d}. {str(stop_name)[:50]} ({row.frequency:,} visits)"
            print(output_line)
            results['top_stops'].append({
                'stop_id': row.stop_id,
                'stop_name': stop_name,
                'frequency': row.frequency
            })
        
        # Analyze least visited stops
        print("\nAnalyzing least visited stops...")
        least_stops = stop_times.groupBy("stop_id") \
            .agg(count("*").alias("frequency")) \
            .join(stops, "stop_id", "left") \
            .select("stop_id", "stop_name", "frequency") \
            .orderBy(asc("frequency")) \
            .limit(10)
        
    # Build least stops list
        print("Top 10 Least Visited Stops:")
        for i, row in enumerate(least_stops.collect(), 1):
            stop_name = row.stop_name or "Unknown"
            output_line = f"{i:2d}. {str(stop_name)[:50]} ({row.frequency:,} visits)"
            print(output_line)
            results['least_stops'].append({
                'stop_id': row.stop_id,
                'stop_name': stop_name,
                'frequency': row.frequency
            })
        
        # Route statistics
        print("\nAnalyzing route statistics...")
        route_trip_counts = trips.groupBy("route_id").agg(count("*").alias("trip_count"))
        total_routes = route_trip_counts.count()
        avg_trips = route_trip_counts.agg({"trip_count": "avg"}).collect()[0][0]
        
        results['route_stats'] = {
            'total_routes': total_routes,
            'avg_trips_per_route': avg_trips
        }
        
        print("Route Statistics:")
        print(f"- Total unique routes: {total_routes:,}")
        print(f"- Average trips per route: {avg_trips:.1f}")
        
        # Save results locally
        print("\nSaving results locally...")
        save_results_locally(results)
        print("Analytics completed successfully!")
        
    except Exception as e:
        print("Error during analytics: " + str(e))
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()