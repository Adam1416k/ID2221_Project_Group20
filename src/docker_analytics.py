"""
GTFS analytics for Docker Spark environment with local results output
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, desc, asc, avg, lit, col
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from download_data import download_data, download_period, cleanup_downloaded_data


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

        f.write(f"Analysis Period: {results_dict['period']['start_date']} to {results_dict['period']['end_date']}\n")
        f.write(f"Number of days: {results_dict['period']['num_days']}\n")
        f.write("\n")

        f.write("Dataset Summary:\n")
        f.write(f"- Agency records: {results_dict['counts']['agency']:,}\n")
        f.write(f"- Routes records: {results_dict['counts']['routes']:,}\n")
        f.write(f"- Stops records: {results_dict['counts']['stops']:,}\n")
        f.write(f"- Trips records: {results_dict['counts']['trips']:,}\n")
        f.write(f"- Stop times records: {results_dict['counts']['stop_times']:,}\n")
        f.write("\n")

        # Top stops
        f.write("Top 30 Most Visited Stops (Average per Day):\n")
        f.write("-" * 50 + "\n")
        for i, row in enumerate(results_dict['top_stops'], 1):
            stop_name = row['stop_name'] or "Unknown"
            f.write(f"{i:2d}. {stop_name[:50]} ({row['avg_visits_per_day']:.1f} avg visits/day)\n")
        f.write("\n")

        # Least visited stops
        f.write("Top 30 Least Visited Stops (Average per Day):\n")
        f.write("-" * 50 + "\n")
        for i, row in enumerate(results_dict['least_stops'], 1):
            stop_name = row['stop_name'] or "Unknown"
            f.write(f"{i:2d}. {stop_name[:50]} ({row['avg_visits_per_day']:.1f} avg visits/day)\n")
        f.write("\n")

        # Route stats
        f.write("Route Statistics:\n")
        f.write("-" * 30 + "\n")
        f.write(f"- Total unique routes: {results_dict['route_stats']['total_routes']:,}\n")
        f.write(f"- Average trips per route: {results_dict['route_stats']['avg_trips_per_route']:.1f}\n")
    
    # Also store raw JSON to a separate file
    json_out_path = os.path.join(output_dir, f"gtfs_summary_{timestamp}.json")
    import json
    with open(json_out_path, "w", encoding="utf-8") as jf:
        json.dump(results_dict, jf, ensure_ascii=False, indent=4)

    print(f"Results saved to: {out_path} and {json_out_path}")
    return out_path

def main():
    # Create Spark session
    spark = SparkSession.builder.appName("GTFS_Analytics_Docker").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]
    
    print("Starting GTFS Analytics with Docker Spark")
    print("Python runtime:", sys.version)

    month = "10"
    start_date, end_date = "2025-10-01", "2025-10-14"
    base_url = f"https://data.samtrafiken.se/trafiklab/gtfs-sverige-2/2025/{month}"
    output_dir = f"../data/raw/sweden-2025-{month}"

    # Download data for the period
    print(f"\nDownloading data from {start_date} to {end_date}...")
    download_period(start_date, end_date, base_url, output_dir)

    try:
        # Calculate number of days in the period
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        num_days = (end_dt - start_dt).days + 1
        
        print(f"\nLoading GTFS data for {num_days} days...")
        
        # Load data from all days in the period
        all_agency: Optional[DataFrame] = None
        all_routes: Optional[DataFrame] = None
        all_stops: Optional[DataFrame] = None
        all_trips: Optional[DataFrame] = None
        all_stop_times: Optional[DataFrame] = None
        
        current_date = start_dt
        day_count = 0
        
        while current_date <= end_dt:
            date_str = current_date.strftime("%Y%m%d")
            day_path = os.path.join(output_dir, f"sweden-{date_str}")
            
            print(f"Loading data for {date_str}...")
            
            # Read data for this day
            agency_day = spark.read.csv(day_path + "/agency.txt", header=True)
            routes_day = spark.read.csv(day_path + "/routes.txt", header=True)
            stops_day = spark.read.csv(day_path + "/stops.txt", header=True)
            trips_day = spark.read.csv(day_path + "/trips.txt", header=True)
            stop_times_day = spark.read.csv(day_path + "/stop_times.txt", header=True).withColumn("date", lit(date_str))
            
            # Union with accumulated data
            if all_agency is None:
                all_agency = agency_day
                all_routes = routes_day
                all_stops = stops_day
                all_trips = trips_day
                all_stop_times = stop_times_day
            else:
                # Type assertions for the else block
                assert all_routes is not None
                assert all_stops is not None
                assert all_trips is not None
                assert all_stop_times is not None
                
                all_agency = all_agency.union(agency_day)
                all_routes = all_routes.union(routes_day)
                all_stops = all_stops.union(stops_day)
                all_trips = all_trips.union(trips_day)
                all_stop_times = all_stop_times.union(stop_times_day)
            
            day_count += 1
            current_date += timedelta(days=1)
        
        print(f"Data loaded successfully for {day_count} days!")
        
        # Type assertions - these are guaranteed to be DataFrames after the loop
        assert all_agency is not None
        assert all_routes is not None
        assert all_stops is not None
        assert all_trips is not None
        assert all_stop_times is not None
        
        # Collect results dictionary
        results: Dict[str, Any] = {
            'period': {
                'start_date': start_date,
                'end_date': end_date,
                'num_days': num_days
            },
            'counts': {
                'agency': all_agency.count(),
                'routes': all_routes.count(),
                'stops': all_stops.count(),
                'trips': all_trips.count(),
                'stop_times': all_stop_times.count()
            },
            'top_stops': [],
            'least_stops': [],
            'route_stats': {}
        }
        
        print("\nDataset Summary:")
        print(f"Period: {start_date} to {end_date} ({num_days} days)")
        for key, value in results['counts'].items():
            print(f"{key.title()}: {value:,}")
        
        # Analyze top stops (most visited on average per day)
        print("\nAnalyzing most visited stops (average per day)...")
        top_stops = all_stop_times.groupBy("stop_id") \
            .agg(count("*").alias("total_frequency")) \
            .withColumn("avg_frequency", col("total_frequency") / num_days) \
            .join(all_stops.dropDuplicates(["stop_id"]), "stop_id", "left") \
            .select("stop_id", "stop_name", "total_frequency", "avg_frequency") \
            .orderBy(desc("total_frequency")) \
            .limit(30)
        
        # Build top stops list
        print("Top 30 Most Visited Stops (Average per Day):")
        for i, row in enumerate(top_stops.collect(), 1):
            stop_name = row.stop_name or "Unknown"
            avg_visits = row.total_frequency / num_days
            output_line = f"{i:2d}. {str(stop_name)[:50]} ({avg_visits:.1f} avg visits/day, {row.total_frequency:,} total)"
            print(output_line)
            results['top_stops'].append({
                'stop_id': row.stop_id,
                'stop_name': stop_name,
                'total_visits': row.total_frequency,
                'avg_visits_per_day': avg_visits
            })
        
        # Analyze least visited stops (average per day)
        print("\nAnalyzing least visited stops (average per day)...")
        least_stops = all_stop_times.groupBy("stop_id") \
            .agg(count("*").alias("total_frequency")) \
            .withColumn("avg_frequency", col("total_frequency") / num_days) \
            .join(all_stops.dropDuplicates(["stop_id"]), "stop_id", "left") \
            .select("stop_id", "stop_name", "total_frequency", "avg_frequency") \
            .orderBy(asc("total_frequency")) \
            .limit(30)
        
        # Build least stops list
        print("Top 30 Least Visited Stops (Average per Day):")
        for i, row in enumerate(least_stops.collect(), 1):
            stop_name = row.stop_name or "Unknown"
            avg_visits = row.total_frequency / num_days
            output_line = f"{i:2d}. {str(stop_name)[:50]} ({avg_visits:.1f} avg visits/day, {row.total_frequency:,} total)"
            print(output_line)
            results['least_stops'].append({
                'stop_id': row.stop_id,
                'stop_name': stop_name,
                'total_visits': row.total_frequency,
                'avg_visits_per_day': avg_visits
            })
        
        # Route statistics
        print("\nAnalyzing route statistics...")
        route_trip_counts = all_trips.groupBy("route_id").agg(count("*").alias("trip_count"))
        total_routes = route_trip_counts.count()
        avg_trips = route_trip_counts.agg({"trip_count": "avg"}).collect()[0][0]
        one_stop_routes = route_trip_counts.filter(route_trip_counts.trip_count == 1).count()
        
        results['route_stats'] = {
            'total_routes': total_routes,
            'avg_trips_per_route': avg_trips,
            'one_stop_routes': one_stop_routes
        }
        
        print("Route Statistics:")
        print(f"- Total unique routes: {total_routes:,}")
        print(f"- Average trips per route: {avg_trips:.1f}")
        print(f"- Routes with only one stop: {one_stop_routes:,}")
        
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
        cleanup_downloaded_data(output_dir)

if __name__ == "__main__":
    main()