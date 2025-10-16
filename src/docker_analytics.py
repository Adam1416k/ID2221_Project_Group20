"""
GTFS analytics for Docker Spark environment with local results output
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, asc
import os
import sys
import codecs
from datetime import datetime

def safe_str_for_file(s):
    """Convert string to safe ASCII for file writing - Python 2/3 compatible"""
    if s is None:
        return "Unknown"
    # In Python 2, unicode strings need special handling
    if sys.version_info[0] == 2:
        try:
            if isinstance(s, unicode):
                return s.encode('ascii', 'replace').decode('ascii')
        except NameError:
            # unicode not defined, treat as string
            pass
    # For Python 3 or Python 2 str objects
    try:
        return s.encode('ascii', 'replace').decode('ascii')
    except (UnicodeDecodeError, AttributeError):
        return str(s)

def save_results_locally(results_dict, output_dir="/data/analytics_results"):
    """Save analytics results to local files"""
    # Create output directory if it doesn't exist (Python 2 compatible)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save summary with proper UTF-8 encoding for Swedish characters (Python 2/3 compatible)
    import codecs
    with codecs.open(os.path.join(output_dir, "gtfs_summary_{}.txt".format(timestamp)), "w", encoding='utf-8') as f:
        f.write("GTFS Analytics Summary - {}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        f.write("=" * 50 + "\n\n")
        
        f.write("Dataset Summary:\n")
        f.write("- Agency records: {:,}\n".format(results_dict['counts']['agency']))
        f.write("- Routes records: {:,}\n".format(results_dict['counts']['routes']))
        f.write("- Stops records: {:,}\n".format(results_dict['counts']['stops']))
        f.write("- Trips records: {:,}\n".format(results_dict['counts']['trips']))
        f.write("- Stop times records: {:,}\n".format(results_dict['counts']['stop_times']))
        f.write("\n")
        
        # Top stops
        f.write("Top 10 Most Visited Stops:\n")
        f.write("-" * 30 + "\n")
        for i, row in enumerate(results_dict['top_stops'], 1):
            stop_name = safe_str_for_file(row['stop_name'])
            f.write("{:2d}. {} ({:,} visits)\n".format(i, stop_name[:50], row['frequency']))
        f.write("\n")
        
        # Least visited stops
        f.write("Top 10 Least Visited Stops:\n")
        f.write("-" * 30 + "\n")
        for i, row in enumerate(results_dict['least_stops'], 1):
            stop_name = safe_str_for_file(row['stop_name'])
            f.write("{:2d}. {} ({:,} visits)\n".format(i, stop_name[:50], row['frequency']))
        f.write("\n")
        
        # Route stats
        f.write("Route Statistics:\n")
        f.write("-" * 30 + "\n")
        f.write("- Total unique routes: {:,}\n".format(results_dict['route_stats']['total_routes']))
        f.write("- Average trips per route: {:.1f}\n".format(results_dict['route_stats']['avg_trips_per_route']))
    
    print("Results saved to: {}".format(os.path.join(output_dir, "gtfs_summary_{}.txt".format(timestamp))))
    return os.path.join(output_dir, "gtfs_summary_{}.txt".format(timestamp))

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("GTFS_Analytics_Docker") \
        .getOrCreate()
    
    print("Starting GTFS Analytics with Docker Spark")
    
    hdfs_path = "/data/raw/sweden-20250930"
    
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
        results = {
            'counts': {
                'agency': agency.count(),
                'routes': routes.count(),
                'stops': stops.count(),
                'trips': trips.count(),
                'stop_times': stop_times.count()
            }
        }
        
        print("\nDataset Summary:")
        for key, value in results['counts'].items():
            print("{}: {:,}".format(key.title(), value))
        
        # Analyze top stops (most visited)
        print("\nAnalyzing most visited stops...")
        top_stops = stop_times.groupBy("stop_id") \
            .agg(count("*").alias("frequency")) \
            .join(stops, "stop_id", "left") \
            .select("stop_id", "stop_name", "frequency") \
            .orderBy(desc("frequency")) \
            .limit(10)
        
        results['top_stops'] = []
        print("Top 10 Most Visited Stops:")
        for i, row in enumerate(top_stops.collect(), 1):
            stop_name = row.stop_name if row.stop_name else "Unknown"
            # Handle Swedish characters properly - convert to safe ASCII representation
            try:
                import sys
                if sys.version_info[0] == 2:
                    # Python 2
                    if isinstance(stop_name, unicode):
                        stop_name_safe = stop_name[:50].encode('ascii', 'replace').decode('ascii')
                    else:
                        stop_name_safe = str(stop_name[:50])
                else:
                    # Python 3
                    stop_name_safe = str(stop_name[:50]).encode('ascii', 'replace').decode('ascii')
            except:
                stop_name_safe = "Unknown_Stop"
            
            output_line = "{:2d}. {} ({:,} visits)".format(i, stop_name_safe, row.frequency)
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
        
        results['least_stops'] = []
        print("Top 10 Least Visited Stops:")
        for i, row in enumerate(least_stops.collect(), 1):
            stop_name = row.stop_name if row.stop_name else "Unknown"
            # Handle Swedish characters properly - convert to safe ASCII representation
            try:
                import sys
                if sys.version_info[0] == 2:
                    # Python 2
                    if isinstance(stop_name, unicode):
                        stop_name_safe = stop_name[:50].encode('ascii', 'replace').decode('ascii')
                    else:
                        stop_name_safe = str(stop_name[:50])
                else:
                    # Python 3
                    stop_name_safe = str(stop_name[:50]).encode('ascii', 'replace').decode('ascii')
            except Exception:
                stop_name_safe = "Unknown_Stop"
            
            output_line = "{:2d}. {} ({:,} visits)".format(i, stop_name_safe, row.frequency)
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
        print("- Total unique routes: {:,}".format(total_routes))
        print("- Average trips per route: {:.1f}".format(avg_trips))
        
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