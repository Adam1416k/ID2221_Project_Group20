"""
Local file-based GTFS analytics using PySpark.
Runs analytics on local GTFS files without HDFS dependency.
"""
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    desc, asc, hour, dayofweek, when, lit, round as spark_round,
    unix_timestamp, from_unixtime, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def create_spark_session():
    """Create Spark session for local GTFS analytics"""
    return SparkSession.builder \
        .appName("GTFS_Local_Analytics") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

def load_gtfs_data_local(spark, local_path):
    """Load GTFS CSV files from local directory into Spark DataFrames"""
    
    # Define schemas for better performance
    agency_schema = StructType([
        StructField("agency_id", StringType(), True),
        StructField("agency_name", StringType(), True),
        StructField("agency_url", StringType(), True),
        StructField("agency_timezone", StringType(), True),
        StructField("agency_lang", StringType(), True),
        StructField("agency_phone", StringType(), True)
    ])
    
    routes_schema = StructType([
        StructField("route_id", StringType(), True),
        StructField("agency_id", StringType(), True),
        StructField("route_short_name", StringType(), True),
        StructField("route_long_name", StringType(), True),
        StructField("route_type", IntegerType(), True),
        StructField("route_color", StringType(), True),
        StructField("route_text_color", StringType(), True)
    ])
    
    stops_schema = StructType([
        StructField("stop_id", StringType(), True),
        StructField("stop_code", StringType(), True),
        StructField("stop_name", StringType(), True),
        StructField("stop_desc", StringType(), True),
        StructField("stop_lat", DoubleType(), True),
        StructField("stop_lon", DoubleType(), True),
        StructField("zone_id", StringType(), True),
        StructField("stop_url", StringType(), True),
        StructField("location_type", IntegerType(), True),
        StructField("parent_station", StringType(), True),
        StructField("wheelchair_boarding", IntegerType(), True),
        StructField("platform_code", StringType(), True)
    ])
    
    trips_schema = StructType([
        StructField("route_id", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("trip_id", StringType(), True),
        StructField("trip_headsign", StringType(), True),
        StructField("trip_short_name", StringType(), True),
        StructField("direction_id", IntegerType(), True),
        StructField("block_id", StringType(), True),
        StructField("shape_id", StringType(), True),
        StructField("wheelchair_accessible", IntegerType(), True),
        StructField("bikes_allowed", IntegerType(), True)
    ])
    
    stop_times_schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("arrival_time", StringType(), True),
        StructField("departure_time", StringType(), True),
        StructField("stop_id", StringType(), True),
        StructField("stop_sequence", IntegerType(), True),
        StructField("stop_headsign", StringType(), True),
        StructField("pickup_type", IntegerType(), True),
        StructField("drop_off_type", IntegerType(), True),
        StructField("shape_dist_traveled", DoubleType(), True),
        StructField("timepoint", IntegerType(), True)
    ])
    
    print(f"Loading GTFS data from {local_path}")
    
    # Load DataFrames from local files
    dfs = {}
    local_path = Path(local_path)
    
    try:
        # Required files
        dfs['agency'] = spark.read.csv(
            str(local_path / "agency.txt"), 
            header=True, 
            schema=agency_schema
        )
        
        dfs['routes'] = spark.read.csv(
            str(local_path / "routes.txt"), 
            header=True, 
            schema=routes_schema
        )
        
        dfs['stops'] = spark.read.csv(
            str(local_path / "stops.txt"), 
            header=True, 
            schema=stops_schema
        )
        
        dfs['trips'] = spark.read.csv(
            str(local_path / "trips.txt"), 
            header=True, 
            schema=trips_schema
        )
        
        dfs['stop_times'] = spark.read.csv(
            str(local_path / "stop_times.txt"), 
            header=True, 
            schema=stop_times_schema
        )
        
        # Optional files
        try:
            dfs['calendar'] = spark.read.csv(str(local_path / "calendar.txt"), header=True)
        except Exception:
            print("WARNING: calendar.txt not found or could not be loaded")
            
        try:
            dfs['calendar_dates'] = spark.read.csv(str(local_path / "calendar_dates.txt"), header=True)
        except Exception:
            print("WARNING: calendar_dates.txt not found or could not be loaded")
        
        print("Successfully loaded GTFS data")
        
        # Show data summary
        for name, df in dfs.items():
            count = df.count()
            print(f"  - {name}: {count:,} records")
            
        return dfs
        
    except Exception as e:
        print(f"ERROR: Error loading GTFS data: {e}")
        return None

def analyze_stop_frequency_local(dfs, output_dir):
    """Analyze stop frequency - busiest and least busy stops"""
    print("\nAnalyzing Stop Frequency...")
    
    stop_times = dfs['stop_times']
    stops = dfs['stops']
    
    # Count visits per stop
    stop_frequency = stop_times \
        .groupBy("stop_id") \
        .agg(count("*").alias("visit_count")) \
        .join(stops, "stop_id") \
        .select("stop_id", "stop_name", "visit_count", "stop_lat", "stop_lon")
    
    # Cache for multiple operations
    stop_frequency.cache()
    
    # Top 20 busiest stops
    print("\nTop 20 Busiest Stops:")
    busiest_stops = stop_frequency.orderBy(desc("visit_count")).limit(20)
    busiest_stops.show()
    
    # Save busiest stops
    busiest_stops.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/busiest_stops", header=True)
    
    # Bottom 20 least busy stops (excluding 0 visits)
    print("\nTop 20 Least Busy Stops (with at least 1 visit):")
    least_busy_stops = stop_frequency \
        .filter(col("visit_count") > 0) \
        .orderBy(asc("visit_count")) \
        .limit(20)
    least_busy_stops.show()
    
    # Save least busy stops
    least_busy_stops.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/least_busy_stops", header=True)
    
    # Statistics
    stats = stop_frequency.agg(
        count("*").alias("total_stops"),
        spark_sum("visit_count").alias("total_visits"),
        avg("visit_count").alias("avg_visits_per_stop"),
        spark_max("visit_count").alias("max_visits"),
        spark_min("visit_count").alias("min_visits")
    ).collect()[0]
    
    print("\nStop Frequency Statistics:")
    print(f"  - Total stops with visits: {stats['total_stops']:,}")
    print(f"  - Total visits: {stats['total_visits']:,}")
    print(f"  - Average visits per stop: {stats['avg_visits_per_stop']:.2f}")
    print(f"  - Max visits (busiest): {stats['max_visits']:,}")
    print(f"  - Min visits: {stats['min_visits']:,}")
    
    # Save all stop frequency data
    stop_frequency.write.mode("overwrite").csv(f"{output_dir}/stop_frequency", header=True)
    
    return {
        'busiest_stops': busiest_stops,
        'least_busy_stops': least_busy_stops,
        'stop_frequency': stop_frequency
    }

def analyze_route_performance_local(dfs, output_dir):
    """Analyze route performance and statistics"""
    print("\nAnalyzing Route Performance...")
    
    routes = dfs['routes']
    trips = dfs['trips']
    stop_times = dfs['stop_times']
    
    # Route statistics
    route_stats = trips \
        .join(routes, "route_id") \
        .join(stop_times, "trip_id") \
        .groupBy("route_id", "route_short_name", "route_long_name", "route_type") \
        .agg(
            count("trip_id").alias("total_stops"),
            spark_sum(lit(1)).alias("trip_count")
        )
    
    # Top routes by trip count
    print("\nTop 20 Routes by Trip Count:")
    top_routes = route_stats.orderBy(desc("trip_count")).limit(20)
    top_routes.show(truncate=False)
    
    # Save route stats
    route_stats.write.mode("overwrite").csv(f"{output_dir}/route_stats", header=True)
    
    return {'route_stats': route_stats}

def analyze_time_patterns_local(dfs, output_dir):
    """Analyze temporal patterns in transit data"""
    print("\nAnalyzing Time Patterns...")
    
    stop_times = dfs['stop_times']
    
    # Clean and convert arrival times
    cleaned_stop_times = stop_times \
        .filter(col("arrival_time").isNotNull()) \
        .withColumn("hour_raw", 
                   regexp_replace(col("arrival_time"), r"(\d+):(\d+):(\d+)", r"$1").cast(IntegerType())) \
        .withColumn("hour_normalized", 
                   when(col("hour_raw") >= 24, col("hour_raw") - 24).otherwise(col("hour_raw")))
    
    # Hourly distribution
    hourly_pattern = cleaned_stop_times \
        .groupBy("hour_normalized") \
        .agg(count("*").alias("trip_count")) \
        .orderBy("hour_normalized")
    
    print("\nHourly Trip Distribution:")
    hourly_pattern.show(24)
    
    # Save hourly patterns
    hourly_pattern.coalesce(1).write.mode("overwrite").csv(f"{output_dir}/hourly_patterns", header=True)
    
    return {'hourly_pattern': hourly_pattern}

def main():
    """Main function for local GTFS analytics"""
    if len(sys.argv) < 2:
        print("Local GTFS Analytics")
        print("Usage: python local_gtfs_analytics.py <path_to_gtfs_directory> [output_directory]")
        print("Example: python local_gtfs_analytics.py data/raw/2025-09/gtfs results")
        sys.exit(1)
    
    local_gtfs_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "analytics_results"
    
    print("Starting Local GTFS Analytics with PySpark")
    
    # Create output directory
    Path(output_dir).mkdir(exist_ok=True)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # Reduce logging verbosity
    
    try:
        print(f"Input path: {local_gtfs_path}")
        print(f"Output path: {output_dir}")
        
        # Load GTFS data
        dfs = load_gtfs_data_local(spark, local_gtfs_path)
        if not dfs:
            print("ERROR: Failed to load GTFS data")
            return
        
        # Perform analytics
        results = {}
        
        # Stop frequency analysis
        stop_results = analyze_stop_frequency_local(dfs, output_dir)
        results.update(stop_results)
        
        # Route performance analysis
        route_results = analyze_route_performance_local(dfs, output_dir)
        results.update(route_results)
        
        # Time pattern analysis
        time_results = analyze_time_patterns_local(dfs, output_dir)
        results.update(time_results)
        
        print("\nAnalytics completed successfully!")
        print(f"Results saved to: {output_dir}")
        print("\nGenerated files:")
        print("  - busiest_stops/")
        print("  - least_busy_stops/")
        print("  - stop_frequency/")
        print("  - route_stats/")
        print("  - hourly_patterns/")
        
    except Exception as e:
        print(f"ERROR: Error during analytics: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()