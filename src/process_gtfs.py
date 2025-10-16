import sys
from pyspark.sql import SparkSession, functions as F, types as T
from utils import ensure_env, GTFS_YEAR, GTFS_MONTH, HDFS_URI, HDFS_BASE

def spark():
    return (SparkSession.builder
            .appName("GTFS-Clean-Normalize")
            .getOrCreate())

def base_paths(year, month):
    raw = f"{HDFS_URI}{HDFS_BASE}/{year}-{month}/gtfs"
    curated = f"{HDFS_URI}{HDFS_BASE}/{year}-{month}/curated"
    return raw, curated

def read_csv(spark, path):
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "false")
            .csv(path))

def clean_stops(df):
    # GTFS: stop_id, stop_name, stop_lat, stop_lon, location_type, parent_station, ...
    return (df
        .withColumn("stop_id", F.col("stop_id").cast("string"))
        .withColumn("stop_name", F.trim(F.col("stop_name")))
        .withColumn("stop_lat", F.col("stop_lat").cast("double"))
        .withColumn("stop_lon", F.col("stop_lon").cast("double"))
        .withColumn("location_type", F.col("location_type").cast("int"))
        .withColumn("parent_station", F.col("parent_station").cast("string"))
        .dropna(subset=["stop_id", "stop_name"])
    )

def clean_routes(df):
    # GTFS: route_id, agency_id, route_short_name, route_long_name, route_type, ...
    return (df
        .withColumn("route_id", F.col("route_id").cast("string"))
        .withColumn("route_short_name", F.trim(F.col("route_short_name")))
        .withColumn("route_long_name", F.trim(F.col("route_long_name")))
        .withColumn("route_type", F.col("route_type").cast("int"))
        .dropna(subset=["route_id"])
    )

def clean_trips(df):
    # GTFS: route_id, service_id, trip_id, trip_headsign, direction_id, ...
    return (df
        .withColumn("route_id", F.col("route_id").cast("string"))
        .withColumn("service_id", F.col("service_id").cast("string"))
        .withColumn("trip_id", F.col("trip_id").cast("string"))
        .withColumn("trip_headsign", F.trim(F.col("trip_headsign")))
        .withColumn("direction_id", F.col("direction_id").cast("int"))
        .dropna(subset=["trip_id", "route_id", "service_id"])
    )

def clean_stop_times(df):
    # GTFS: trip_id, arrival_time, departure_time, stop_id, stop_sequence, ...
    # Times in GTFS can be 24+ hours (e.g., 25:10:00). Keep as string; parse later if needed.
    return (df
        .withColumn("trip_id", F.col("trip_id").cast("string"))
        .withColumn("stop_id", F.col("stop_id").cast("string"))
        .withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
        .withColumn("arrival_time", F.trim(F.col("arrival_time")))
        .withColumn("departure_time", F.trim(F.col("departure_time")))
        .dropna(subset=["trip_id", "stop_id", "stop_sequence"])
    )

def clean_calendar(df):
    # GTFS: service_id, monday..sunday (0/1), start_date, end_date (YYYYMMDD)
    return (df
        .withColumn("service_id", F.col("service_id").cast("string"))
        .withColumn("start_date", F.col("start_date").cast("string"))
        .withColumn("end_date", F.col("end_date").cast("string"))
    )

def write_parquet(df, path, name, mode="overwrite"):
    df.write.mode(mode).parquet(f"{path}/{name}")

def main():
    ensure_env()
    year, month = GTFS_YEAR, GTFS_MONTH
    raw, curated = base_paths(year, month)
    s = spark()

    # Read raw GTFS CSVs from HDFS
    stops = read_csv(s, f"{raw}/stops.txt")
    routes = read_csv(s, f"{raw}/routes.txt")
    trips = read_csv(s, f"{raw}/trips.txt")
    stop_times = read_csv(s, f"{raw}/stop_times.txt")

    # calendar is optional but usually present
    try:
        calendar = read_csv(s, f"{raw}/calendar.txt")
    except Exception:
        calendar = None

    # Clean
    stops_c = clean_stops(stops)
    routes_c = clean_routes(routes)
    trips_c = clean_trips(trips)
    stop_times_c = clean_stop_times(stop_times)
    if calendar is not None:
        calendar_c = clean_calendar(calendar)

    # Write curated parquet tables
    write_parquet(stops_c, curated, "stops")
    write_parquet(routes_c, curated, "routes")
    write_parquet(trips_c, curated, "trips")
    write_parquet(stop_times_c, curated, "stop_times")
    if calendar is not None:
        write_parquet(calendar_c, curated, "calendar")

        
    print(f"Results written to: {curated}")
    print("Processing complete. Curated parquet tables written to:", curated)

if __name__ == "__main__":
    main()
