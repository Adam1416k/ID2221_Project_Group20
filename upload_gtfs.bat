@echo off
echo Uploading GTFS files to HDFS...

set CONTAINER=id2221_project_group20-namenode-1
set HDFS_PATH=/data/gtfs_sverige/2025-09/gtfs/

echo Uploading routes.txt...
docker cp data\raw\2025-09\gtfs\routes.txt %CONTAINER%:/tmp/
docker exec -i %CONTAINER% hdfs dfs -put /tmp/routes.txt %HDFS_PATH%

echo Uploading trips.txt...
docker cp data\raw\2025-09\gtfs\trips.txt %CONTAINER%:/tmp/
docker exec -i %CONTAINER% hdfs dfs -put /tmp/trips.txt %HDFS_PATH%

echo Uploading stop_times.txt...
docker cp data\raw\2025-09\gtfs\stop_times.txt %CONTAINER%:/tmp/
docker exec -i %CONTAINER% hdfs dfs -put /tmp/stop_times.txt %HDFS_PATH%

echo Uploading calendar.txt...
docker cp data\raw\2025-09\gtfs\calendar.txt %CONTAINER%:/tmp/
docker exec -i %CONTAINER% hdfs dfs -put /tmp/calendar.txt %HDFS_PATH%

echo Upload complete! Checking files...
docker exec -i %CONTAINER% hdfs dfs -ls %HDFS_PATH%

echo Done!