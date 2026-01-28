from pyspark.sql.types import *
from pyspark.sql.functions import *
import argparse

# src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']
def requirements():
    
    
    # spark.sql("""
    # CREATE EXTERNAL VOLUME IF NOT EXISTS ong_streamworkspace_37859.default.ong_sensorstream
    # LOCATION 'abfss://analyticscontainer@analyticsstorage37859.dfs.core.windows.net/analytics';
    # CREATE VOLUME IF NOT EXISTS ong_streamworkspace_37859.default.checkpoints;
    # CREATE SCHEMA IF NOT EXISTS ong_streamworkspace_37859.landing;
    # CREATE SCHEMA IF NOT EXISTS ong_streamworkspace_37859.raw;
    # """)
    

    production_src = "/Volumes/ong_streamworkspace_37859/default/ong_sensorstream/output"
    destinationlakeCheckpoints = "/Volumes/ong_streamworkspace_37859/default/checkpoints/landing"
    outlake = "outlake.landing"

    return production_src, destinationlakeCheckpoints, outlake
def telemetry_dump(production_source, source, checkPoints, outlake):
    print(f"{source.upper()} LANDINGZONE SINK")
    files_path = f'{production_source}/{source}'
    src_inferredSchema = f'{files_path}/_checkpoint'
    dest_checkpointpath = f'{checkPoints}/{source}'
    outputPath = f"{outlake}.`{source}`"

    print("Streaming Begins")


    # Write stream to table
    spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", src_inferredSchema) \
        .load(files_path).writeStream \
    .outputMode("append") \
    .option("checkpointLocation", dest_checkpointpath) \
    .trigger(availableNow=True) \
    .toTable(outputPath)
    print("LANDING ZONE SINKING COMPLETED")

    return outputPath




if __name__ == "__main__":

    production_src, destinationlakeCheckpoints, outlake = requirements()

    parser = argparse.ArgumentParser(
                    description='Ingest Stream Data')
    parser.add_argument('input',
                    type=str,
                    help = "The type of event data coming in")

                    
    args = parser.parse_args()
    source = args.input
    telemetry_dump(production_src, source, destinationlakeCheckpoints, outlake)


