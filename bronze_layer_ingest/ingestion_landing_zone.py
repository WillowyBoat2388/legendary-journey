from pyspark.sql.types import *
from pyspark.sql.functions import *
import argparse

# src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']

def requirements():
    # Retrieve workspace name from Databricks secret and format it for schema/volume usage
    secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
    workspace_name = secret_name.replace("-", "_")
    SCHEMA1  = f"{workspace_name}.landing"

    # Define base volume path
    VOLUME_PATH = f"/Volumes/{workspace_name}/default"

    # Define source and checkpoint paths for production data
    production_src = f"{VOLUME_PATH}/ong_sensorstream/output"
    destinationlakeCheckpoints = f"{VOLUME_PATH}/checkpoints/landing"
    # outlake = "outlake.landing"

    return production_src, destinationlakeCheckpoints, SCHEMA1

def telemetry_dump(production_source, source, checkPoints, outlake):
    # Print which source is being processed
    print(f"{source.upper()} LANDINGZONE SINK")
    files_path = f'{production_source}/{source}'
    src_inferredSchema = f'{files_path}/_checkpoint'
    dest_checkpointpath = f'{checkPoints}/{source}'
    outputPath = f"{outlake}.`{source}`"

    print("Streaming Begins")

    # Read streaming data from cloudFiles (parquet), infer schema, and write to Delta table with checkpointing
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

    # Get required paths and schema
    production_src, destinationlakeCheckpoints, outlake = requirements()

    # Parse command-line argument for source type
    parser = argparse.ArgumentParser(
                    description='Ingest Stream Data')
    parser.add_argument('input',
                    type=str,
                    help = "The type of event data coming in")

    args = parser.parse_args()
    source = args.input

    # Start telemetry data ingestion
    telemetry_dump(production_src, source, destinationlakeCheckpoints, outlake)