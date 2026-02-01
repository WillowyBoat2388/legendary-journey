from pyspark.sql.types import *
from pyspark.sql.functions import *
import argparse

# src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']
def requirements():
    secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
    workspace_name = secret_name.replace("-", "_")
    SCHEMA1  = f"{workspace_name}.landing"


    VOLUME_PATH = f"/Volumes/{workspace_name}/default"

    production_src = f"{VOLUME_PATH}/ong_sensorstream/output"
    destinationlakeCheckpoints = f"{VOLUME_PATH}/checkpoints/landing"
    # outlake = "outlake.landing"

    return production_src, destinationlakeCheckpoints, SCHEMA1
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


