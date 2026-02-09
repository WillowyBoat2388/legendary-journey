from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import argparse
# import os

def requirements(table):
    # Retrieve workspace name from Databricks secret and format it
    secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
    workspace_name = secret_name.replace("-", "_")
    SCHEMA1  = f"{workspace_name}.landing"
    SCHEMA2  = f"{workspace_name}.raw"

    VOLUME_PATH = f"/Volumes/{workspace_name}/default"
    
    sourcezone = SCHEMA1
    destzone = SCHEMA2

    # Define source and destination table names
    source_table = f'{sourcezone}.`{table}`'
    dest_table = f'{destzone}.`{table}`'
    # src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']
    
    # Define checkpoint path and schema contract path
    checkPoint = f"{VOLUME_PATH}/checkpoints/raw/{dest_table}"
    schema_json  = f"../resources/contracts/producer/{table}.json"

    # Load schema from JSON contract
    with open(schema_json, "r") as f:
        loaded_json_data = json.load(f)

    OnGProdContract = StructType.fromJson(loaded_json_data)
    
    return [OnGProdContract, source_table, dest_table, checkPoint]

def run(OnGProdContract, source_table, dest_table, checkPoint):
    # Ingest data from source_table to dest_table using clusterBy on client_id
    print("RAW ZONE SINKING STARTED")

    spark.readStream.schema(OnGProdContract).option("maxFilesPerTrigger", 1).table(source_table).writeStream.outputMode("append").option("checkpointLocation", checkPoint).clusterBy("client_id").trigger(availableNow=True).toTable(dest_table)

    print("RAW ZONE SINKING COMPLETED")

    return

def run2(OnGProdContract, source_table, dest_table, checkPoint):
    # Ingest data from source_table to dest_table using partitionBy on client_id
    print("RAW ZONE SINKING STARTED")

    spark.readStream.schema(OnGProdContract).option("maxFilesPerTrigger", 1).table(source_table).writeStream.outputMode("append").option("checkpointLocation", checkPoint).partitionBy("client_id").trigger(availableNow=True).toTable(dest_table)

    print("RAW ZONE SINKING COMPLETED")

    return

if __name__ == "__main__":
    # Parse command line argument for table name
    parser = argparse.ArgumentParser(
                    description='Ingest Stream Data')
    parser.add_argument('input',
                    type=str,
                    help = "The type of event data coming in")
    
    args = parser.parse_args()
    table = args.input
    
    req = requirements(table)
    # Use run2 for 'well-telemetry', otherwise use run
    if table == 'well-telemetry':
        run2(req[0], req[1], req[2], req[3])
    else:
        run(req[0], req[1], req[2], req[3])
    