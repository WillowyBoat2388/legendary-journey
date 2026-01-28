from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
import argparse

def requirements(table):
    spark.sql(""" CREATE EXTERNAL VOLUME IF NOT EXISTS ong_streamworkspace_37859.default.resources
    LOCATION 'abfss://analyticscontainer@analyticsstorage37859.dfs.core.windows.net/resources';""")
    
    sourcezone = "outlake.landing"
    destzone = "outlake.raw"

    source_table = f'{sourcezone}.`{table}`'
    dest_table = f'{destzone}.`{table}`'
    # src_list = ['production-daily-data', 'reservoir', 'equipment-events', 'wellbore', 'facility-telemetry', 'well-telemetry']
    
    
    checkPoint = f"/Volumes/ong_streamworkspace_37859/default/checkpoints/raw/{dest_table}"
    schema_json  = f"/Volumes/ong_streamworkspace_37859/default/resources/contracts/producer/{table}.json"

    with open(schema_json, "r") as f:
        loaded_json_data = json.load(f)

    OnGProdContract = StructType.fromJson(loaded_json_data)
    
    return [OnGProdContract, source_table, dest_table, checkPoint]

def run(OnGProdContract, source_table, dest_table, checkPoint):
    
    print("RAW ZONE SINKING STARTED")

    spark.readStream.schema(OnGProdContract).option("maxFilesPerTrigger", 1).table(source_table).writeStream.outputMode("append").option("checkpointLocation", checkPoint).clusterBy("client_id").trigger(availableNow=True).toTable(dest_table)


    print("RAW ZONE SINKING COMPLETED")

    return

def run2(OnGProdContract, source_table, dest_table, checkPoint):
    
    print("RAW ZONE SINKING STARTED")

    spark.readStream.schema(OnGProdContract).option("maxFilesPerTrigger", 1).table(source_table).writeStream.outputMode("append").option("checkpointLocation", checkPoint).partitionBy("client_id").trigger(availableNow=True).toTable(dest_table)


    print("RAW ZONE SINKING COMPLETED")

    return

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
                    description='Ingest Stream Data')
    parser.add_argument('input',
                    type=str,
                    help = "The type of event data coming in")
    
    args = parser.parse_args()
    table = args.input
    
    req = requirements(table)
    if table == 'well-telemetry':
        run2(req[0], req[1], req[2], req[3])
    else:
        run(req[0], req[1], req[2], req[3])

    