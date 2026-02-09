from pyspark.sql.types import *
from pyspark.sql.functions import *

# Get the workspace name from Databricks secrets and format it for schema/volume usage
secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1  = f"{workspace_name}.raw"
SCHEMA2  = f"{workspace_name}.base"

VOLUME_PATH = f"/Volumes/{workspace_name}/default"

sourcezone = SCHEMA1
destzone = SCHEMA2

# Define source and destination table names
source_table1 = f'{sourcezone}.`well-telemetry`'
checkPoint = f'{VOLUME_PATH}/checkpoints/base/lease_test'
dest_table = f'{destzone}.lease'

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(df, batchId):
  # Truncate timestamp to second granularity
  df = df.withColumn("timestamp", date_trunc("second", "timestamp"))
  # Pivot the dataframe to transform sensor readings into columns
  result_df = (df.groupBy("client_id", "well_id", "timestamp", "sensor_id")
    .pivot("col_name")
    .agg(first("value")))
  print("batch inserts ongoing")

  # Get distinct rows with the columns you need for join
  additional_cols = df.select("client_id", "well_id", "sensor_id", "timestamp", "location", "status", "quality").distinct()

  # Join pivoted sensor readings with additional columns
  out_df = result_df.join(additional_cols, ["client_id", "well_id", "sensor_id", "timestamp"], "left")
  # Write the result to the Delta table with schema merging enabled
  (out_df.write
          .format("delta")
          .option("mergeSchema", "true")
          .mode("append")
          .saveAsTable(dest_table))
  return

print("Ready to begin pushing for each batch")
     
# Read streaming data from the source table, transform, and write using foreachBatch
(
    spark.readStream.table(source_table1)
        .withColumn("timestamp", to_timestamp("timestamp"))  # Ensure timestamp is in correct format
        .withColumn("col_name", concat_ws("_", "sensor_type", "unit"))  # Create a column name for pivot
        .writeStream
        .option("checkpointLocation", checkPoint)  # Set checkpoint location for streaming
        .foreachBatch(upsertToDelta)  # Use custom upsert function for each batch
        .trigger(availableNow=True)  # Trigger available data now
        .start()
)