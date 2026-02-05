from pyspark.sql.types import *
from pyspark.sql.functions import *

# spark.sql("DROP TABLE IF EXISTS base.lease")

secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1  = f"{workspace_name}.raw"
SCHEMA2  = f"{workspace_name}.base"

VOLUME_PATH = f"/Volumes/{workspace_name}/default"

sourcezone = SCHEMA1
destzone = SCHEMA2

source_table1 = f'{sourcezone}.`well-telemetry`'
checkPoint = f'{VOLUME_PATH}/checkpoints/base/lease_test'

dbutils.fs.rm(f'{VOLUME_PATH}/checkpoints/test/01', recurse=True)

dest_table = f'{destzone}.lease'

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(df, batchId):
  # Pivot the dataframe to transform sensor readings into columns
  result_df = (df.groupBy("client_id", "well_id", "timestamp", "sensor_id")
    .pivot("col_name")
    .agg(first("value")))
  print("batch inserts ongoing")

  # Get distinct rows with the columns you need
  additional_cols = df.select("client_id", "well_id", "sensor_id", "timestamp", "location", "status", "quality").distinct()

  # Join them together
    # Write to Delta table

  out_df = result_df.join(additional_cols, ["client_id", "well_id", "sensor_id", "timestamp"], "left")
  (out_df.write
          .format("delta")
          .option("mergeSchema", "true")
          .mode("append")
          .saveAsTable(dest_table))
  return

print("Ready to begin pushing for each batch")
     
     
     
        
display(spark.readStream.table(source_table1)
    .withColumn("timestamp", to_timestamp("timestamp")).limit(1000)
    , checkpointLocation = f"{VOLUME_PATH}/checkpoints/test/01"
    )






# (
#     spark.readStream.table(source_table1)
#         .withColumn("timestamp", to_timestamp("timestamp"))
#         .withColumn("timestamp", date_trunc("MM-dd-yyyy HH:mm:ss", "timestamp"))
#         .withColumn("col_name", concat_ws("_", "sensor_type", "unit"))
#         .writeStream
#         .option("checkpointLocation", checkPoint)
#         .foreachBatch(upsertToDelta)
#         .trigger(availableNow=True)
#         .start()
# )
