from pyspark.sql.types import *
from pyspark.sql.functions import *

# spark.sql("DROP TABLE IF EXISTS base.lease")
spark.conf.set("spark.sql.shuffle.partitions", "1")
secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1  = f"{workspace_name}.raw"
SCHEMA2  = f"{workspace_name}.base"

VOLUME_PATH = f"/Volumes/{workspace_name}/default"

sourcezone = SCHEMA1
destzone = SCHEMA2

source_table1 = f'{sourcezone}.`well-telemetry`'
checkPoint = f'{VOLUME_PATH}/checkpoints/base/lease_test'

dest_table = f'{destzone}.lease'

# Function to upsert microBatchOutputDF into Delta table using merge
def upsertToDelta(df, batchId):
  # Pivot the dataframe to transform sensor readings into columns
  result_df = (df.groupBy("client_id", "well_id", "timestamp", "sensor_id")
    .pivot("col_name")
    .agg(first("value")))
  print("batch inserts ongoing")
  # Write to Delta table
  (result_df.write
          .format("delta")
          .option("mergeSchema", "true")
          .mode("append")
          .saveAsTable(dest_table))
  return

print("Ready to begin pushing for each batch")
(
    spark.readStream.table(source_table1)
        .withColumn("timestamp", to_timestamp("timestamp"))
        .withColumn("col_name", concat_ws("_", "sensor_type", "unit"))
        .writeStream
        .foreachBatch(upsertToDelta)
        .option("checkpointLocation", checkPoint)
        .trigger(availableNow=True)
        .start()
)
