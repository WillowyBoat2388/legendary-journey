from pyspark.sql.types import *
from pyspark.sql.functions import *

# Set Spark shuffle partitions for performance tuning
spark.conf.set("spark.sql.shuffle.partitions", "400")
# Enable adaptive query execution for better join performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# Optimize shuffle for stream-stream joins
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")

# Retrieve workspace name from Databricks secrets and format it
secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1  = f"{workspace_name}.raw"
SCHEMA2  = f"{workspace_name}.base"

# Define volume path for checkpoints
VOLUME_PATH = f"/Volumes/{workspace_name}/default"

# Set source and destination schemas
sourcezone = SCHEMA1
destzone = SCHEMA2

# Define source and destination table names
source_table1 = f'{sourcezone}.`facility-telemetry`'
source_table2 = f'{sourcezone}.`well-telemetry`'
checkPoint = f'{VOLUME_PATH}/checkpoints/base/firm_info'
dest_table = f'{destzone}.firm_info'

# Debug: Print table and checkpoint paths
# print(f"[DEBUG] Source Table 1: {source_table1}")
# print(f"[DEBUG] Source Table 2: {source_table2}")
# print(f"[DEBUG] Destination Table: {dest_table}")
# print(f"[DEBUG] Checkpoint Path: {checkPoint}")

# Read and transform facility-telemetry stream
# Removed dropDuplicates() - expensive stateful operation in streaming
facility_df = (
    spark.readStream
    .table(source_table1)
    .select("client_id", "facility_id")
    .withColumn("facility_parts", split(col("facility_id"), "_"))
    .withColumn("FACILITY", concat_ws("_", slice(col("facility_parts"), 1, size(col("facility_parts")) - 1)))
    .withColumn("FACILITY_ID", col("facility_parts")[size(col("facility_parts")) - 1].cast("int"))
    .drop("facility_parts")
)
print("[DEBUG] facility_df streaming transformation defined.")

# Read well-telemetry stream - removed distinct() for performance
# Select only client_id to minimize shuffle data
well_df = (
    spark.readStream
    .table(source_table2)
    .select("client_id")
)
print("[DEBUG] well_df streaming transformation defined.")

# Stream-stream join - leverage clustering on client_id
# Using simple join syntax since tables are clustered
joined_df = facility_df.join(well_df, "client_id", "inner")
print("[DEBUG] Stream-stream join between facility_df and well_df defined.")

# Apply transformations on the joined result to extract firm and facility info
result_df = (
    joined_df
    .withColumn("client_parts", split(col("client_id"), "_"))
    .withColumn("DRILLING_FIRM", concat_ws("_", slice(col("client_parts"), 1, size(col("client_parts")) - 1)))
    .withColumn("FIRM_ID", col("client_parts")[size(col("client_parts")) - 1].cast("int"))
    .drop("client_parts", "client_id")
    .select("DRILLING_FIRM", "FIRM_ID", "FACILITY", "FACILITY_ID")
)
print("[DEBUG] result_df transformation defined.")

# Write the result to the destination Delta table with checkpointing
(
    result_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkPoint)
    .trigger(availableNow=True)
    .toTable(dest_table)
)
print("[DEBUG] Streaming write to Delta table initiated.")