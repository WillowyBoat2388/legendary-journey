from pyspark.sql.types import *
from pyspark.sql.functions import *


spark.conf.set("spark.sql.shuffle.partitions", "1")
secret_name = str(dbutils.secrets.get(scope='databricks-keyvault', key='databricks-workspace-name')).lower()
workspace_name = secret_name.replace("-", "_")
SCHEMA1  = f"{workspace_name}.raw"
SCHEMA2  = f"{workspace_name}.base"

VOLUME_PATH = f"/Volumes/{workspace_name}/default"

sourcezone = SCHEMA1
destzone = SCHEMA2

source_table1 = f'{sourcezone}.`facility-telemetry`'
source_table2 = f'{sourcezone}.`well-telemetry`'
checkPoint = f'{VOLUME_PATH}/checkpoints/base/firm_info'

dest_table = f'{destzone}.firm_info'

# Read and transform facility-telemetry
facility_df = (spark.readStream
    .table(source_table1)
    .select("client_id", "facility_id")
    .withColumn("facility_parts", split(col("facility_id"), "_"))
    .withColumn("FACILITY", concat_ws("_", slice(col("facility_parts"), 1, size(col("facility_parts")) - 1)))
    .withColumn("FACILITY_ID", col("facility_parts")[size(col("facility_parts")) - 1].cast("int"))
    .drop("facility_parts")
)

# Read and transform well-telemetry
well_df = (spark.readStream
    .table(source_table2)
    .select("client_id")
)

# Join both streams
joined_df = facility_df.join(well_df, "client_id", "inner")

# Apply transformations on the joined result (client_id from both sources)
result_df = (joined_df
    .withColumn("client_parts", split(col("client_id"), "_"))
    .withColumn("DRILLING_FIRM", concat_ws("_", slice(col("client_parts"), 1, size(col("client_parts")) - 1)))
    .withColumn("FIRM_ID", col("client_parts")[size(col("client_parts")) - 1].cast("int"))
    .select("DRILLING_FIRM", "FIRM_ID", "FACILITY", "FACILITY_ID")
)

# Preview facility_df intermediate output using memory sink
query = (result_df
    .writeStream
    .format("memory")
    .queryName("preview")
    .option("checkpointLocation", f"{VOLUME_PATH}/checkpoints/preview/test5")
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)

# Wait for some data to arrive
import time
time.sleep(5)

# Query the in-memory table to see results
display(spark.sql("SELECT * FROM preview LIMIT 100"))

# Stop the preview query
query.stop()

# Write to destination
# (result_df
#     .writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", checkPoint)
#     .trigger(availableNow=True)
#     .toTable(dest_table)
# )
